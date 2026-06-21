import pytest

import duckdb_unittest
from conftest import _resolve_seed_table
from duckdb_unittest import DuckDBUnittestRunner
from spark_seed import SparkSeedTable


class RecordingStdin:
    def __init__(self):
        self.value = ""
        self.closed = False

    def write(self, text):
        self.value += text

    def flush(self):
        pass

    def close(self):
        self.closed = True


class FakeProcess:
    def __init__(self, returncode=0, stdout="All tests passed", stderr=""):
        self.recorded_stdin = RecordingStdin()
        self.stdin = self.recorded_stdin
        self.returncode = None
        self.finished_returncode = returncode
        self.stdout_text = stdout
        self.stderr_text = stderr
        self.killed = False

    def communicate(self):
        self.returncode = self.finished_returncode
        return self.stdout_text, self.stderr_text

    def poll(self):
        return self.returncode

    def kill(self):
        self.killed = True
        self.returncode = -9

    def wait(self):
        return self.returncode


@pytest.fixture()
def fake_popen(monkeypatch):
    processes = []

    def install(process=None):
        process = process or FakeProcess()
        processes.append(process)
        monkeypatch.setattr(duckdb_unittest.subprocess, "Popen", lambda *args, **kwargs: process)
        return process

    return install


def test_unittest_runner_writes_preamble_and_commands(fake_popen):
    process = fake_popen()

    with DuckDBUnittestRunner("unittest") as runner:
        runner.statement_ok("select 1")
        runner.statement_error("select broken", "<REGEX>:.*broken.*")
        runner.statement_error("select another_broken")
        runner.query("IBT", "select 1, NULL, true", [(1, None, True)])
        runner.send("halt")

    assert "require-env CATALOG_TEST_CONFIG_SETUP" in process.recorded_stdin.value
    assert "statement ok\nselect 1" in process.recorded_stdin.value
    assert "statement error\nselect broken\n----\n<REGEX>:.*broken.*" in process.recorded_stdin.value
    assert "statement error\nselect another_broken" in process.recorded_stdin.value
    assert "query IBT\nselect 1, NULL, true\n----\n1\tNULL\ttrue" in process.recorded_stdin.value
    assert process.recorded_stdin.value.endswith("halt\n\n")
    assert process.recorded_stdin.closed


def test_unittest_runner_rejects_query_rows_with_wrong_width(fake_popen):
    process = fake_popen()

    with pytest.raises(ValueError, match="Expected 2 values"):
        with DuckDBUnittestRunner("unittest") as runner:
            runner.query("II", "select 1", [(1,)])

    assert process.killed


def test_unittest_runner_reports_process_output(fake_popen):
    fake_popen(FakeProcess(returncode=1, stdout="failed output", stderr="failure details"))

    with pytest.raises(AssertionError, match="stdin unittest exited with code 1") as exc:
        with DuckDBUnittestRunner("unittest"):
            pass

    assert "require-env CATALOG_TEST_CONFIG_SETUP" in str(exc.value)
    assert "failed output" in str(exc.value)
    assert "failure details" in str(exc.value)


def test_unittest_runner_prints_recorded_stdin_when_requested(fake_popen, capsys):
    fake_popen()

    with DuckDBUnittestRunner("unittest", print_stdin=True) as runner:
        runner.statement_ok("select 42")

    captured = capsys.readouterr()
    assert "DuckDBUnittestRunner stdin:" in captured.out
    assert "statement ok\nselect 42" in captured.out


@pytest.mark.parametrize(
    ("commit", "terminator"),
    [(True, "commit"), (False, "abort")],
)
def test_unittest_runner_transaction(fake_popen, commit, terminator):
    process = fake_popen()

    with DuckDBUnittestRunner("unittest") as runner:
        with runner.with_transaction(commit=commit):
            runner.statement_ok("select 1")

    transaction = process.recorded_stdin.value.split("begin transaction", 1)[1]
    assert f"statement ok\nselect 1\n\nstatement ok\n{terminator}" in transaction


def test_unittest_runner_aborts_transaction_on_body_exception(fake_popen):
    process = fake_popen()

    with pytest.raises(RuntimeError, match="transaction failed"):
        with DuckDBUnittestRunner("unittest") as runner:
            with runner.with_transaction():
                raise RuntimeError("transaction failed")

    assert process.recorded_stdin.value.endswith("statement ok\nabort\n\n")


def test_unittest_runner_preserves_body_exception(fake_popen):
    process = fake_popen()

    with pytest.raises(RuntimeError, match="test body failed"):
        with DuckDBUnittestRunner("unittest"):
            raise RuntimeError("test body failed")

    assert process.killed


class RecordingSparkSQL:
    def __init__(self):
        self.statements = []

    def sql(self, statement):
        self.statements.append(statement.strip())
        if statement.startswith("DROP TABLE"):
            raise RuntimeError("table does not exist")


class RecordingConnection:
    def __init__(self):
        self.con = RecordingSparkSQL()


def test_inline_spark_seed_table_uses_generator_cleanup_and_executes_sql():
    connection = RecordingConnection()
    seed = SparkSeedTable(
        "nested.namespace.example",
        "CREATE TABLE nested.namespace.example (id INT); INSERT INTO nested.namespace.example VALUES (1);",
    )

    seed.generate(connection)

    assert connection.con.statements == [
        "CREATE NAMESPACE IF NOT EXISTS nested.namespace",
        "DROP TABLE nested.namespace.example",
        "CREATE TABLE nested.namespace.example (id INT)",
        "INSERT INTO nested.namespace.example VALUES (1)",
    ]


def test_seed_table_resolver_accepts_inline_and_registered_tables():
    inline = SparkSeedTable("default.inline", "CREATE TABLE default.inline (id INT)")

    assert _resolve_seed_table(inline) is inline
    assert _resolve_seed_table("row_lineage_test").qualified_name == "default.row_lineage_test"


def test_seed_table_resolver_rejects_unsupported_values():
    with pytest.raises(ValueError, match="registered table names or SparkSeedTable objects"):
        _resolve_seed_table(42)
