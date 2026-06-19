import os
import subprocess
import sys
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from scripts.data_generators.connections import IcebergConnection
from scripts.data_generators.tests.default.row_lineage_test_upgraded import Test as RowLineageTestUpgraded
from test_spark_read import Row, requires_iceberg_server

FIXTURE_CONFIG_PATH = REPO_ROOT / "test" / "configs" / "fixture.json"
ROW_LINEAGE_DUCKDB_TEST_PATH = (
    REPO_ROOT
    / "test"
    / "sql"
    / "local"
    / "catalog_test_config_setup"
    / "catalog_agnostic"
    / "test_row_lineage_write_after_upgrade.test"
)


@pytest.fixture()
def spark_rest_connection():
    connection = IcebergConnection.get_class("spark-rest")()
    try:
        yield connection
    finally:
        connection.close()


def _seed_row_lineage_table(connection) -> None:
    generator = RowLineageTestUpgraded(write_intermediates=False)
    generator.generate(connection)


def _run_duckdb_stdin_test(unittest_binary: str) -> tuple[str, str, int]:
    proc = subprocess.Popen(
        [
            unittest_binary,
            "--stdin",
            "--test-config",
            str(FIXTURE_CONFIG_PATH),
        ],
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        env={**os.environ},
    )

    sql_text = ROW_LINEAGE_DUCKDB_TEST_PATH.read_text()

    try:
        proc.stdin.write(sql_text)
        proc.stdin.flush()
        proc.stdin.close()

        stdout = proc.stdout.read()
        stderr = proc.stderr.read()
        proc.wait()
    finally:
        if proc.poll() is None:
            proc.kill()
            proc.wait()

    return stdout, stderr, proc.returncode


@requires_iceberg_server
class TestRowLineageUnittestStdin:
    @pytest.mark.requires_spark(">=4.0")
    def test_row_lineage_test_upgraded_end_to_end(self, spark_rest_connection, unittest_binary):
        _seed_row_lineage_table(spark_rest_connection)

        stdout, stderr, returncode = _run_duckdb_stdin_test(unittest_binary)

        assert returncode == 0, (
            f"stdin unittest exited with code {returncode}\n" f"stdout:\n{stdout}\n" f"stderr:\n{stderr}"
        )
        assert "All tests passed" in stdout, f"stdout:\n{stdout}\nstderr:\n{stderr}"
        assert "<stdin>" in stdout, f"stdout:\n{stdout}\nstderr:\n{stderr}"

        spark_rest_connection.restart()
        df = spark_rest_connection.con.sql(
            """
            select _last_updated_sequence_number, _row_id IS NOT NULL as has_row_id, * from default.row_lineage_test_upgraded order by id;
            """
        )
        res = df.collect()
        assert res == [
            Row(_last_updated_sequence_number=8, has_row_id=True, id=2, data="replaced_again"),
            Row(_last_updated_sequence_number=7, has_row_id=True, id=7, data="g_new"),
        ]
