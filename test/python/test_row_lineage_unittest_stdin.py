import os
import subprocess
from pathlib import Path

import pytest

from test_spark_read import Row, SparkSession, requires_iceberg_server, spark_con

SCRIPT_DIR = Path(__file__).resolve().parent
ROW_LINEAGE_GENERATOR_SQL = (
    SCRIPT_DIR.parent.parent
    / "scripts"
    / "data_generators"
    / "tests"
    / "default"
    / "row_lineage_test_upgraded"
    / "test.sql"
)


def _seed_row_lineage_table(spark_con) -> None:
    sql_text = ROW_LINEAGE_GENERATOR_SQL.read_text()

    spark_con.sql(f"CREATE NAMESPACE IF NOT EXISTS default")
    try:
        spark_con.sql(f"DROP TABLE default.row_lineage_test_upgraded")
    except Exception:
        pass

    for query in sql_text.split(";"):
        statement = query.strip()
        print(statement)
        if statement:
            spark_con.sql(statement)


def _run_duckdb_stdin_test(unittest_binary: str) -> tuple[str, str, int]:
    proc = subprocess.Popen(
        [
            unittest_binary,
            "--stdin",
            "--test-config",
            "/Users/thijs/DuckDBLabs/duckdb_iceberg/test/configs/fixture.json",
        ],
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        env={**os.environ},
    )

    sql_text = Path(
        '/Users/thijs/DuckDBLabs/duckdb_iceberg/test/sql/local/catalog_test_config_setup/catalog_agnostic/test_row_lineage_write_after_upgrade.test'
    ).read_text()

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


def _restart_spark_session(existing_spark):
    app_name = existing_spark.sparkContext.appName
    configs = dict(existing_spark.sparkContext.getConf().getAll())

    existing_spark.stop()

    builder = SparkSession.builder.appName(app_name)
    for key, value in configs.items():
        # Reapply the existing session config so the restarted session talks to the same REST catalog.
        builder = builder.config(key, value)

    restarted_spark = builder.getOrCreate()
    restarted_spark.sql("USE demo")
    restarted_spark.sql("CREATE NAMESPACE IF NOT EXISTS default")
    restarted_spark.sql("USE NAMESPACE default")
    return restarted_spark


@requires_iceberg_server
class TestRowLineageUnittestStdin:
    @pytest.mark.requires_spark(">=4.0")
    def test_row_lineage_test_upgraded_end_to_end(self, spark_con, unittest_binary):
        _seed_row_lineage_table(spark_con)

        stdout, stderr, returncode = _run_duckdb_stdin_test(unittest_binary)

        assert returncode == 0, (
            f"stdin unittest exited with code {returncode}\n" f"stdout:\n{stdout}\n" f"stderr:\n{stderr}"
        )
        assert "All tests passed" in stdout, f"stdout:\n{stdout}\nstderr:\n{stderr}"
        assert "<stdin>" in stdout, f"stdout:\n{stdout}\nstderr:\n{stderr}"

        spark_con = _restart_spark_session(spark_con)
        df = spark_con.sql(
            """
            select _last_updated_sequence_number, _row_id IS NOT NULL as has_row_id, * from default.row_lineage_test_upgraded order by id;
            """
        )
        res = df.collect()
        assert res == [
            Row(_last_updated_sequence_number=8, has_row_id=True, id=2, data="replaced_again"),
            Row(_last_updated_sequence_number=7, has_row_id=True, id=7, data="g_new"),
        ]
