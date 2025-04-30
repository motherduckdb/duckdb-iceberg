import duckdb
import os
from scripts.data_generators.tests.base import IcebergTest
import pathlib
import tempfile


@IcebergTest.register()
class Test(IcebergTest):
    def __init__(self):
        path = pathlib.PurePath(__file__)
        super().__init__(path.parent.name)

        # Create a temporary directory
        self.tempdir = pathlib.Path(tempfile.mkdtemp())
        self.parquet_file = self.tempdir / "tmp.parquet"

        # Connect to DuckDB
        duckdb_con = duckdb.connect()

        # Generate TPCH lineitem data
        duckdb_con.execute("call dbgen(sf=0.001)")

        # Create view with various column types
        duckdb_con.query(
            """CREATE VIEW test_table AS
				SELECT
					(l_orderkey % 2 = 0) AS l_orderkey_bool,
					l_partkey::INT32 AS l_partkey_int,
					l_suppkey::INT64 AS l_suppkey_long,
					l_extendedprice::FLOAT AS l_extendedprice_float,
					l_extendedprice::DOUBLE AS l_extendedprice_double,
					l_extendedprice::DECIMAL(9,2) AS l_extendedprice_dec9_2,
					l_extendedprice::DECIMAL(18,6) AS l_extendedprice_dec18_6,
					l_extendedprice::DECIMAL(38,10) AS l_extendedprice_dec38_10,
					l_shipdate::DATE AS l_shipdate_date,
					l_partkey AS l_partkey_time,
					l_commitdate::TIMESTAMP AS l_commitdate_timestamp,
					l_commitdate::TIMESTAMPTZ AS l_commitdate_timestamp_tz,
					l_comment AS l_comment_string,
					gen_random_uuid()::VARCHAR AS uuid,
					l_comment::BLOB AS l_comment_blob
				FROM lineitem;"""
        )

        # Export the view to a Parquet file in the temp directory
        duckdb_con.execute(f"COPY test_table TO '{self.parquet_file}' (FORMAT PARQUET)")

    def setup(self, con):
        con.con.read.parquet(self.parquet_file.as_posix()).createOrReplaceTempView('parquet_file_view')
