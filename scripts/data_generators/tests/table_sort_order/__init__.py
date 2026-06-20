from scripts.data_generators.tests.base import IcebergTest
import pathlib
import tempfile
import duckdb


POLARIS_SKIP_REASON = (
    "Polaris requires partition and sort updates to be applied as delete-and-insert operations, "
    "which Spark does not support for this generator"
)


@IcebergTest.register()
class Test(IcebergTest):
    skips = {"polaris": POLARIS_SKIP_REASON}

    def __init__(self):
        path = pathlib.PurePath(__file__)
        super().__init__(path.parent.name)

        # Create a temporary directory
        self.tempdir = pathlib.Path(tempfile.mkdtemp())
        self.parquet_file = self.tempdir / "tmp.parquet"

        duckdb_con = duckdb.connect()
        duckdb_con.execute("call dbgen(sf=0.01)")
        duckdb_con.execute(f"copy lineitem to '{self.parquet_file}' (FORMAT PARQUET)")

    def setup(self, con):
        con.con.read.parquet(self.parquet_file.as_posix()).createOrReplaceTempView('parquet_file_view')
