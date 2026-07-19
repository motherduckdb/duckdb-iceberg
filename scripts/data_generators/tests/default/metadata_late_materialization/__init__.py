from scripts.data_generators.tests.base import IcebergTest


@IcebergTest.register()
class Test(IcebergTest):
    benchmark_only = True
    supported_catalogs = {"fixture"}
    manifest_count = 256
    rows_per_manifest = 4096

    def __init__(self):
        super().__init__(__file__, write_intermediates=False)

    def setup(self, con) -> None:
        con.con.sql(
            f"""
            CREATE TABLE {self.qualified_name} (
                partition_id INT,
                payload BIGINT
            )
            USING ICEBERG
            PARTITIONED BY (partition_id)
            TBLPROPERTIES (
                'format-version'='2',
                'commit.manifest-merge.enabled'='false'
            )
            """
        )

        for partition_id in range(self.manifest_count):
            first_value = partition_id * self.rows_per_manifest
            con.con.sql(
                f"""
                INSERT INTO {self.qualified_name}
                SELECT {partition_id}, id + {first_value}
                FROM range(0, {self.rows_per_manifest})
                """
            )
