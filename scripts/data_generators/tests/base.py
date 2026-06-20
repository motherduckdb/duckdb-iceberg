#!/usr/bin/python3
import os
from typing import Type, List
import shutil

SCRIPT_DIR = os.path.dirname(__file__)
INTERMEDIATE_DIR = os.path.join(SCRIPT_DIR, '..', '..', '..', 'data', 'generated', 'intermediates')


class IcebergTest:
    registry: List[Type['IcebergTest']] = []
    catalog_mapping: dict[str, str] = {}
    skips: dict[str, str] = {}
    supported_catalogs: set[str] | None = None
    expected_failures: dict[str, str] = {}

    @classmethod
    def register(cls):
        def decorator(subclass):
            cls.registry.append(subclass)
            return subclass

        return decorator

    def __init__(self, table: str, *, namespace=None, write_intermediates=True):
        self.table = table
        self.write_intermediates = write_intermediates
        self.namespace = ['default'] if not namespace else namespace
        self.files = self.get_files()

    def get_files(self):
        sql_files = [f for f in os.listdir(os.path.join(SCRIPT_DIR, self.table)) if f.endswith('.sql')]
        sql_files.sort()
        return sql_files

    def setup(self, con) -> None:
        pass

    def generate(self, con):
        self.setup(con)

        intermediate_dir = os.path.join(INTERMEDIATE_DIR, con.name, self.table)
        last_file = None
        for path in self.files:
            full_file_path = os.path.join(SCRIPT_DIR, self.table, path)
            with open(full_file_path, 'r') as file:
                snapshot_name = os.path.basename(path)[:-4]
                last_file = snapshot_name
                queries = [x for x in file.read().split(';') if x.strip() != '']

                for query in queries:
                    # Execute query on the underlying engine (e.g., Spark)
                    con.con.sql(query)

                    if self.write_intermediates:
                        namespace = '.'.join(self.namespace)
                        df = con.con.read.table(f"{namespace}.{self.table}")
                        intermediate_data_path = os.path.join(intermediate_dir, snapshot_name, 'data.parquet')
                        df.write.mode("overwrite").parquet(intermediate_data_path)

        if self.write_intermediates and last_file:
            # Finally, copy the latest results to a "last" dir for easy test writing
            shutil.copytree(
                os.path.join(intermediate_dir, last_file, 'data.parquet'),
                os.path.join(intermediate_dir, 'last', 'data.parquet'),
                dirs_exist_ok=True,
            )
