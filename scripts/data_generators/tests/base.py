#!/usr/bin/python3
from pathlib import Path
from typing import Type, List
import shutil

SCRIPT_DIR = Path(__file__).resolve().parent
INTERMEDIATE_DIR = SCRIPT_DIR.parent.parent.parent / 'data' / 'generated' / 'intermediates'


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

    def __init__(self, source_file: str, *, write_intermediates=True):
        self.test_dir = Path(source_file).resolve().parent
        relative_parts = self.test_dir.relative_to(SCRIPT_DIR).parts
        if len(relative_parts) < 2:
            raise ValueError(
                f"Expected test directories to include at least namespace/table under {SCRIPT_DIR}, got {self.test_dir}"
            )

        self.table = relative_parts[-1]
        self.write_intermediates = write_intermediates
        self.namespace = list(relative_parts[:-1])
        self.files = self.get_files()
        self.qualified_name = '.'.join([*self.namespace, self.table])

    def get_files(self):
        sql_files = [path.name for path in self.test_dir.iterdir() if path.suffix == '.sql']
        sql_files.sort()
        return sql_files

    def cleanup_generated_tables(self, con) -> None:
        namespace_name = '.'.join(self.namespace)
        con.con.sql(f"CREATE NAMESPACE IF NOT EXISTS {namespace_name}")
        try:
            con.con.sql(f"DROP TABLE {self.qualified_name}")
        except Exception:
            pass

    def setup(self, con) -> None:
        pass

    def generate(self, con):
        self.cleanup_generated_tables(con)
        self.setup(con)

        intermediate_dir = INTERMEDIATE_DIR / con.name / Path(*self.namespace) / self.table
        last_file = None
        for path in self.files:
            full_file_path = self.test_dir / path
            with open(full_file_path, 'r') as file:
                snapshot_name = Path(path).stem
                last_file = snapshot_name
                queries = [x for x in file.read().split(';') if x.strip() != '']

                for query in queries:
                    # Execute query on the underlying engine (e.g., Spark)
                    con.con.sql(query)

                    if self.write_intermediates:
                        df = con.con.read.table(self.qualified_name)
                        intermediate_data_path = intermediate_dir / snapshot_name / 'data.parquet'
                        df.write.mode("overwrite").parquet(intermediate_data_path.as_posix())

        if self.write_intermediates and last_file:
            # Finally, copy the latest results to a "last" dir for easy test writing
            shutil.copytree(
                (intermediate_dir / last_file / 'data.parquet').as_posix(),
                (intermediate_dir / 'last' / 'data.parquet').as_posix(),
                dirs_exist_ok=True,
            )
