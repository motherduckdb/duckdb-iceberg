from scripts.data_generators.tests.base import IcebergTest
import pathlib


@IcebergTest.register()
class Test(IcebergTest):
    def __init__(self):
        path = pathlib.PurePath(__file__)
        # write_intermediates=False: the per-step parquet snapshot reads the table
        # by name (always main); branch-only writes would produce misleading dumps.
        super().__init__(path.parent.name, write_intermediates=False)
