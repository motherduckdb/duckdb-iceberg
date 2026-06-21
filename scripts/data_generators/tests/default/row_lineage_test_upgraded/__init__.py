from scripts.data_generators.tests.base import IcebergTest


@IcebergTest.register()
class Test(IcebergTest):
    expected_failures = {
        "nessie": "format-version update not supported yet"
    }

    def __init__(self, *, write_intermediates=True):
        super().__init__(__file__, write_intermediates=write_intermediates)
