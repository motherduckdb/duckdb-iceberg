from scripts.data_generators.tests.base import IcebergTest
import pathlib


@IcebergTest.register()
class Test(IcebergTest):
    expected_failures = {
        "nessie": "format-version update not supported yet"
    }

    def __init__(self):
        path = pathlib.PurePath(__file__)
        super().__init__(__file__)
