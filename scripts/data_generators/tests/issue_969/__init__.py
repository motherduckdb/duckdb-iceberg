from scripts.data_generators.tests.base import IcebergTest
import pathlib


@IcebergTest.register()
class Test(IcebergTest):
    expected_failures = {
        "nessie": "Non-main branches not supported"
    }

    def __init__(self):
        path = pathlib.PurePath(__file__)
        super().__init__(path.parent.name)
