from scripts.data_generators.tests.base import IcebergTest
import pathlib


POLARIS_SKIP_REASON = (
    "Polaris requires partition and sort updates to be applied as delete-and-insert operations, "
    "which Spark does not support for this generator"
)


@IcebergTest.register()
class Test(IcebergTest):
    skips = {"polaris": POLARIS_SKIP_REASON}

    def __init__(self):
        path = pathlib.PurePath(__file__)
        super().__init__(__file__, write_intermediates=False)
