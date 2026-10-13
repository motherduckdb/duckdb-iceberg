import pytest

from scripts.data_generators.tests import IcebergTest


def _generator_id(test_class: type[IcebergTest]) -> str:
    module_name = test_class.__module__
    package_prefix = "scripts.data_generators.tests."
    if module_name.startswith(package_prefix):
        module_name = module_name[len(package_prefix):]
    return module_name.rsplit(".__init__", 1)[0]


@pytest.fixture(params=IcebergTest.registry, ids=_generator_id)
def generator_case(request: pytest.FixtureRequest) -> IcebergTest:
    return request.param()


def test_generate_table(generator_case: IcebergTest, iceberg_connection) -> None:
    generator_case.generate(iceberg_connection)
