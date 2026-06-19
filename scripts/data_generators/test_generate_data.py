import pytest

from scripts.data_generators.tests import IcebergTest


def _generator_id(test_class: type[IcebergTest]) -> str:
    return test_class.__module__.rsplit(".", 1)[-1]


@pytest.fixture(params=IcebergTest.registry, ids=_generator_id)
def generator_case(request: pytest.FixtureRequest) -> IcebergTest:
    return request.param()


def test_generate_table(generator_case: IcebergTest, iceberg_connection) -> None:
    generator_case.generate(iceberg_connection)
