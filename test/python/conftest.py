import sys
from pathlib import Path

import pytest
from packaging.version import Version
from packaging.specifiers import SpecifierSet

pyspark = pytest.importorskip("pyspark")

PYSPARK_VERSION = Version(pyspark.__version__)
REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from scripts.data_generators.connections import IcebergConnection
from scripts.data_generators.tests import IcebergTest


def pytest_configure(config):
    config.addinivalue_line(
        "markers",
        "requires_spark(spec): require Spark version matching spec "
        "(PEP 440 specifier, e.g. '>=3.5,<4.0', '==4.0.*')",
    )
    config.addinivalue_line(
        "markers",
        "spark_seed_tables(*names): seed the spark_rest_connection fixture with one or more generator tables",
    )


def pytest_addoption(parser):
    parser.addoption(
        "--unittest-binary",
        action="store",
        default=None,
        help="Provide the unittest binary to use for stdin-driven integration tests",
    )


@pytest.fixture()
def unittest_binary(request):
    custom_arg = request.config.getoption("--unittest-binary")
    if not custom_arg:
        raise ValueError(
            "Please provide a unittest binary path to the tester, using '--unittest-binary <path_to_unittest>'"
        )
    return custom_arg


def _find_generator_case(table_name: str):
    matches = []
    for generator_class in IcebergTest.registry:
        generator = generator_class()
        if generator.table == table_name or generator.qualified_name == table_name:
            matches.append(generator)

    if not matches:
        raise ValueError(f"No data generator registered for table '{table_name}'")
    if len(matches) > 1:
        matched_names = ", ".join(generator.qualified_name for generator in matches)
        raise ValueError(
            f"Multiple data generators match '{table_name}': {matched_names}. "
            "Use the fully qualified generator name instead."
        )
    return matches[0]


@pytest.fixture()
def spark_rest_connection(request):
    connection = IcebergConnection.get_class("spark-rest")()
    try:
        seed_marker = request.node.get_closest_marker("spark_seed_tables")
        seed_names = list(seed_marker.args) if seed_marker else []

        for table_name in seed_names:
            generator = _find_generator_case(table_name)
            generator.write_intermediates = False
            generator.generate(connection)

        yield connection
    finally:
        connection.close()


# Dynamically skip tests on Spark versions that aren't compatible with it
def pytest_collection_modifyitems(config, items):
    for item in items:
        marker = item.get_closest_marker("requires_spark")
        if marker:
            spec = SpecifierSet(marker.args[0])

            if PYSPARK_VERSION not in spec:
                item.add_marker(
                    pytest.mark.skip(
                        reason=(f"Requires Spark {spec}, " f"but current PySpark version is {PYSPARK_VERSION}")
                    )
                )
