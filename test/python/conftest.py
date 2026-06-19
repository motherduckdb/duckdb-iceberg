import pytest
from packaging.version import Version
from packaging.specifiers import SpecifierSet

pyspark = pytest.importorskip("pyspark")

PYSPARK_VERSION = Version(pyspark.__version__)


def pytest_configure(config):
    config.addinivalue_line(
        "markers",
        "requires_spark(spec): require Spark version matching spec "
        "(PEP 440 specifier, e.g. '>=3.5,<4.0', '==4.0.*')",
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
