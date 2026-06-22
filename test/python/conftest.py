import importlib
import importlib.util
import sys
from pathlib import Path

import pytest
from packaging.specifiers import SpecifierSet
from packaging.version import Version


REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from scripts.data_generators.integration_config import (
    REST_CATALOG_NAMES,
    get_rest_catalog_profile,
    resolve_active_catalog,
    resolve_pyspark_runtime,
)
from scripts.data_generators.tests import IcebergTest
from spark_seed import SparkSeedTable


if importlib.util.find_spec("pyspark") is not None:
    pyspark = importlib.import_module("pyspark")
    PYSPARK_VERSION = Version(pyspark.__version__)
else:
    pyspark = None
    PYSPARK_VERSION = None


def _requires_catalog_options(path: str) -> bool:
    return "cloud" not in Path(path).parts


def _selected_catalog_profile(config: pytest.Config):
    try:
        catalog = resolve_active_catalog(
            allowed_catalogs=REST_CATALOG_NAMES,
            purpose="catalog-backed test/python runs",
        )
    except RuntimeError as exc:
        raise pytest.UsageError(str(exc)) from exc
    return get_rest_catalog_profile(catalog)


def _selected_spark_runtime(config: pytest.Config):
    try:
        runtime, _ = resolve_pyspark_runtime(purpose="catalog-backed test/python runs")
    except RuntimeError as exc:
        raise pytest.UsageError(str(exc)) from exc
    return runtime


def pytest_configure(config):
    config.addinivalue_line(
        "markers",
        "requires_spark(spec): require Spark version matching spec "
        "(PEP 440 specifier, e.g. '>=3.5,<4.0', '==4.0.*')",
    )
    config.addinivalue_line(
        "markers",
        "spark_seed_tables(*tables): seed catalog_connection with registered names or SparkSeedTable objects",
    )


def pytest_addoption(parser):
    parser.addoption(
        "--unittest-binary",
        action="store",
        default=None,
        help="Provide the unittest binary to use for stdin-driven integration tests",
    )
    parser.addoption(
        "--print-unittest-stdin",
        action="store_true",
        default=False,
        help="Print the sqllogictest stdin transcript for stdin-driven integration tests",
    )


def pytest_ignore_collect(collection_path, config):
    requested_paths = [Path(arg) for arg in config.args if not arg.startswith("-")]
    explicit_cloud_run = any("cloud" in path.parts for path in requested_paths)
    if not explicit_cloud_run and "cloud" in Path(str(collection_path)).parts:
        return True
    return False


@pytest.fixture()
def unittest_binary(request):
    custom_arg = request.config.getoption("--unittest-binary")
    if not custom_arg:
        raise ValueError(
            "Please provide a unittest binary path to the tester, using '--unittest-binary <path_to_unittest>'"
        )
    return custom_arg


@pytest.fixture()
def print_unittest_stdin(pytestconfig):
    return pytestconfig.getoption("--print-unittest-stdin")


@pytest.fixture(scope="session")
def catalog_profile(pytestconfig):
    profile = getattr(pytestconfig, "_catalog_profile", None)
    if profile is None:
        raise pytest.UsageError("Catalog profile is only available for catalog-backed test/python runs")
    return profile


@pytest.fixture(scope="session")
def spark_runtime(pytestconfig):
    runtime = getattr(pytestconfig, "_spark_runtime", None)
    if runtime is None:
        raise pytest.UsageError("Spark runtime is only available for catalog-backed test/python runs")
    return runtime


@pytest.fixture(scope="session")
def unittest_test_config(catalog_profile):
    return catalog_profile.unittest_config


@pytest.fixture(scope="session")
def duckdb_catalog_init_sql(catalog_profile):
    return catalog_profile.duckdb_catalog_init_sql


@pytest.fixture(scope="session")
def bearer_token(catalog_profile):
    requests = pytest.importorskip("requests")
    response = requests.post(
        catalog_profile.pyiceberg_oauth_token_url,
        data=catalog_profile.pyiceberg_oauth_payload,
    )
    assert response.status_code == 200
    access_token = response.json().get("access_token")
    assert access_token
    return access_token


@pytest.fixture(scope="session")
def rest_catalog(catalog_profile, bearer_token):
    pyice_rest = pytest.importorskip("pyiceberg.catalog.rest")
    return pyice_rest.RestCatalog("rest", **catalog_profile.build_pyiceberg_config(bearer_token))


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


def _resolve_seed_table(table):
    if isinstance(table, str):
        return _find_generator_case(table)
    if isinstance(table, SparkSeedTable):
        return table
    raise ValueError(
        "spark_seed_tables entries must be registered table names or SparkSeedTable objects, "
        f"got {type(table).__name__}"
    )


@pytest.fixture(scope="session")
def catalog_session_connection(catalog_profile, spark_runtime):
    from scripts.data_generators.connections import IcebergConnection

    connection = IcebergConnection.get_class(catalog_profile.connection_key)(runtime=spark_runtime)
    yield connection
    connection.close()


@pytest.fixture()
def catalog_connection(request, catalog_session_connection):
    connection = catalog_session_connection
    seed_marker = request.node.get_closest_marker("spark_seed_tables")
    seed_names = list(seed_marker.args) if seed_marker else []

    for table in seed_names:
        seed_table = _resolve_seed_table(table)
        if isinstance(seed_table, IcebergTest):
            seed_table.write_intermediates = False
        seed_table.generate(connection)

    yield connection


@pytest.fixture()
def spark_con(catalog_connection):
    return catalog_connection.con


def require_table_support(table_name: str, spark_runtime, catalog_profile) -> None:
    if "format_version_3" not in table_name:
        return
    if not spark_runtime.supports_v3:
        pytest.skip(f"Spark runtime {spark_runtime.name} does not support Iceberg format version 3")
    if not catalog_profile.supports_v3_tables:
        pytest.skip(f"Catalog '{catalog_profile.name}' does not support Iceberg format version 3 in this suite")


def pytest_collection_modifyitems(config, items):
    needs_catalog_options = any(_requires_catalog_options(str(item.fspath)) for item in items)
    if needs_catalog_options:
        config._catalog_profile = _selected_catalog_profile(config)
        config._spark_runtime = _selected_spark_runtime(config)

    for item in items:
        marker = item.get_closest_marker("requires_spark")
        if marker is None:
            continue

        spec = SpecifierSet(marker.args[0])
        if PYSPARK_VERSION is None:
            item.add_marker(pytest.mark.skip(reason=f"Requires Spark {spec}, but PySpark is not installed"))
        elif PYSPARK_VERSION not in spec:
            item.add_marker(
                pytest.mark.skip(reason=f"Requires Spark {spec}, but current PySpark version is {PYSPARK_VERSION}")
            )
