## README
Script used to generate test data for this repo.
Run it with pytest, one catalog per invocation:
`python3 -m pytest scripts/data_generators/test_generate_data.py --catalog <catalog>`

Prefer `make data` or the catalog-specific make targets.

The generator uses PySpark with the Iceberg extension to populate the active catalog for every registered `IcebergTest`.
Intermediates for each step are saved to `data/generated/intermediates/{connection_key}/{table}/{step}`.

### Validation
- count(*) after each step
- full table copy to parquet file after each step

### Idea behind script:
- generate data easily within this repo
- contains all iceberg datatypes (currently WIP)
- contains nulls
- configurable scale factor (currently WIP)
- verify behaviour matches spark

### Todo's:
- Arbitrary precision Decimals?
- Time not yet working
- PySpark does not support UUID
- Generate similar data from snowflake's iceberg implementation
- value deletes?

### To add a test (generate an iceberg table):
- Create a new folder in `scripts/data_generators/tests`
- Write the `__init__.py`, which is usually as simple as:
```py
from scripts.data_generators.tests.base import IcebergTest
import pathlib

@IcebergTest.register()
class Test(IcebergTest):
    def __init__(self):
        path = pathlib.PurePath(__file__)
        super().__init__(path.parent.name)
```
- Add the `.sql` files to the folder (one statement per file)
- If the case should use a different connection implementation for a specific catalog, set `catalog_mapping`, for example:
```py
class Test(IcebergTest):
    catalog_mapping = {"spark-rest": "spark-rest-single-thread"}
```
- If a case should be skipped for a specific catalog, set `skips = {"catalog": "reason"}`.
- If the case only applies to some catalogs, set `supported_catalogs`.
- If a catalog is a known limitation, set `expected_failures = {"catalog": "reason"}` so pytest reports a strict xfail.

### To add a new connection (new iceberg catalog type to test against):
- Create a new folder in `scripts/data_generators/connections`
- Create an `__init__.py` file in the folder
- Add a new derived class of `IcebergConnection`
- Add the `@IcebergConnection.register(CONNECTION_KEY)` decorator to it
- Make its constructor accept any `--connection-arg key=value` values it needs as keyword arguments
