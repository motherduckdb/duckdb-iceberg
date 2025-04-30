## README
Script used to generate test data for this repo.
Can be used directly with `python3 -m scripts.data_generators.generate_data [target]+`, but prefer to use `make data`

The script uses PySpark with the Iceberg Extension to generate a dataset in the targeted catalog, for every IcebergTest defined.
Intermediates for every step of an IcebergTest are saved to `data/generated/intermediates/{target}/{table}/{step}`

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

### To add a new connection (new iceberg catalog type to test against):
- Add an option to the choices for `targets`, in `generate_data.py`
- Create a new folder in `scripts/data_generators/connections`
- Create an `__init__.py` file in the folder
- Add a new derived class of `IcebergConnection`
- Add the `@IcebergConnection.register(CONNECTION_KEY)` decorator to it, where `CONNECTION_KEY` is the choice added in step 1
- Define the `get_connection` method for the new class