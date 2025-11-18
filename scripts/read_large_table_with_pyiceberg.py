from pyiceberg.catalog import load_catalog
from pyiceberg.expressions import GreaterThanOrEqual
from pyiceberg.transforms import YearTransform
from pyiceberg.table import Table
from datetime import datetime
import time

# -------------------------------------------------------------------
# 1. Load the Iceberg REST catalog
# -------------------------------------------------------------------
catalog = load_catalog(
    "local_rest",
    **{
        "uri": "http://127.0.0.1:8181",
        "type": "rest",
        "warehouse": '',
        "s3.endpoint": "http://127.0.0.1:9000",
        "s3.access-key-id": "admin",
        "s3.secret-access-key": "password",
        "s3.path-style-access": "true",
        "s3.ssl.enabled": "false",
    },
)

# -------------------------------------------------------------------
# 2. Load the table reference
# -------------------------------------------------------------------
table: Table = catalog.load_table("default.large_partitioned_table")

# -------------------------------------------------------------------
# 3. Define an expression: year(joined) > 2017
# -------------------------------------------------------------------
# -------------------------------------------------------------------
# 4. Read and filter the table
# -------------------------------------------------------------------
# This uses table.scan() and filter() to push down predicates
start = time.time()
df = table.scan(
    GreaterThanOrEqual("joined", datetime(2017, 1, 1)),
).to_arrow()
end = time.time()
print(f"Pyiceberg read {len(df)} rows in {end - start:.2f} seconds")

# prints all the files scanned.
# [task.file.file_path for task in scan.plan_files()]
