import duckdb
import os
from pyiceberg.catalog import load_catalog
import pyarrow.parquet as pq
import pyarrow as pa

# Base directory for all data
BASE_DIR = "data/persistent/column_mapping"

# Ensure necessary directories exist
WAREHOUSE_DIR = f"{BASE_DIR}/warehouse/example"
os.makedirs(WAREHOUSE_DIR, exist_ok=True)

# Create a sample Parquet file
sample_data = pa.table(
    {
        "id": pa.array([1, 2, 3], type=pa.int64()),
        "name": pa.array(["Alice", "Bob", "Charlie"], type=pa.string()),
        "age": pa.array([25, 30, 35], type=pa.int64()),
        # Map column
        "attributes": pa.array(
            [
                {"height": "5.5", "weight": "130"},
                {"height": "6.0", "weight": "180"},
                {"height": "5.8", "weight": "160"},
            ],
            type=pa.map_(pa.string(), pa.string()),
        ),
        # List column
        "scores": pa.array([[85, 90], [78, 82, 88], [92]], type=pa.list_(pa.int64())),
        # Struct column
        "profile": pa.array(
            [
                {"email": "alice@example.com", "verified": True},
                {"email": "bob@example.com", "verified": False},
                {"email": "charlie@example.com", "verified": True},
            ],
            type=pa.struct({"email": pa.string(), "verified": pa.bool_()}),
        ),
    }
)

pq.write_table(sample_data, f"{WAREHOUSE_DIR}/mock_data.parquet")


def add_parquet_file_to_table(uri: str, namespace: str, catalog_name: str, table_name: str, parquet_path: str):
    """Registers a Parquet file into an Iceberg table."""

    # Load Iceberg catalog with warehouse in the new base directory
    catalog = load_catalog(catalog_name, uri=uri, warehouse=f"{BASE_DIR}/warehouse")

    # Read and clean Parquet schema
    parquet_table = pq.read_table(parquet_path)
    parquet_schema = parquet_table.schema.remove_metadata()

    # Create namespace if needed
    catalog.create_namespace_if_not_exists(namespace)
    table_identifier = f"{namespace}.{table_name}"

    try:
        # Try to load existing table first
        table = catalog.load_table(table_identifier)
    except:
        # If table doesn't exist or is corrupted, drop it and create new
        try:
            catalog.drop_table(table_identifier)
        except:
            pass
        table = catalog.create_table(table_identifier, schema=parquet_schema)

    # Check if file is already registered, to keep idempotent
    existing_files = table.inspect.files()
    if parquet_path not in str(existing_files):
        table.add_files([parquet_path])

    return table


# Example usage
if __name__ == "__main__":
    file_path = f"{WAREHOUSE_DIR}/mock_data.parquet"
    catalog_name = "my_catalog"
    table_name = "my_table"
    uri_catalogue = f"sqlite:///{BASE_DIR}/catalog.sqlite"
    namespace = "default"

    table = add_parquet_file_to_table(uri_catalogue, namespace, catalog_name, table_name, file_path)
    # conn.sql(f"SET unsafe_enable_version_guessing = true; SELECT * FROM iceberg_scan('warehouse/default.db/my_table');").show()
