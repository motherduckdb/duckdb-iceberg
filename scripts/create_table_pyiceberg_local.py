from pyiceberg.catalog import load_catalog
from pyiceberg.catalog.rest import RestCatalog
import os
import pyarrow as pa
import sys
import duckdb
from datetime import date


DATABASE_NAME = "default"
TABLE_NAME = "basic_insert_test"


def get_local_catalog():
    rest_catalog = load_catalog(
        "local_rest",
        **{
            "type": "rest",
            "warehouse": "",
            "uri": f"http://127.0.0.1:8181",
            "s3.endpoint": "127.0.0.1:9000",
            "s3.access-key-id": "admin",
            "s3.secret-access-key": "password",
        },
    )
    return rest_catalog


def main():
    create_table(get_local_catalog())


def create_basic_schema() -> pa.Schema:
    return pa.schema(
        [
            pa.field("id", pa.int64()),
            pa.field("name", pa.string()),
            pa.field("address", pa.string()),
            pa.field("date", pa.date32()),
        ]
    )


def get_namespaces(rest_catalog):
    return rest_catalog.list_namespaces()


def get_tables(rest_catalog, namespace):
    return rest_catalog.list_tables(namespace)


def delete_table(rest_catalog):
    global DATABASE_NAME, TABLE_NAME
    namespaces = list(map(lambda t: t[0], get_namespaces(rest_catalog)))
    if DATABASE_NAME not in namespaces:
        print(f"schema {DATABASE_NAME} does not exist")
        print(f"known namespaces: " + str(get_namespaces(rest_catalog)))
        print(f"creating namespace {DATABASE_NAME}")
        rest_catalog.create_namespace(f"{DATABASE_NAME}")

    namespace = (DATABASE_NAME,)  # Tuple format
    # List tables in the namespace
    tables = list(map(lambda t: t[1], get_tables(rest_catalog, namespace)))

    if TABLE_NAME not in tables:
        print(f"table {TABLE_NAME} does not exist in database {DATABASE_NAME}, skipping delete")
        return

    identifier = (f"{DATABASE_NAME}", f"{TABLE_NAME}")
    # Drop the table
    rest_catalog.drop_table(identifier, purge_requested=True)
    print(f"{rest_catalog.name}: table {TABLE_NAME} dropped succesfully")


def create_table(rest_catalog):
    global DATABASE_NAME, TABLE_NAME
    namespaces = list(map(lambda t: t[0], get_namespaces(rest_catalog)))
    if DATABASE_NAME not in namespaces:
        print(f"schema {DATABASE_NAME} does not exist")
        print(f"known namespaces: " + str(get_namespaces(rest_catalog)))
        print(f"creating namespace {DATABASE_NAME}")

    namespace = (DATABASE_NAME,)  # Tuple format
    # List tables in the namespace
    tables = list(map(lambda t: t[1], get_tables(rest_catalog, namespace)))
    if TABLE_NAME in tables:
        print(f"{rest_catalog.name}: table {TABLE_NAME} already exists in database {DATABASE_NAME}")
        return

    table_schema = create_basic_schema()
    rest_catalog.create_table(identifier=f"{DATABASE_NAME}.{TABLE_NAME}", schema=table_schema)
    print("Table created")


#     basic_table = catalog.load_table(f"{DATABASE_NAME}.{TABLE_NAME}")
# data = [
#     pa.array([1, 2, 3], type=pa.int32()),
#     pa.array(["Alice", "Bob", "Charlie"], type=pa.string()),
#     pa.array(["Smith", "Jones", "Brown"], type=pa.string()),
#     pa.array(
#         [
#             date(1990, 1, 1),
#             date(1985, 6, 15),
#             date(2000, 12, 31),
#         ],
#         type=pa.date32(),
#     ),
# ]
#
# table_data = pa.Table.from_arrays(data, schema=table_schema)
# basic_table.append(table_data)
# print(f"{rest_catalog.name}: appended data to {TABLE_NAME}")


if __name__ == "__main__":
    main()
