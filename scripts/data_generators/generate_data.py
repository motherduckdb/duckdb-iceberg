from scripts.data_generators.tests import IcebergTest
import argparse


parser = argparse.ArgumentParser(description="Generate data for various systems.")
parser.add_argument(
    "targets",
    nargs="+",
    choices=["polaris", "lakekeeper", "local", "spark-rest"],
    help="Specify one or more catalogs/targets to generate for",
)
parser.add_argument("--test", help='Generate only a specific test (for debugging)', action='store')
parser.add_argument("--target", help='Override registry target for the catalog (advanced)', action='store')
parser.add_argument("--conn", dest="conn_kv", action="append", help="Connection kwargs as key=value (repeatable)")

args = parser.parse_args()

test_classes = IcebergTest.registry
all_tests = []
actual_tests = []

for test_class in test_classes:
    all_tests.append(test_class())

if args.test:
    actual_tests = [x for x in all_tests if x.table == args.test]
else:
    actual_tests = all_tests

connection_kwargs = {}
if args.conn_kv:
    for kv in args.conn_kv:
        if '=' in kv:
            k, v = kv.split('=', 1)
            connection_kwargs[k] = v

for catalog in args.targets:
    print(f"Generating for '{catalog}'")
    for test in actual_tests:
        print(f"Generating test '{test.table}'")
        test.generate(catalog, target=args.target, connection_kwargs=connection_kwargs)

if __name__ == "__main__":
    if __package__ is None:
        raise RuntimeError(
            "This script must be run as a module.\n" "Use: python -m scripts.data_generators.generate_data [args]"
        )
