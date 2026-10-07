# Mock REST catalog

This test-only Python standard-library server exercises the real Iceberg HTTP client
and local Parquet/Avro writes without Docker, Spark, or an object store.

Run from the repository root with an existing extension-enabled build:

```sh
make mock
./build/reldebug/test/unittest \
  --test-config "$(scripts/catalog_test_config.sh)" \
  "test/sql/local/catalog_test_config_setup/catalog_agnostic/*" \
  "exclude:*.test_slow"
make mock-stop
```

`make mock` stops the active catalog, starts a fresh mock, and records `mock` in
`.catalogs/.active_catalog`. The server runs in the background and shares state
across normal unittest invocations until stopped. You can also pass
`--test-config test/configs/mock.json` directly or select an individual SQL test.
Run test files serially against the shared catalog.

[mock.json](../../test/configs/mock.json) owns the fixed loopback endpoint,
attachment SQL, capability flags, and skip policy. The launcher reads the endpoint
from that file and fails if the port is occupied. Python integration tests and
Spark data generation are outside this backend's scope.

Each start creates a fresh run directory under `.catalogs/mock/` containing the
warehouse, request journal and server log. `make mock-stop` shuts down the owned
server through a token-authenticated control endpoint, clears the active marker
if it names `mock`, and retains the run files. Restarting does not reload old
warehouse metadata. Large request/response journal payloads are truncated to
16 KiB; published metadata files remain complete.

The mock supports basic namespace operations, staged v2 table creation, create-time
metadata updates, append/delete/overwrite/replace snapshots, optimistic requirements,
table loading/listing, and unregistering tables. Committed snapshots are retained
and exposed through the snapshot-history capability. Metadata files are immutable
and completed before publication. Unregistering leaves storage files intact.

History reconstruction/time travel, multi-table commits, v3, views, purge,
relocation, credentials, and server-side scan planning remain unsupported.
The SQL config controls exclusions and expected
unsupported errors; a passing suite does not imply support for skipped behavior.
The server reports unsupported/error counts on stop.

The `test-mock-catalog.yml` workflow runs the catalog-agnostic SQL suite with the
Linux relassert build artifact, excluding `.test_slow`, and uploads `.catalogs/mock`
on failure. Real-catalog CI remains necessary for interoperability testing.
