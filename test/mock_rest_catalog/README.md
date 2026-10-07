# Mock REST catalog tests

This test-only Python standard-library server exercises the real Iceberg HTTP client
and local Parquet/Avro writes without Docker, Spark, or an object store.

## Existing catalog workflow

Run from the repository root with an existing extension-enabled build:

```sh
make mock
./build/reldebug/test/unittest --test-config test/configs/mock.json \
  test/sql/local/catalog_test_config_setup/catalog_agnostic/create/test_create_table.test
make mock-stop
```

`make mock` follows the existing catalog-switching workflow: it stops the active
catalog, starts a fresh mock, and records `mock` in `.catalogs/.active_catalog`.
The server runs in the background and shares state across normal unittest
invocations until stopped. `scripts/catalog_test_config.sh` resolves the mock
config, so `--test-config "$(scripts/catalog_test_config.sh)"` also works.

The fixed loopback endpoint is defined in [mock.json](../configs/mock.json);
the launcher reads it from that file. If the port is occupied, startup fails.
There is no Docker or pip setup. Each start creates a new run directory under
`.catalogs/mock/` containing its warehouse, request journal and server log.
`make mock-stop` shuts down the owned server through a token-authenticated control
endpoint, clears the active marker if it names `mock`, and retains the run files.
Starting again uses an empty catalog; it does not reload old warehouse metadata.

Use explicit test filters: [selection.json](selection.json) lists the eight verified
files. Passing only `--test-config` does not restrict unittest to those files, and
the mock does not yet support the entire suite. Run test files serially against
the shared catalog. Python integration tests and Spark data generation remain
outside this backend's scope.

## Isolated acceptance runner

For independent warehouses and strict reporting of skips and unsupported requests:

```sh
python3 -m unittest discover -s test/mock_rest_catalog -v
python3 scripts/run_mock_catalog_tests.py --unittest-binary build/reldebug/test/unittest
```

To run one admitted file:

```sh
python3 scripts/run_mock_catalog_tests.py \
  --unittest-binary build/reldebug/test/unittest \
  create/test_create_empty_table.test
```

The wrapper always runs an attachment/create/append/fresh-connection/drop smoke
test first. It then runs the exact files in [selection.json](selection.json), serially,
with a new server and warehouse per file. Each server binds to an OS-assigned
loopback port. Additional connections within a file share catalog state.
The wrapper does not read or change `.catalogs/.active_catalog`.

[mock.json](../configs/mock.json) owns attachment SQL and support flags.
The wrapper replaces the fixed URI in a temporary config and removes inherited
catalog support flags. The build must support `--test-config`, `--stdin`, and
`--emit-test-events` (as the current DuckDB submodule does). Missing registrations,
zero assertions, skips, unsupported requests, server exceptions, and timeouts fail
the run. The summary counts selected SQL files separately from the smoke test.
Normal unittest invocations report their own SQL results; only the wrapper also
fails the run for every unsupported server request, including one hidden by an
expected-error assertion. The managed server reports unsupported/error counts on stop.

Successful runs remove their temporary warehouse. Failures retain `config.json`,
`unittest.log`, `requests.jsonl`, and all warehouse files in the printed diagnostics
directory. Large request/response journal payloads are truncated to 16 KiB;
published metadata files remain complete. `--timeout` sets a per-file limit in
seconds (default 120). Set `TMPDIR` before launching to choose the parent directory
for diagnostics. Interruption and timeout stop the child and close the server.

## Implemented contract

- Namespace creation, lookup, listing and nonempty-safe deletion.
- v2 table creation with unique local locations, including staged creation.
- Staged UUIDs distinguish competing attempts; only a successful `assert-create`
  commit publishes a table. Abandoned staged files live until wrapper cleanup.
- Create-time schema/spec/order updates, properties, append snapshots and the
  main snapshot reference.
- Optimistic requirements and ordered updates are validated against private
  candidates. HTTP requests hold the catalog lock through validation and
  publication. Metadata files are immutable and completed before publication.
- Loading, listing and unregistering tables. Unregister leaves storage files intact.

Coverage is explicitly limited to the eight selected SQL files and the smoke test.
Independent protocol tests cover conflicts, failed publication, exact 64-bit
snapshot IDs, identifier escaping, immutable files, and runner failure detection.
This is not a general-purpose or production catalog.

History reconstruction/time travel, multi-table commits, v3, views, purge,
relocation, non-append snapshot operations, credentials, and server-side scan
planning remain outside this milestone. The config advertises only implemented
endpoints and enables only the staged-create support gate. Real-catalog CI remains
necessary for interoperability testing.

The dedicated `test-mock-catalog.yml` workflow runs these checks with the existing
Linux relassert build artifact. Local acceptance was verified with the available
macOS reldebug build; the new GitHub Actions job still needs its first remote run.
