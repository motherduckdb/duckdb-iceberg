# Catalog test capabilities

Catalog configurations declare sqllogictest capabilities in `test_env`. Tests
declare their requirements near the top of the file, before SQL or includes:

```text
require-env CATALOG_TEST_CONFIG_SETUP

require-env FORMAT_V3_SUPPORT true

require-env UNKNOWN_SUPPORT true
```

Each requirement must match. Multiple accepted values on one directive are
alternatives, for example `require-env SCAN_PLANNING_MODE client server`.
`require-env-not NAME value [value ...]` excludes the listed values. Both forms
skip the test if the variable is undefined; absence does not mean support.
Use positive requirements for capabilities, and explicitly define every flag in
each catalog config. Config `test_env` values take precedence over ordinary
process environment variables unless explicitly passed through by the runner.

Boolean capabilities use the strings `true` and `false`. `unknown` is reserved
for a capability that needs validation before enabling its tests; it also fails
`require-env NAME true`. These values describe the configured catalog version,
credentials, and execution mode, not every deployment of that catalog product.
The initial declarations preserve the existing test policy; `true` is not a
claim that a fresh cross-catalog certification was performed.

## Flags

| Flag | Requirement |
| --- | --- |
| `FORMAT_V3_SUPPORT` | Create and commit format-version 3 tables. Individual V3 features may need additional flags below. |
| `UNKNOWN_SUPPORT` | Store Iceberg `unknown` columns and evolve them to concrete types. Does not specify whether a catalog rejects UNKNOWN in V2. |
| `VARIANT_SUPPORT` | Store VARIANT columns. Does not promise support for every nested writer representation. |
| `GEOMETRY_SUPPORT` | Store geometry columns and preserve their Iceberg type metadata. |
| `TIMESTAMP_NS_SUPPORT` | Store nanosecond timestamp and timestamptz columns, including schema evolution and partition transforms exercised by the tests. |
| `COLUMN_DEFAULT_SUPPORT` | Preserve and apply column defaults on creation and schema evolution. Specific default types may have additional requirements. |
| `TIMESTAMP_NS_DEFAULT_SUPPORT` | Preserve defaults on nanosecond timestamps without a time zone. |
| `TIMESTAMPTZ_NS_DEFAULT_SUPPORT` | Preserve defaults on nanosecond timestamps with a time zone. Kept separate because Polaris currently has only the former default-type exception. |
| `TIMETRAVEL_SUPPORT` | Resolve historical snapshots for version/timestamp reads, historical scan plans, and rollback. |
| `SNAPSHOT_HISTORY_SUPPORT` | Expose the committed snapshot history and its operation/summary metadata. Used by snapshot inspection tests that do not perform time-travel reads. |
| `METADATA_LOG_SUPPORT` | Return metadata-log entries needed to reconstruct table state at transaction start. Distinct from the snapshot list. |
| `METADATA_FILE_READ_SUPPORT` | Allow the client to read metadata JSON files using the configured storage credentials, including historical files. |
| `STAGED_CREATE_SUPPORT` | Support staging a table creation before its commit. |
| `MULTI_TABLE_COMMIT_SUPPORT` | Support the REST multi-table transaction commit endpoint. |
| `FILE_CLEANUP_SUPPORT` | Allow deletion of uncommitted files using the configured credentials. |
| `NAMESPACE_PROPERTIES_SUPPORT` | Support setting and removing namespace properties. Does not require an automatically populated `namespace_id`. |
| `CASE_SENSITIVE_TABLE_NAMES_SUPPORT` | Support distinct table names differing only in case. |
| `ATOMIC_COMMIT_CONFLICT_SUPPORT` | Reject concurrent commits with stale parent state so retries preserve every successful write. Enabled for Lakekeeper and S3 Tables; false for the standard fixture and unknown for the other configurations pending validation. |
| `SCAN_PLANNING_MODE` | `client` or `server`. Tests asserting client pruning logs or request counts require `client`; fixture-latest uses `server`. |

Transaction-start reconstruction can require both `METADATA_LOG_SUPPORT` and
`METADATA_FILE_READ_SUPPORT`. Do not label these tests as time travel merely
because they read an older transaction state. Tests using V3 types/defaults
declare the particular feature as well as format support where applicable.

## Maintaining the declarations

When adding a test, declare the capabilities it actually exercises. Keep
`CATALOG_TEST_CONFIG_SETUP` for catalog initialization and genuinely
catalog-specific tests; prefer capabilities over catalog-name allowlists in
`catalog_agnostic`. When a catalog gains support, update its flag and run the
affected tests. Include new flags in all configs, including
`fixture_duckdb_tests.json`, which can also run extension tests.

Keep `skip_tests` for known bugs, unexplained failures, hardcoded fixture paths,
and unavailable generated data. These are not evidence of absent capabilities.
For example, the S3 Tables UNKNOWN skips describe acceptance behavior that
differs from test expectations, and Gravitino's namespace-property skips assume
an existing `namespace_id`. Neither justifies declaring the feature unsupported.
The existing server-planning bug exceptions also remain explicit skips.

Capability gates apply to whole files. Split tests when unrelated assertions
would otherwise lose useful coverage; in particular, client-planning assertions
can eventually be separated from result checks to run the latter in both modes.

Python capability declarations are independent and unchanged by this migration.
This document and the JSON flags govern sqllogictests only.

Validate JSON with `jq empty test/configs/*.json`, and run affected tests serially
with the active catalog config. Do not start or switch catalogs just to validate
documentation. Review changes to skip coverage as well as JSON syntax: a removed
path must have an equivalent capability gate (or be an obsolete path).
