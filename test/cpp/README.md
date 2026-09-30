# Positional delete unit tests

Add `-DICEBERG_BUILD_CPP_TESTS=ON` to the CMake configuration (or
`EXT_FLAGS='-DICEBERG_BUILD_CPP_TESTS=ON'` to `make release`). This target does not
require DuckDB's `ENABLE_UNITTEST_CPP_TESTS` or a REST catalog.

```sh
cmake --build build/release --target iceberg_delete_unittest -j3
build/release/extension/iceberg/iceberg_delete_unittest '[positional-delete]'
```

The optional microbenchmark compares bitmap construction and range filtering to
the previous unordered-set implementation at sparse and dense deletion rates.
Run it separately on an idle machine; timings are informational.

```sh
build/release/extension/iceberg/iceberg_delete_unittest '[positional-delete-benchmark]'
```
