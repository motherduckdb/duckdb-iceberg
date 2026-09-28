# `iceberg_scan_tasks` benchmarks

These catalog-independent benchmarks measure execution of pre-materialized scan
tasks. Each workload has a one-thread baseline and an eight-thread counterpart.

| Case | Tasks | Rows per task | Purpose |
| --- | ---: | ---: | --- |
| `tiny_2048` | 2,048 | 32 | One vector of tasks; reader setup and scheduling overhead |
| `tiny_16384` | 16,384 | 32 | Multiple input vectors; sustained task turnover |
| `multi_row_group` | 64 | 262,144 | 32 Parquet row groups per task; sharing work within active tasks |
| `skewed` | 4,096 | First task: 8,388,608; remaining: 32 | A long-running task mixed with many tiny tasks |

The task table is deliberately small enough to fit in one DuckDB storage row
group. Increasing `threads` alone need not parallelize its scan. This exposes the
current dependence on upstream task-input parallelism and provides a baseline
for sharing active task execution through global state. The eight-thread cases
do not assert a speedup; compare their timings before and after executor changes.

Setup writes deterministic local Parquet files with field IDs and 8,192-row
groups, reads their sizes, and materializes the complete task descriptors. Only
the `iceberg_scan_tasks` query and its aggregation are timed. Every run checks
the row count, both physical column sums, and a task-specific partition constant
sum to detect omitted or duplicated work. Checksums force data decoding instead
of relying on a count-only scan.

Tasks reuse one data file (two for the skewed case). This keeps setup and disk
space small and emphasizes CPU, reader initialization, and scheduling. These
are warm-filesystem microbenchmarks, not measurements of distinct-file or remote
I/O throughput. DuckDB's external file cache is disabled; the OS cache is not.
There are no delete files or catalog requests in the timed path.

From the repository root, build and run serially:

```sh
# Set VCPKG_TOOLCHAIN_PATH if your environment does not already provide it.
BUILD_BENCHMARK=1 make reldebug
./build/reldebug/benchmark/benchmark_runner 'benchmark/scan_tasks/.*' --timed-runs 5
```

The runner performs a warmup before the timed runs and verifies the results.
Generated files live in `duckdb_benchmark_data/` and are overwritten on subsequent
runs. Do not commit them or run multiple instances of the same case concurrently.
For revision comparisons, use the same build configuration, hardware, thread
counts, and otherwise idle machine; retain the per-run timings and compare
medians for each case and the one/eight-thread ratios. No wall-clock threshold is
asserted because it would depend on the machine.
