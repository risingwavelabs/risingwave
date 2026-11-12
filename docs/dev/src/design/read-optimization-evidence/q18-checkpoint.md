# Q18 read optimization checkpoint

These artifacts record completed TPC-H SF1 Q18 experiments from November 12,
2026. The [design](../hummock-read-scheduling.md) describes the current code
and the [experiment record](../read-optimization-testing.md#tpc-h-sf1-q18-locality-backfill-reproduction)
describes the historical runs.

| Artifact | Contents |
| --- | --- |
| [Q18 SQL](./q18.sql) | MV definition and session settings. The inputs were DuckDB 1.4.4 TPC-H SF1 customer, orders, and lineitem, with 150,000, 1,500,000, and 6,001,215 rows. |
| [Q64 TOML](./q18-q64.toml), [Range TOML](./q18-range.toml) | Effective worker configuration for the repeat and historical Range control. |
| [I/O distributions](./q18-io-distribution.json) | Successful physical GET sizes, logical read sizes, group sizes, gap sizes, and accounting for Range and the two Q64 whole-window runs. |
| [Admission comparison](./q18-admission-comparison.json) | Historical Range, Q32, traced Q32, and Q128 comparison with per-arm measurements. |
| [Q32 trace analysis](./q18-gap-trace-complete.json) | Matched complete windows and split attribution. Its source path names the original local trace. |

The runs used a fresh `risedev d full` cluster with one 2 GiB CN, four-way
streaming parallelism, MinIO, and the same cold seed at Hummock version 392.
The completed Q64 distribution repeat took 422.291 seconds, issued 47,508
successful CN GETs, read 5.586 GiB, and passed the independent 100-row oracle.
Source-scan phases are split at the first five-second sample that reached
13,652,430 source rows. The repeat retained bucket samples at that boundary.
For earlier runs with aggregate samples, phase histograms are unavailable.

| Historical binary | SHA-256 |
| --- | --- |
| Range, Q32 trace, and original admission comparison | `12635d17893492c8b5ee64d6dcaa397f6d3856ac132bca3454d0aac17c6732f9` |
| Q64 whole-window and distribution repeat | `dc3cd7db780e88b511f89e1dd96d37c61441b9f48a6c02a0aa3a143e4626efbc` |

Both historical binaries used workspace base `817b276bcf69917c61b093f4251d080035d2e618`
plus their respective working changes. The Q64 binary predates the preparation
permit cleanup in this checkpoint. The worker configuration also changed
between Range and Q64, so the numbers do not isolate one configuration switch.

The compact artifacts contain aggregate metrics and trace attribution. Full
Prometheus scrapes, raw trace logs, binaries, and the cold seed remain in the
original local experiment directories under `target/locality-tpch-q18`.
