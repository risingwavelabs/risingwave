### SF10 Q18 ColdRanges fixes, six I/O workers, 2026-11-18

One guarded `risedev d full` arm used source
`0bb0df4acf388a958e9592787ec36ceebcb376ed` on
`li0k/io-read-scheduler-simplify`. The same TOML, v1346 cold APFS seed
clone, source SST fingerprint, fixed4 plan, six LocalityProviders,
53 actors, 12 advertised slots and 136,472,104 snapshot rows matched
the previous six-worker measurement. The CN had eight streaming and
six dedicated I/O workers before and after the query. G128/P32/B16,
N16/1 MiB, zero scheduler gap, in-flight reuse, MultiGet and metadata reuse
stayed enabled. Foyer file caches, tracing, async stack traces and iterator
profiling stayed off. Local MinIO was the object backend.

The native release used the previous build command with
`CARGO_INCREMENTAL=0`, `CARGO_PROFILE_RELEASE_SPLIT_DEBUGINFO=unpacked`,
`--no-default-features`, `-j 4`, and only
`profile.release.package.risingwave_license.debug-assertions=true`.
The application crates had optimization level 3 and debug assertions
disabled. Binary SHA-256 was
`81fc8b116fcd1522f87eef68195d914ade84bfabfc2c0f29bc92eef56c0d78ed`.
Build artifacts and startup executable ownership were verified before
measurement. Build time was 14m 28s. No build overlapped the query.

| Metric | Previous ColdRanges 6 workers | Fixed ColdRanges 6 workers |
| --- | ---: | ---: |
| CREATE MV wall, s | 717.82 | 677.11 |
| Source checkpoint, s | 86.03 | 96.25 |
| Source complete from 1 s scrapes, s | 83.00–84.01 | 91.00–92.01 |
| Source physical GETs | 35,938 | 221,199 |
| Source GETs at most 64 KiB | 765 | 195,121 |
| Source read GiB / mean GET KiB | 12.548 / 366.1 | 12.851 / 60.9 |
| Source cumulative physical GET time, s | 147.09 | 597.34 |
| After-source physical GETs | 82,431 | 83,208 |
| Total GETs / read GiB | 118,369 / 36.350 | 304,407 / 36.779 |
| Physical GET mean / p99 / p99.9, ms | 2.298 / 32 / 64 | 2.358 / 16 / 32 |
| ObjectStore caller mean / p99 / p99.9, ms | 12.211 / 378.77 / 568.15 | 8.124 / 252.51 / 378.77 |
| After 5 s admission / dispatch p99, ms | 0.01 / 5.12 | 40.96 / 40.96 |
| Source peak sampled prefetch accounting, MiB | 3000.73 | 4096.00 |
| Source denied prefetch windows | Counter unavailable | 199,920 |
| Whole CN peak RSS, MiB | 6,014 | 4,371 |

The physical GET increase is concentrated in source scanning: 185,261
of 186,038 additional GETs occurred before the source checkpoint.
Total read volume rose about 1.18%, while the source request size fell
from 366.1 to 60.9 KiB. Histogram percentiles are upper bucket bounds.
ObjectStore caller means compare different request populations after
the fallback expanded logical reads. Cumulative service time sums
parallel reads and is not query elapsed time.

#### Budget rejection and fallback

`ColdRangePrefetch::reservation_bytes` now reserves compressed bytes
plus a conservative decoded weight and bookkeeping for every block.
`Block::estimated_decode_memory_weight` bounds restart-point count
using the entire uncompressed payload. This is a deliberate worst-case
estimate, not measured retained memory. The full-window reservation uses
atomic `MemoryUsageTracker::try_new`. On failure,
`prefetch_cold_ranges` increments `budget_denied` and returns None.
The iterator then uses ordinary `get_with_read_plug` for the demand
block. It does not shorten the window to fit the remaining capacity.

From 6 to 95 seconds, 81 of the 89 one-second samples had more than
3.99 GiB charged to prefetch. The previous run's corresponding sampled
median was 1.68 GiB and maximum 2.78 GiB. The new run rejected 199,920
window reservations, all before source completion. After-source rejects
were zero. The failed-whole-window path and the 195,121 source GETs at
most 64 KiB support attribution of the GET regression to budget pressure
and per-block fallback. The old build lacked the rejection counter.
We have not tested a budget adjustment or an implementation change.

#### Singleton sharing, overlap, and elapsed time

The new path registered 49,828 singleton Fill reads through Foyer,
including 49,209 after source completion. In provider interval 24→22,
table 38 direct-prefetch segment count went from 42,462 to 1, while
ordinary data misses went from 401 to 42,646. Singleton reads have moved
to the intended shared block-fetch accounting. The code change and
existing targeted UT evidence cover producer sharing; a total miss count
does not establish how many physical duplicates were avoided.

The new partial-flight metric recorded 111 dispatches and 7,030,955
intersection bytes (61 dispatches before source completion). Buffered
overlap recorded 6,328 Fill outer fetches and 132,193,671 bytes, including
6,093 / 117,336,266 before source completion. NotFill and Disable were
zero. These are reuse-opportunity observations with different scopes.
They must not be added together or treated as an exact count of duplicate
physical GETs. The measured partial-flight count is small relative to
the 199,920 denied windows.

The 40.71 s lower total wall time accompanies a downstream 22→20 phase
reduction from 182.00 to 143.01 s. Table 32 SST candidate checks fell
from 121.02M to 81.29M. The 24→22 phase stayed about 256 s, while table
38 candidates rose from 210.00M to 239.15M. Intermediate state is generated
during the query. This single run shows an IO regression in source scans,
and does not establish a wall-time speedup of the fixed prefetch.

All 100 output-oracle rows matched, with no measured read errors or retries.
Minimum sampled free disk was 66.50 GiB. Owned runtime services, listeners,
containers and the temporary MinIO/Postgres clone were removed after the
run. The unchanged seed and other sessions' worktrees were retained.

The local archive is
`target/locality-tpch-q18/sf10-q18-cold-ranges-0bb0df4acf-io6.c2jh4g2n/`.
`fixed-vs-previous.json` retains both compared phase reports, controls,
binary manifests, histograms and overlap observations.
`fixed-vs-previous.md` retains the detailed numeric comparison.
Source-phase outer-fetch counts are unavailable because this series was
not selected by the earlier lightweight sampler; whole-query values
come from complete before/after scrapes.
