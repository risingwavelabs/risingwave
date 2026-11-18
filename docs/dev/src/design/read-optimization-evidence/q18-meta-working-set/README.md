# SF10 Q18 HashJoin metadata working set

This experiment samples the production HashJoin prefix-iterator path on
TPC-H SF10 Q18, then compares metadata retention budgets after recovery
from the same stopped PostgreSQL and MinIO snapshot.

The release binary was built from
`98126f898412f675ebdd406c35cd273f4972dd2b` plus the
[sample hooks](sampling-instrumentation.patch.gz). See
[build identity](build-identity.json) for the command and binary SHA-256.
All CN measurements use the same binary. Only the repository's test
license crate has debug assertions. The main release binary uses
optimizations at level 3.

## Working set and limit

The capture starts from the v1346 SF10 seed with 1.5 million customer
rows, 15 million orders and 59,986,052 line items. It uses
[Q18 with six LocalityProvider nodes](plan.txt). Fragment 24 sends the
left probe input to HashJoin fragment 23. That path probes state table
38. One in 64 eligible input chunks records the actual Foyer weight of
each distinct SST meta handle in first-touch order and its visit count.
This includes both handles retained by the batch and handles rejected
by its budget.

All 97 samples match the frozen SST membership and contain 11 distinct
SST objects apiece. The median total weight is 8.805 MiB, the range is
8.448–8.806 MiB, and the largest handle is 2.047 MiB. Every sample has
four handles over 1 MiB. The 16-entry cap therefore leaves room for
every observed object. A 1 MiB regular budget and one extra oversized
slot retain four of the 11 handles, and subsequent probes look up the
other seven in Foyer again.

| Regular budget | Repeated visits served by the batch, modeled |
| ---: | ---: |
| 1 MiB | 36.36% |
| 3 MiB | 18.18% |
| 4 MiB | 27.27% |
| 8 MiB | 81.91% |
| 9 MiB | 100.00% |

The model uses the recorded access order, first oversized handle and
monotonic regular byte and entry budgets. The dip at 3–4 MiB follows
from first-touch admission. Handles that previously used the oversized
slot now fit within and consume the regular budget. 9 MiB is the smallest
whole-MiB budget that retains every sampled handle. See
[chunk samples and the full budget sweep](chunk-samples-and-budget-sweep.json).

## Recovered comparison

The capture pauses fragment 24 inside the same job after 1,573,674
reported probes, lets barriers settle, and stops the services. Recovery
clones the same stopped database and object namespace for each arm.
The table-38/39 [SST layout](frozen-table-38-39-layout.tsv) has 95 SST
entries and SHA-256
`7a535eb1dc8c00b422424576719a8b7cab8dac88d013431b09c30409e9a8d00b`
over its lines joined by LF without a trailing LF. The layout and
[vnode groups](vnode-placement.json) are checked after each recovery.
The original SF10 input has SHA-256
`355ce8fecdb67bd601fb5f3cf0f7976cf22f56830d2432ec352f96ee3053b0aa`
over the source SST catalog projection.

Each arm starts with cold Foyer memory caches and the same
`risedev d full` topology. The CN has 12 slots, 8 streaming workers,
and 6 dedicated I/O workers. Q18 uses fixed job parallelism 4.
[Read controls](run-controls.toml) include ColdRanges N16, a 4 GiB
prefetch budget, G128/P32/B16, a 1 MiB maximum physical read, and a
64 KiB maximum gap. Foyer disk cache, fastrace, and metadata sampling
are off. The runs use the same local exclusion guard. Automatic
compaction is disabled in the recovered test arms to freeze the
layout. The only arm-level configuration difference is
`storage.read_batch.meta_max_bytes`.

The comparison uses the first 2.5 million reported HashJoin probes
after resuming. The first 1 MiB attempt continued beyond that window
and stalled at 3,809,844 probes with L0 write-stop active for downstream
compaction group 17. The target SSTs were still in group 18 with the
expected layout. The comparison excludes that stall and uses the same
probe horizon in every arm. One-second scrapes bracket the horizon.
Cumulative metrics and elapsed time are linearly interpolated across
that interval. RSS is the sampled process peak through the upper scrape.
HashJoin time is the sum of the four actors' left match timers,
including awaited storage work and excluding downstream yield time.

The 9 MiB bootstrap required a retry when the original CN boot
encountered a gap between its pinned version and the next Hummock
delta before starting probes. The retry restored the same sealed
snapshot. Startup time is outside the measurements.

## Results

The arm order was 1, 9, 9, 1 MiB. All four arms reached the same
2.5 million reported probes and misses without a retry in the timed
window. The Bloom and iterator counters are flushed independently
and differ by up to 1.88% at the cut. All arms have approximately 11
Bloom checks per observed iterator initialization. Per-iterator
normalization gives the same result for initialization time.

| Metric | a1 (1 MiB) | b1 (9 MiB) | b2 (9 MiB) | a2 (1 MiB) | B change vs A average |
|---|---:|---:|---:|---:|---:|
| Foyer meta queries | 17,745,612 | 111,827 | 111,681 | 17,628,281 | -99.37% |
| Foyer meta misses | 60 | 62 | 59 | 59 | +1.68% |
| Per-chunk meta hits | 10,078,184 | 28,046,497 | 28,010,257 | 10,011,546 | +179.03% |
| Retain rejects | 17,706,050 | 1,720 | 1,704 | 17,588,978 | -99.99% |
| HashJoin active actor-seconds | 297.354 | 75.665 | 77.270 | 289.729 | -73.95% |
| Iterator init actor-seconds | 250.885 | 47.305 | 48.415 | 244.836 | -80.69% |
| Wall seconds to 2.5M probes | 78.080 | 19.196 | 19.610 | 74.308 | -74.53% |
| Baseline CN RSS GiB | 0.179 | 0.177 | 0.180 | 0.178 | +0.13% |
| Peak CN RSS GiB | 1.831 | 2.210 | 2.255 | 1.857 | +21.08% |
| CN CPU seconds | 427 | 136 | 139 | 414 | -67.30% |
| CN object GETs | 9,362 | 9,514 | 9,485 | 9,289 | +1.87% |
| CN read GiB | 0.985 | 1.103 | 1.104 | 0.969 | +12.92% |

The 99.37% reduction is in logical Foyer lookups for table 38. The
batch still checks Bloom filters and probes for matching rows. Actual
metadata misses stayed around 60 per run. The iterator initialization
timer accounts for most of the observed improvement in the HashJoin
timer. CN object store metrics include other work in the same phase.

Peak RSS rose by 0.389 GiB across the two pairs. The retained weight
difference from the sampled 1 MiB policy is at most about 23.1 MiB
across four simultaneously active batches on this side of this
HashJoin. At the three available end scrapes, total Foyer metadata
weight increased from 46.1 MiB to 51.1–51.6 MiB, while uploader
immutable bytes increased from 24.6 MiB to 99.0–108.3 MiB. The
resident-minus-active jemalloc footprint was 239–350 MiB larger.
These observations separate the extra process RSS from the weight
charged to this join's sampled batches. They do not identify the
allocation sites of all extra pages.

9 MiB fits this Q18 layout and is the budget tested here. The samples
cover an early portion of this layout's probe phase. The comparison
measures a fixed probe prefix on local MinIO. A production default
also depends on other SST layouts and CN memory under concurrency.

The [comparison JSON](comparison.json) contains the per-arm data and
scrape brackets. The per-arm folders hold the raw phase samples and
process cleanup confirmations. RiseDev services and clone data for the
arms were removed. The original seed and stopped intermediate
checkpoint were retained locally.
