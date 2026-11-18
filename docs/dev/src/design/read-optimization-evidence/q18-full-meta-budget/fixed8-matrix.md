# Aligned fixed-8 SF10 Q18 main versus optimized

Median paired CREATE MV change was **-34.3%**
(-222.24 s). The per-version median elapsed times
were 648.05 s and 425.81 s.

## Controls

Three pairs ran in AB, BA, AB order under a shared host lease.
A used clean main ancestor `88b9811433`
and legacy Range prefetch. B used optimized `98126f8984`,
ColdRanges, 9 MiB SST-meta reuse, native MultiGet, six dedicated I/O
workers, G128/P32/B16, 64 KiB gaps, and 1 MiB maximum physical range.
The archived optimized opt3 release contains an inactive sampling hook.
Both release binaries used the repository test license build and
were reused across the matrix.

Each arm started an isolated `risedev d full` with fresh Foyer memory
and a fresh APFS clone of the same stopped v1346 fixture. Host page
cache was left in its normal state. Both used 12 advertised slots,
fixed SQL parallelism 8, eight streaming runtime workers, 12 batch
workers, 4 GiB prefetch budget, N16, Foyer disk cache and tracing off,
and regular automatic compaction. The six-provider EXPLAIN, 105 job
actors, vnode partitions, exact 136,472,104 source rows and independent
100-row oracle matched. The source SST hash was `355ce8fecdb67bd601fb5f3cf0f7976cf22f56830d2432ec352f96ee3053b0aa`.

## Per-run results

| Pair | Order | Run | Full s | Source s | HashJoin phase s | CN CPU s | Peak RSS GiB | GETs | Read GiB | Backend mean ms | Backend p99 upper ms |
| ---: | :---: | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 1 | AB | main | 860.01 | 85.35 | 305.01 | 4,847 | 5.748 | 126,200 | 37.757 | 18.925 | 512 |
| 1 | AB | opt | 490.13 | 83.34 | 109.02 | 2,646 | 5.620 | 121,745 | 36.198 | 2.753 | 32 |
| 2 | BA | opt | 425.81 | 76.28 | 70.00 | 2,517 | 5.624 | 120,965 | 36.318 | 2.341 | 16 |
| 2 | BA | main | 648.05 | 83.37 | 130.00 | 3,652 | 5.626 | 125,323 | 37.780 | 16.817 | 512 |
| 3 | AB | main | 642.86 | 82.30 | 129.00 | 3,623 | 5.703 | 127,153 | 37.328 | 19.097 | 512 |
| 3 | AB | opt | 423.20 | 77.33 | 73.01 | 2,512 | 5.516 | 117,506 | 36.281 | 2.236 | 16 |

## Phase and resource medians

For each metric, compute the main and optimized medians independently.
The last column is the median of the three within-pair percentage
changes. Backend latency buckets bound percentiles. They describe the
backend read future and exclude scheduler admission and dispatch.
Source measurements include overlapping operators on the same CN.

| Metric | Main median | Optimized median | Median paired change |
| --- | ---: | ---: | ---: |
| Full CREATE MV, s | 648.05 | 425.81 | -34.3% |
| Source scan, s | 83.37 | 77.33 | -6.0% |
| DDL start to provider 24, s | 256.14 | 191.13 | -21.7% |
| Provider 24 to 22, s | 130.00 | 73.01 | -46.2% |
| Provider 22 to 20, s | 113.00 | 110.01 | -4.5% |
| Provider 20 to MV complete, s | 155.85 | 57.96 | -62.8% |
| CN CPU, s | 3,652 | 2,517 | -31.1% |
| CN peak RSS, GiB | 5.703 | 5.620 | -2.2% |
| Target HashJoin Bloom checks | 80,331,564 | 74,978,204 | -23.2% |
| Target HashJoin Foyer meta queries | 80,331,716 | 293,306 | -99.7% |
| Target HashJoin Foyer meta misses | 25 | 5 | -77.4% |
| Target HashJoin iterator init, actor-s | 770.60 | 301.43 | -65.4% |
| Target HashJoin phase CN GETs | 46,087 | 46,700 | +1.3% |
| Target HashJoin phase backend GET mean, ms | 1.087 | 1.095 | +0.7% |
| Target HashJoin phase backend GET p99 upper bound, ms | 8 | 8 | +0.0% |
| Whole run physical GETs | 126,200 | 120,965 | -3.5% |
| Whole run read GiB | 37.757 | 36.281 | -3.9% |
| Whole run mean range, KiB | 313.7 | 314.8 | -0.4% |
| Whole run backend GET mean, ms | 18.925 | 2.341 | -86.1% |
| Whole run backend GET p99 upper bound, ms | 512 | 16 | -96.9% |
| Source interval physical GETs | 39,448 | 38,694 | -1.8% |
| Source interval read GiB | 13.298 | 12.443 | -6.4% |
| Source interval mean range, KiB | 353.5 | 337.2 | -4.7% |
| Source interval backend GET mean, ms | 36.956 | 3.426 | -90.9% |
| Source interval backend GET p99 upper bound, ms | 512 | 32 | -93.8% |
| Post-source interval physical GETs | 86,782 | 81,533 | -4.9% |
| Post-source interval read GiB | 24.456 | 23.819 | -2.2% |
| Post-source interval mean range, KiB | 295.5 | 307.9 | +2.8% |
| Post-source interval backend GET mean, ms | 10.628 | 1.816 | -82.9% |
| Post-source interval backend GET p99 upper bound, ms | 256 | 16 | -93.8% |

## Target HashJoin

The provider 24 to 22 phase contained 15 million target HashJoin
probes in each run. Intermediate SST layouts changed under normal
compaction. The table shows observed layout and lookup work together.

| Pair | Run | Bloom checks | Foyer meta queries | Batch reuse hits | Foyer meta misses | L0 at provider 24 / 22 |
| ---: | --- | ---: | ---: | ---: | ---: | ---: |
| 1 | main | 176,980,067 | 176,980,215 | 0 | 21 | 46 / 30 |
| 1 | opt | 74,978,204 | 293,306 | 74,684,099 | 5 | 41 / 21 |
| 2 | main | 76,630,344 | 76,631,042 | 0 | 25 | 42 / 18 |
| 2 | opt | 58,824,676 | 230,102 | 58,594,469 | 2 | 15 / 21 |
| 3 | main | 80,331,564 | 80,331,716 | 0 | 31 | 36 / 27 |
| 3 | opt | 100,552,432 | 833,552 | 99,718,698 | 7 | 22 / 22 |

Main's first pair ran 212 s longer than its second pair. The target
HashJoin phase accounted for 175 s of that difference. At the same
15 million probes, table 38 performed 177.0 million versus 76.6 million
Bloom checks. That stage's runtime-created SST shape differed.

In pair 3, the optimized arm ran 25.2% more Bloom checks and still
reduced iterator initialization from 770.6 to 301.4 actor-seconds.
CN GETs were 45,342 and 45,373 during that phase. The optimized path
bypassed Foyer on 99.17% of table 38 metadata accesses. It recorded
442,112 metadata retention budget rejections in this arm, with a
fallback to the ordinary Foyer lookup. The other two optimized arms
recorded zero budget rejections in the target phase. These runs
measure the whole configuration and its interaction with compaction.

## Final aggregation and materialization interval

The provider-20-to-MV interval was 149–159 s on main and 53–59 s
on the optimized release. HashAgg state table 27 went from
83.2–90.8 million Foyer metadata lookups to 8.9–9.6 million
while the optimized point reads checked more Bloom candidates
in each pair. Neither arm recorded a table-27 metadata miss
in this interval. The interval also includes downstream work,
so it does not isolate the contribution of MultiGet.

## Scheduler and prefetch

| Pair | Merged members | Inflight reuses | Successful groups | Scheduler gap MiB | Source admission p99 upper ms | Source queue p99 upper ms | Admission sum at first scrape ≥5s | ColdRanges denied / shrunk |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 1 | 6,791 | 64 | 118,930 | 73.89 | 655.36 | 40.96 | 99.930% | 0 / 0 |
| 2 | 8,013 | 111 | 118,214 | 99.90 | 327.68 | 40.96 | 99.958% | 0 / 0 |
| 3 | 5,952 | 53 | 114,879 | 61.62 | 327.68 | 40.96 | 99.947% | 0 / 0 |

Logical scheduler merges and inflight reuses count work inside the
optimized path. The cross-version GET delta counts actual physical
operations and includes the effect of legacy Range prefetch in main.
Scheduler admission can dominate the tail during startup even when
the physical GET histogram improves.

[Per-run measurements, effective configs, phase counters and layout fingerprints](fixed8-matrix.json).
The interrupted fixed-4 attempt is excluded. The six successful arms
cleaned up their owned processes, listeners, containers and test clones.
