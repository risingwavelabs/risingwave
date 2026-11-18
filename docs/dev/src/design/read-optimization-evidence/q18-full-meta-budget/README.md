# Full SF10 Q18 metadata budget A/B

Full CREATE MV decreased from 535.9 to 461.9 seconds with the 9 MiB
budget. The target HashJoin phase was 43.0 seconds shorter. Its
per-iterator candidate count also differed across the two live
compaction runs. Peak CN RSS was 5.612 versus 5.602 GiB.

## Controls

Each budget ran once to successful foreground CREATE MV completion
with the same opt3 release as the
[fixed-snapshot comparison](../q18-meta-working-set/README.md).
The binary SHA-256 and source patch identity are in
[build-identity.json](build-identity.json). Metadata sampling,
fastrace and Foyer disk caches were off. Each arm cloned the same
stopped PostgreSQL and MinIO fixture at Hummock v1346, checked the
source SST fingerprint and ran `risedev d full` with the shared
exclusive local guard. Normal automatic compaction was enabled.
The only TOML difference is `storage.read_batch.meta_max_bytes`.

Both runs verified six LocalityProviders, the same plan and vnode
placement, 136,472,104 source snapshot reads, and the independently
calculated 100-row result. The CN had 12 slots, 8 streaming runtime
workers, 6 dedicated I/O workers and fixed Q18 parallelism 4.
ColdRanges, N16, G128/P32/B16, the 4 GiB prefetch budget, 64 KiB
scheduler gaps and 1 MiB physical read cap were held constant.

## Timings and counters

| Metric | 1 MiB | 9 MiB |
| --- | ---: | ---: |
| CREATE MV wall, s | 535.86 | 461.87 |
| Source snapshot complete, s since DDL start | 78.32 | 83.31 |
| Time through the start of provider 24, s | 171.09 | 179.10 |
| Target probe phase, provider 24 to 22, s | 138.00 | 95.00 |
| Provider 22 to 20, s | 145.00 | 118.00 |
| Provider 20 to completed MV, s | 81.77 | 69.78 |
| Fragment 23 left match, actor-seconds | 537.77 | 368.13 |
| Table 38 iterator initialization, actor-seconds | 333.01 | 179.15 |
| Table 38 Bloom checks per iterator | 6.000 | 5.012 |
| Table 38 Foyer metadata lookups | 44,219,712 | 294,065 |
| Table 38 metadata lookup misses | 59 | 19 |
| Table 38 per-batch meta reuse fraction | 50.87% | 99.61% |
| Table 38 budget rejections | 44,040,007 | 0 |
| CN object GETs | 120,198 | 124,053 |
| CN object reads, GiB | 36.270 | 36.385 |
| Full CN peak RSS, GiB | 5.612 | 5.602 |
| CN peak RSS in target probe phase, GiB | 5.311 | 5.305 |

Both arms completed 15,000,000 table-38 probes and the same number of
join-cache misses. The 9 MiB arm did 16.5% fewer Bloom checks per
iterator. Its Foyer lookup fraction fell much further, from 49.13%
to 0.39%. Iterator initialization time per checked candidate fell
from 3.70 to 2.38 microseconds. These are aggregate times, including
other iterator work.

The 74.0-second end-to-end improvement decomposes into 8.0 more
seconds before provider 24, 43.0 fewer seconds in the target probe
phase and 39.0 fewer seconds after provider 22. The post-22 interval
also recorded fewer meta lookups. Table 32 fell from 65.0 to 1.81
million before provider 20, and HashAgg state table 27 fell from
46.2 to 4.80 million after provider 20. The budget is shared by
these read paths. The A/B includes the effect of live compaction
and the measured changes cannot be assigned exclusively to the
target HashJoin.

The full 1 Hz process RSS peaks came during source scanning, at
51.2 and 72.3 seconds after DDL start. The 9 MiB arm's peak was
10.6 MiB lower overall and 6.1 MiB lower in the target probe phase.

## Intermediate layout

The source SST projection hash matches the seed in both arms. The
table-38/39 catalog filter returned the following table-38 layout
during the target probe phase. Membership, levels, ranges and sizes
were stable across the start, periodic probes and end of the phase
within each run. The layout hashes differ across arms even after
excluding generated SST and object IDs.

| Layout | 1 MiB | 9 MiB |
| --- | ---: | ---: |
| SST entries | 97 | 99 |
| L0 entries | 19 | 21 |
| L5 entries | 16 | 18 |
| L6 entries | 62 | 60 |
| Encoded SST bytes | 2,911,002,192 | 2,991,363,068 |

[Comparison JSON](comparison.json) includes boundary timestamps,
exact layout fingerprints, per-stage Foyer counters and memory
gauges. The per-arm directories contain the effective TOML,
intermediate SST rows, selected CN and Meta scrapes and the
one-second RSS series. Both completed arms passed the oracle and
cleaned up their RiseDev processes, containers and cold clones.

The [downstream attribution](downstream-attribution.md) compares
operator timers, effective SST candidates, and metadata reuse for
the 27 seconds in the semi-join interval and the 12 seconds in the
final aggregation interval.

The [fixed-8 comparison](fixed8-matrix.md) reruns main and the optimized
9 MiB configuration for three matched pairs with 12 advertised slots.
