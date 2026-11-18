# SF10 Q18 metadata pins at 1 and 9 MiB

This replay uses the stopped PostgreSQL/MinIO snapshot from the
[previous Q18 working-set experiment](../q18-meta-working-set/README.md).
It includes the first 2.5 million probes of the four-actor fragment
23 HashJoin after recovery. Both runs use the same instrumented
opt3 release, cache and scheduler configuration, `risedev d full`,
and the exclusive local guard. The only TOML difference is
`storage.read_batch.meta_max_bytes`. Compaction is frozen equally
in both recovered runs. The table 38/39 SST projection and vnode
assignment match the original stopped checkpoint at start and
through the measured phase. The measurement ends before the known
downstream L0 write-stop horizon.

The pins count unique indexed records on the Foyer metadata cache's
LRU pin list. Multiple handle clones leave one entry in that list.
The sum is taken under the shard read locks. Samples are roughly
200 ms apart in the timed window. A per-shard counter on insert
detects over-capacity insertions even between samples. It also
tracks the largest post-insert excess over capacity. The
[deterministic smoke check](foyer-counter-smoke.rs) verified clone
deduplication, last-handle release, and usage of 12 bytes after an
insertion into a 10-byte cache with a pinned entry.

| Measurement | 1 MiB | 9 MiB |
| --- | ---: | ---: |
| Samples in the probe window | 382 | 113 |
| Peak sampled unique LRU pins | 33 | 47 |
| Peak sampled pinned weight, MiB | 13.32 | 24.99 |
| p95 sampled pinned weight, MiB | 11.21 | 23.32 |
| Peak meta cache usage, MiB | 46.06 | 50.71 |
| Meta cache capacity, MiB | 229 | 229 |
| Post-insert shard over-capacity events | 0 | 0 |
| Peak CN RSS, GiB | 1.807 | 2.239 |
| Iterator initialization mean, µs | 97.76 | 20.99 |
| Iterator initialization, actor-s | 246.00 | 53.28 |
| Probe window wall, s | 76.43 | 22.16 |
| Table 38 Foyer meta queries | 17,655,560 | 110,855 |
| Table 38 outer metadata misses | 60 | 59 |
| CN object GET mean, ms | 1.02 | 1.15 |

The meta cache had one LRU shard and stayed within 22.2% of configured
capacity. The independent peaks for pin count and weight need not
occur at the same sample. At the peak-weight sample the counts were
29 and 43. The increased pinned weight consumes eviction headroom
inside the cache. Total indexed weight rose only 4.65 MiB at peak.
Zero ordinary evictions, removals or replacements were recorded.

Peak process RSS rose by 442.6 MiB in the faster arm. At the final
scrape, immutable uploader bytes were 18.5 versus 73.2 MiB, and
jemalloc resident-minus-active memory was about 344 MiB larger.
These end gauges help explain the gap in scale between the additional
pinned meta weight and RSS. The RSS peaks were sampled independently
and the allocation sites were not profiled.

SST IDs and ranges were fixed across the pair. The reported probe
threshold uses the surrounding counter samples. The differential is
for this older snapshot with four probe actors, one metadata shard,
and pin sampling enabled on both arms. It leaves production
high-concurrency limits open.

The [release identity](service-artifact.json) and compressed
RisingWave/Foyer diagnostic diffs identify the exact instrumented
build. The [comparison JSON](comparison.json) and arm directories
contain the run results, time series, configuration and final heap
gauges. Both owned RiseDev runs stopped and their scratch data was
removed. The shared sealed checkpoint was retained.
