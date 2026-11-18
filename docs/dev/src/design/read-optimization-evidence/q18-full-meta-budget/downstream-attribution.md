# Where the 39 downstream seconds went

This analysis uses the existing full SF10 Q18 pair. The boundary is
the first start event from a LocalityProvider fragment. The two
downstream intervals account for 39.0 seconds less wall time.

| Interval | Principal read path | 1 MiB | 9 MiB | Saved |
| --- | --- | ---: | ---: | ---: |
| Provider 22 to provider 20 | Fragment 21 left semi HashJoin, table 32 prefix iterators | 145.0 s | 118.0 s | 27.0 s |
| Provider 20 to completed MV | Fragment 20 final HashAgg, table 27 native point batches, then TopN and Materialize | 81.8 s | 69.8 s | 12.0 s |

## Left semi HashJoin

Both intervals reported 59,986,052 lookups against table 32 and about
15 million join-cache misses. The table-32 data misses were 5,788 and
5,841. Whole-CN object GETs in the interval were 13,735 and 13,463.

| Table 32 measure in 22–20 | 1 MiB | 9 MiB |
| --- | ---: | ---: |
| Fragment 21 left match time, actor-s | 543.09 | 397.23 |
| Iterator initialization, actor-s | 338.34 | 189.27 |
| Bloom checks, millions | 81.21 | 46.03 |
| Bloom checks per iterator | 5.47 | 3.10 |
| Iterator initialization per Bloom check, µs | 4.17 | 4.11 |
| Foyer meta lookups, millions | 64.98 | 1.81 |
| Share of metadata visits that query Foyer | 80.02% | 3.93% |
| Retention budget rejects, millions | 64.72 | 1.11 |

The match timer follows the operator's left input and excludes time
after yielding a result. Iterator initialization accounted for nearly
all of the observed decrease in its cumulative timer. Both effects are
visible. The effective SST candidate work dropped 43.3% and the
candidate-weighted initialization time was nearly flat. Handle reuse
also rose substantially and bypassed repeated hot Foyer queries.
Holding the 1 MiB Foyer share fixed, fewer candidates account for
28.14 million of the 63.17 million avoided lookups. The share
change accounts for the remaining 35.03 million. That is a lookup
count decomposition with the 1 MiB arm as the reference. The
time attributable to each mechanism is unresolved.

## Final HashAgg

The final HashAgg restored 14,957,581 groups in each run. Both
runs executed about 234,300 point batches against its intermediate
state table 27. The table counts below use the full read counters to
avoid dropping the first batches at the provider-20 scrape cut.
The final interval contains over 99.9% of the cumulative batch
latency in both arms.

| Table 27 measure | 1 MiB | 9 MiB |
| --- | ---: | ---: |
| Keys submitted in point batches | 14,957,575 | 14,957,574 |
| Bloom checks, millions | 82.83 | 110.20 |
| Bloom checks per batched key | 5.54 | 7.37 |
| Point batch mean, ms | 0.591 | 0.388 |
| Cumulative point batch duration, actor-s | 138.41 | 90.91 |
| Foyer meta lookups, millions | 46.16 | 4.80 |
| Share of metadata visits that query Foyer | 55.73% | 4.36% |
| Retention budget rejects, millions | 45.58 | 3.13 |
| Actual meta misses | 0 | 0 |
| Actual data misses | 5 | 7 |

This stage has the clearest evidence for a retention benefit. The
point-batch work stayed the same and the effective candidate work
increased by 33.0%. The fall in budget rejects and hot Foyer queries
coincided with a 34.3% decrease in mean batch latency. Completed MV
time also includes locality scanning and TopN work. The 12-second
interval delta measures the whole pipeline.

The archived full-run SST catalog projection covers table 38 and
its companion table. For tables 32 and 27, the comparison observes
the effective candidate work through Bloom and metadata counters.
It also observes different compaction outputs for the two runs.
An object-by-object read-version comparison for the downstream
tables would require additional snapshots. Actor-s sums can overlap
across the four actors. The one-second scrape brackets and the
full-scrape end counters are in
[downstream-attribution.json](downstream-attribution.json), with
the relevant raw samples in
[downstream-scrapes.jsonl.gz](downstream-scrapes.jsonl.gz).
