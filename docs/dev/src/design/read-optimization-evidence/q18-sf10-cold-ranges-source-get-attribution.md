### SF10 Q18 source GET increase after ColdRanges fixes

This analysis uses retained trace-off data. It executes no workload.
The endpoints are pre-fix `6c1ab58217` with six IO workers and fixed
`0bb0df4acf` with six IO workers and a 16 GiB prefetch budget. Both
use the same cold v1346 source SST fingerprint, SQL, plan and source row
counts. G128/P32/B16, N16/1 MiB, zero scheduler gap, MultiGet and metadata
reuse match. Foyer disk cache is off. The fixed run has zero budget
rejections.

The immediate source of additional submissions is splitting windows at
resident cache blocks. The fixed source interval reports 8,485 internal
cache gaps, and its cold segment count increases by 8,586. Aggregate
submission/merge accounting reconciles all 8,594 additional physical GETs.

| Source-phase counter | Pre-fix 6 workers | Fixed 6 workers, 16 GiB |
| --- | ---: | ---: |
| Physical GETs | 35,938 | 44,532 |
| Physical bytes, GiB | 12.548 | 12.291 |
| Prefetch windows inspected | 38,673 | 38,229 |
| Nonempty windows | 38,122 | 38,223 |
| Windows with two cold segments | 0 | 8,485 |
| Declared cold segments | 38,122 | 46,708 |
| Direct cold segments | 38,122 | 45,680 |
| Singleton Fill registrations | Separate counter unavailable | 1,028 |
| Internal cache gaps | 0 | 8,485 |
| Cached blocks inside those gaps | 0 | 11,589 |
| Gap bytes | 0 | 196,170,757 |
| All logical ObjectStore reads | 39,223 | 47,731 |
| Logical members saved by scheduler merging | 3,233 | 3,091 |
| Logical requests saved by in-flight reuse | 52 | 108 |

The physical-read identity holds at each endpoint:
physical GETs = logical ObjectStore reads - merged members - in-flight
followers. There are zero observed read errors, retries and cancelled
outcomes in the measured interval.

Each window also obeys declared cold segments = nonempty windows +
internal gaps. The exact difference account is:

| Term | Change in GET-count accounting |
| --- | ---: |
| Additional internal-gap splits | +8,485 |
| Additional nonempty windows | +101 |
| Remaining logical-request difference after removing declared cold segments | -78 |
| Fewer logical members merged | +142 |
| More in-flight sharing | -56 |
| Total | +8,594 |

The -78 residual is derived from all logical reads minus declared
segments. It includes ordinary/unbounded requests and cache-sharing,
polling and phase-boundary effects, and is not a separately instrumented
request category. The decomposition is aggregate counter bookkeeping;
it does not tag a unique physical GET to each gap.

#### Code path and cache publication

In the fixed implementation, `prefetch_cold_ranges` uses an actual
memory-cache `get` and retains the entry. Every hit closes the preceding
cold segment; the trailing segment starts after the hit. With disk cache
off, these measured internal gaps correspond to resident memory entries,
rather than false-positive disk-index hints. The source interval also
has zero stale-hint fallbacks.

For example, a window A B C D E F with only C cached submits AB and DEF.
Both spans may be smaller than the byte/window cap. At
`max_gap_bytes = 0`, the scheduler cannot combine them across C.
This increases request count while skipping C's bytes.

The relevant implementation is
[cache lookup and splitting](https://github.com/risingwavelabs/risingwave/blob/0bb0df4acf388a958e9592787ec36ceebcb376ed/src/storage/src/hummock/sstable_store/cold_range.rs#L255).
The internal-gap and window-run counters are computed from the resulting
`Read` and `Single` parts. The old version counted direct singleton
parts as `Read`, so the new singleton representation does not by itself
create the gap-count increase.

The pre-fix read path decoded and inserted an entire completed cold run
before returning its first block. The fixed `decode_next` decodes and
inserts only the consumed block. Later blocks remain in a private raw
buffer until consumption. Consequently, another iterator observes
cache publication at block-consumption times. This changes both the
extent and timing of visible resident runs. Q18's plan includes two
StreamTableScan operators for lineitem, giving concurrent readers a
reachable way to observe those changing cache contents.

This code change supplies a mechanism for fragmented cache visibility.
The retained metrics establish that fragmented windows occurred.
Trace-off counters do not link a specific gap to the iterator and cache
insertion that created it, so per-gap publication causality remains
unmeasured. The relevant implementations are the
[pre-fix whole-run decode](https://github.com/risingwavelabs/risingwave/blob/6c1ab582177236100bb178b939e64191051acc5f/src/storage/src/hummock/sstable_store/cold_range.rs#L90)
and [fixed block-at-a-time decode](https://github.com/risingwavelabs/risingwave/blob/0bb0df4acf388a958e9592787ec36ceebcb376ed/src/storage/src/hummock/sstable_store/cold_range.rs#L150).

#### Duplicate-read coverage and gap sizes

The fixed source interval has 323 Fill ordinary-block external callbacks
whose bytes were already returned in a live, unconsumed ColdRanges
buffer, totaling 5,301,413 bytes. This observation is upstream of
scheduler reuse and is not an exact duplicate-physical-GET count.

The scheduler separately records 228 partial-overlap physical
dispatches with 16,288,342 intersection bytes while older GETs are
active. These scopes can overlap. The counters must be interpreted
separately. A new direct multi-block range against a completed private
buffer is outside the ordinary-block callback observation. Therefore
the available counters do not form a complete duplicate-read census.

The gap histogram reports 8,458 of 8,485 gaps at most 64 KiB, with a
mean gap of 22.58 KiB and 187.08 MiB of total skipped gap bytes.
All members come from a window already capped at 1 MiB. Bridging every
eligible within-window gap would remove up to 8,458 declared segment
boundaries, before admission/timing and reuse effects, with at most
187.08 MiB of added gap bytes. This is opportunity accounting. The
current zero-gap scheduler configuration forbids such bridging, and
the retained data does not establish whether both sides were queued
together. No gap policy was changed.

#### Phase-boundary cross-check

The old source checkpoint is 86.03 s, while its first LocalityProvider
starts at 85.14 s. The new checkpoint is 81.04 s and its first provider
starts at 81.10 s. The comparison therefore includes a small old-side
overlap with downstream startup.

The increase is already present before either run's first provider.
At the closest approximately 60.7 s full scrapes, the old run reports
27,035 GETs and zero gaps; the fixed run reports 35,674 GETs and 7,340
gaps. By approximately 76 s, those are 33,317 GETs / zero gaps and
43,396 GETs / 8,356 gaps. Progress differs at equal wall times; these
samples locate the effect during source scanning rather than at the
final checkpoint boundary.

The offline `source-get-attribution.json` contains the complete
arithmetic, histograms, overlap observations, source identity and
pre-provider samples. Code and runtime configuration were read from
their measured versions. No application changes or new Q18 execution
were performed.
