# Streaming read optimization controls and diagnostics

Blockwise prefetch, read coalescing, and point MultiGet are opt-in. These startup controls require a worker restart;
changing a SQL session parameter does not change them. Use the same TOML on every CN.
No object format, visibility or compaction policy changes are required.
See the [read scheduling design](./hummock-read-scheduling.md) for the current
read path, resource ownership, and Q18 stage assessment.
The [stage results archive](./read-optimization-evidence/stage-results.json)
retains the machine-readable measurements behind the changelog, Locality
Backfill, and fixed-SST replay experiments below.

## Configuration

```toml
[storage.blockwise_prefetch]
enabled = true
changelog_enabled = false
locality_backfill_enabled = false
max_blocks = 3
max_bytes = 262144

[storage.object_store]
enable_hummock_read_coalescing = true

[storage.object_store.read_scheduler]
batch_prepare_timeout_ms = 1
max_batch_requests = 16
max_admitted_requests = 32
max_inflight_reads = 32
max_read_bytes = 262144

[storage.read_batch]
enabled = false
meta_reuse = false

[batch]
point_get_concurrency = 1
```

Numeric limits must be positive. `batch_prepare_timeout_ms` accepts 0 to
disable window preparation. The `blockwise_prefetch` and `read_scheduler`
sections reject unknown fields. Keep `point_get_concurrency` at 1 for
streaming-only experiments. A value of 1 for `max_blocks` disables speculative
neighbors, while still permitting demand reads.

| Control | Owner and effect | Evidence for tuning |
| --- | --- | --- |
| `blockwise_prefetch.enabled` | Bounded blockwise lookahead for HashJoin row/degree reads and fully consumed forward scans in Temporal Join, AsOf Join, and OverWindow. | Window distribution and consumed/discarded handles. |
| `changelog_enabled` | Independent opt-in for Hummock change log scans; the vnode/key upper bound is passed to SST iterators before lookahead. | Long cold sequential scans are candidates; verify old/new value rows and epoch/range boundaries. |
| `locality_backfill_enabled` | Switches LocalityProvider snapshot scans from contiguous range reads to blockwise prefetch. | Compare backfill duration, physical GETs and bytes with the same scheduler and backfill rate. |
| `max_blocks` | Per-iterator window including the demand block. Later windows are issued as consumption advances. | First-poll `consume_pending` together with material wait duration and consumed windows can motivate an N comparison. Single-block ranges cannot benefit. |
| `max_bytes` | Window sum of max(compressed, decoded) block bytes. An oversized demand block reads alone. This is not a CN-wide memory cap. | `limit="bytes"`, window bytes, RSS and discarded handles. |
| `batch_prepare_timeout_ms` | Bounds preparation after the first cold member of an explicit prefetch window. `0` disables window preparation. | Preparation timeouts, GET count and logical read latency. |
| `max_batch_requests` | Logical receive budget per dequeue pass, capped by Q. A packet that does not fit the remaining budget is deferred intact. | Pending requests from multiple passes can enter one GET. |
| `max_admitted_requests` | Ordinary retained logical requests, queued plus in flight, per scheduler. A complete mergeable window can keep its existing preparation permits through completion when ordinary admission is short. | Pending/waiting gauges, admission latency, and `window_admitted_with_preparation`. |
| `max_inflight_reads` | Concurrent scheduled physical reads, including retry loops and range-error fallback children. One merged GET uses one slot. | Scheduled physical in-flight gauge and enqueue-to-dispatch latency. Oversized/unbounded direct reads are excluded. |
| `max_read_bytes` | Maximum eligible input and merged contiguous range. Same object, adjacent/overlapping ranges only. | `route="oversized"`, `split_bytes`, actual operation bytes and latency. |
| `read_batch.enabled` | Opt-in point MultiGet for missing state in HashAgg and Materialize. | `state_store_get_batch_keys`, `state_store_get_batch_duration`, and `state_store_get_batch_outcomes`. |
| `read_batch.meta_reuse` | Reuse Foyer SST metadata handles within one MultiGet. | Compare `meta_total` and `meta_batch_ready` for the same table and workload. |

Ordinary eligible reads wait for an admission slot before allocating an owned scheduler request.
Explicit windows reserve preparation capacity before staging cold reads. The scheduler
keeps unsubmitted requests available for merging until physical capacity is available,
then selects the group containing the oldest request. An isolated request dispatches
without waiting for future neighbors. Logical admission still bounds visibility; it
does not guarantee that every prefetch window is admitted together. Oversized and unbounded reads still go directly to the backend.
The limit bounds scheduler-retained requests, not upstream Foyer producer tasks. Dropping
a prefetch handle does not necessarily cancel its independently running Foyer producer.
Dispatcher failure returns an error and wakes admission waiters.
Physical reads run in independent tasks. The fixed-SST changelog comparison below
selected this mode with a 1 ms preparation deadline, so it is now the default
when coalescing is enabled. Remove `independent_io_execution` from older test TOMLs.

For an in-image control, disable prefix, changelog and Locality Backfill
Blockwise prefetch, read coalescing, and point MultiGet. For a main comparison,
use a separate main image built with the same architecture, features and profile.

To isolate metadata reuse, set `storage.read_batch.enabled = true` in both
arms and toggle only `meta_reuse`. `concurrency` limits concurrent keys per
call. `meta_max_entries` and `meta_max_bytes` bound regular retained handles,
with one extra slot for an oversized SST. The call owns these handles until
it returns. Foyer handles metadata loads and single-flight. Historical
MultiGet results below identify the builds and settings they measured.

## Metric meanings

Metrics below are available without TRACE. Storage per-table counters use the usual
Info metric level. Aggregate all relevant CN instances; retain instance labels when
investigating skew. Take deltas over a common interval and drain in-flight operations
before reconciling counts. Counts from different layers are not interchangeable.
The defaults retain logical admission=32 and physical concurrency=32 for compatibility,
not as tuned values. Keeping logical admission too small still restricts merging even
with a separate physical limit; increase that capacity independently in experiments.

| Metric | Meaning / exclusions |
| --- | --- |
| `object_store_read_requests{media_type,route}` | Logical reads reaching the object wrapper. Routes: direct (disabled), scheduled (including admission wait), oversized, unbounded, empty_range. Foyer hits/followers need not reach here. |
| `object_store_read_request_bytes{media_type,route}` | Bounded logical range sizes before coalescing. Overlaps count repeatedly. Unbounded reads have no size sample. |
| `object_store_read_request_duration_seconds{media_type,route}` | Logical latency including scheduler wait and retry, also recorded for cancelled futures. |
| `object_store_read_outcomes{media_type,stage,outcome}` | Completion counts: logical, physical (one direct read plus its retry loop), attempt (each backend read invocation); ok/error/cancelled. SDK-internal HTTP retries are not separate attempts here. |
| `object_store_read_in_flight{media_type,stage}` | Live reads at each of those layers, including waiting futures; RAII decrements on cancellation. |
| `object_store_operation_bytes{type="read"}` | Successful physical read result sizes. `_count` is successful physical operations; `_sum` is successful returned bytes. Neither counts failed/partial network transfers. |
| `object_store_operation_latency{type="read"}` | Physical read duration including its retry loop. Compare with logical latency and queue latency. |
| `object_store_request_retry_count{type="read"}`, `object_store_failure_count{type="read"}` | Wrapper retries and terminal failures. Use with attempt outcomes; do not count group-size savings as net savings during failures. |
| `object_store_read_scheduler_batch_size` | Requests received together at dequeue, including ones cancelled before dispatch. Requests from different dequeues can merge while waiting for physical capacity; this is not a merge-window histogram. |
| `object_store_read_scheduler_group_size` | Logical members per dispatched group. Sum minus count is *planned* consolidation, not exact net GET savings under cancellation/fallback. |
| `object_store_read_scheduler_queue_duration_seconds` | Logical admission to initial physical dispatch, including time waiting for physical capacity. Excludes retry/fallback after that initial dispatch. |
| `object_store_read_scheduler_waiting` | Caller futures waiting for admission; excludes admitted requests. Returns to zero on admission, cancellation or worker failure. |
| `object_store_read_scheduler_admission_duration_seconds` | Admission acquisition duration, including immediately admitted requests and cancelled/failed waits. Logical read latency includes this interval. |
| `object_store_read_scheduler_preparing` | Preparation reservations before publication. A prefix that moves to ordinary admission releases its reservations while the tail waits. |
| `object_store_read_scheduler_prepare_duration_seconds{reason}`, `object_store_read_scheduler_prepared_batch_size` | Time from the first staged cold member until window release, and the number of surviving cold reads at release. All-hit windows may resolve without starting a preparation driver. |
| `object_store_read_scheduler_queued` | Admitted logical requests awaiting their initial physical dispatch; includes channel and worker pending requests. |
| `object_store_read_scheduler_in_flight` | Scheduled physical reads, including retry loops and fallback children; excludes direct reads. Bounded by `max_inflight_reads`. |
| `object_store_read_scheduler_pending` | Admitted queued/in-flight logical requests. Should return to zero when quiescent. |
| `object_store_read_scheduler_events{event}` | split_object/split_gap/split_bytes right boundaries of selected dispatch groups; cancelled_queued (while pending before dispatch), cancelled_before_dispatch (after planning); group_ok/error/cancelled; range_fallback_requests; merged_requests (members minus one for successful groups). Boundaries describe the pending set at group selection, not all potential future neighbors; they are not directly comparable to the earlier per-dequeue planner counts. |
| Window events in `object_store_read_scheduler_events` | `window_complete` and `window_timeout` count window closures, `window_late` counts members, and `window_budget_reject` counts failed preparation reservations. `window_split_admission` and `window_split_limit` count prefix-publication boundaries. `window_admitted_with_preparation` counts packets that transferred preparation permits. These counts describe distinct stages. |
| `object_store_read_scheduler_gap_bytes` | Positive same-object gaps limiting the selected dispatch group. Cancellation after planning can leave an interior hole in a retained group. |
| `state_store_sst_store_block_request_counts{table_id,type}` | Cache query and miss counts. For point MultiGet, `meta_total` counts metadata lookups through Foyer, `meta_batch_ready` counts ready handles reused within the call, and `meta_batch_budget_reject` counts metadata returned by Foyer without retaining a handle due to the per-call budget. |
| `state_store_blockwise_prefetch_window_blocks{limit}` | Window size including demand, clipped by range/blocks/bytes. A window of one produces no lookahead. |
| `state_store_blockwise_prefetch_window_bytes` | Selected window budget weight, including demand. Not transferred bytes. |
| `state_store_blockwise_prefetch_events{event}` | policy_skipped; registered_ready/pending; registration_error; consumed (handle taken); consume_ready/pending (first poll of the retained response at consumption); consume_ok/error; discarded. Registration reports the original cache lookup; consumption reports whether waiting actually yields. Consumed is an attempted use; cancellation can occur before success. |
| `state_store_blockwise_prefetch_bytes{event}` | Compressed lengths of registered, consumed or discarded neighbor handles; not physical network bytes. |
| `state_store_blockwise_prefetch_wait_duration_seconds` | Wait when consuming a registered neighbor, including cancellation. High pending fraction/wait can mean the lookahead is too short or the backend is slow. |
| `state_store_block_cache_read_events{event}` | Fill requests classified memory_hit/pending at registration; outer_fetch counts actual producer invocations (including other cache policies). Pending also includes disk-cache reads and shared fetch followers. It is not an S3-miss counter. |

Before dispatch, cancelled members are removed and the outer span is trimmed. If an interior
member cancels, the existing group is retained rather than split into additional GETs; its
bytes may still be read. If every member cancels, no read is issued. Pending cancellation wakes the worker even
when all physical slots are occupied. Range-error fallback releases the parent physical
slot before children acquire slots from the same budget; a slow child does not delay
other completed children when capacity allows. Cancellation after
dispatch cannot undo bytes already transferred.

Discarding a prefetch handle does not establish wasted IO: Foyer can retain the result
or share its producer with another iterator. The metrics do not subtract pending minus
outer_fetch to claim an exact follower count. For that attribution use Foyer's own
cache metrics plus a short capture with `RW_STORAGE_IO_FASTRACE=true` on the reader
compute node. Set `RW_TRACING_ENDPOINT` to the OTLP gRPC collector if RiseDev or the
deployment does not supply it. The flag routes `state_store_read_batch` and
`object_store_read_scheduler` to the existing Fastrace exporter. Match logical
`request_id` to `request_ids` on scheduler dequeue and dispatch spans to follow a
block read through the scheduler. Both targets use DEBUG so release binaries
retain them, and require explicit enablement. The exporter sends up to 512 spans
per request by default. Set `OTEL_BSP_MAX_EXPORT_BATCH_SIZE` to lower the span
count for a collector with a smaller message limit. The embedded Meta collector
keeps 256 traces by default. Increase `meta.developer.cached_traces_num` for a
local capture that needs to retain several stages of a busy workload. Leave the
flag off for timed comparisons and use Prometheus counters and histograms for
aggregate read behavior.

The isolated logger and scheduler check exported both logical request IDs, their
dequeue events, and the merged physical read to an embedded OTLP service. The
guarded two-CN changelog smoke passed exact row and revision checks. Its Meta
dashboard trace query returned no records, so full-cluster trace correlation
is still unverified. Local evidence is in `target/io-fastrace-migration/`.

## Useful PromQL

Apply the same job/cluster selector to every expression in an actual dashboard.
These examples aggregate across CNs; keep `instance` in each `by` for skew diagnosis.

```promql
# Physical backend invocations/sec (includes retries visible to the wrapper).
sum(rate(object_store_read_outcomes{stage="attempt"}[5m]))
# Successful physical mean read size; failed reads are excluded.
sum(rate(object_store_operation_bytes_sum{type="read"}[5m]))
/ sum(rate(object_store_operation_bytes_count{type="read"}[5m]))
# Queue p99 by backend.
histogram_quantile(0.99, sum by (le, media_type)
  (rate(object_store_read_scheduler_queue_duration_seconds_bucket[5m])))
# Logical p99 by route; compare with physical operation latency separately.
histogram_quantile(0.99, sum by (le, route)
  (rate(object_store_read_request_duration_seconds_bucket[5m])))
# Admission p99 by backend; compare with the admitted queue p99 above.
histogram_quantile(0.99, sum by (le, media_type)
  (rate(object_store_read_scheduler_admission_duration_seconds_bucket[5m])))
# Callers waiting for admission.
sum by (media_type) (object_store_read_scheduler_waiting)
# Metadata cache queries and misses by table.
sum by (table_id, type) (rate(state_store_sst_store_block_request_counts{
  type=~"meta_total|meta_miss"}[5m]))
# Which bound limits blockwise windows?
sum by (limit) (rate(state_store_blockwise_prefetch_window_blocks_count[5m]))
```

No samples is different from zero latency or perfect cache hit rate. Use absolute
counts as well as ratios, and establish that the operator/table actually exercised
the path. Compare rows/results and checkpoint progress before comparing performance.

## Cloud test preflight

1. Finish source validation and independent review; record exact commit and image digest.
2. Confirm ARM64 before fast-build deployment. Build both main/candidate with identical
   profile and architecture. A successful GitHub dispatch is not completed image publication.
3. Verify TOML delivery to every CN and metric scraping. Use restart-applied effective
   configuration, not only the submitted dashboard form. Preserve state/cache budgets,
   topology, input rows, SST/version identity where replay supports it, and measurement duration.
4. Run correctness smoke with native batching, prefix lookahead and scheduler individually
   enabled, then together. Check error/cancel counters and pending gauges settle after drain.
5. Only then run longer workload comparisons; collect CPU/RSS, barrier latency, throughput,
   physical operation counts/bytes, p95/p99 and compaction state alongside these read metrics.

This document is a test protocol, not evidence that a Cloud run has passed.


## Local cross-database changelog calibration (2026-11-10)

This calibration predates asynchronous admission: the tested scheduler bypassed
eligible requests when its admission limit was full. It is not a measurement of the
current waiting policy.

A guarded `risedev d full` run used separate writer/reader CN resource groups,
with the reader set to `role: streaming` so producer `INSERT SELECT` batch reads
could not contaminate reader IO. Both CNs retained default memory/parallelism,
with real Foyer disk caches enabled. The fixed workload had 65,536 primary keys,
a varied deterministic 2 KiB body, and eight updates per key. Measurement began
after initial snapshot completion. Every arm consumed 524,288 UPDATE changelog
rows and passed full-row equality after the measurement window.

| Candidate configuration | Reader GETs | Reader MiB | Merged requests | Aggregate progress lag p95 (s) |
| --- | ---: | ---: | ---: | ---: |
| OFF | 37,552 | 2,272.5 | 0 | 3.364 |
| N=3, scheduler admission=32 | 36,705 | 2,284.4 | 1,041 | 3.530 |
| N=16, scheduler admission=32 | 36,505 | 2,266.9 | 965 | 3.567 |
| N=3, scheduler admission=256 | 28,963 | 2,275.0 | 8,634 | 4.215 |

All N=16 windows were range-limited to three or four blocks. All prefetched
blocks were consumed. At N=3, admission32 bypassed84.2% of eligible requests;
admission256 reduced bypass to37.6%. Permits cover queued and in-flight reads.
The higher admission limit saved22.9% of GETs versus OFF without material byte
amplification, but did not establish an overall speedup: aggregate progress lag
and sampled RSS increased, and mean logical read latency rose from21.4ms OFF
to48.9ms with admission256. Queue time alone does not explain that difference.

This was one finite round per configuration on a dev candidate binary and
local MinIO, not main, release, or real S3. Input was fixed; SST layout was not.
Aggregate progress lag uses sampled revision sums and is not per-row latency.
OFF/ON also changes range-end clipping. Reader metrics include downstream
materialization reads. Evidence is in `target/cross-db-changelog/REPORT.txt`
and its adjacent SQL, configurations, metrics, and timing files; binary SHA256
`d045ff08037b893b4d904ef9754433d7adff3ac2700110602e9da87258fb0660`.
The serving-mixed calibration was excluded; all owned services/data were cleaned.

## Asynchronous admission, latest-only follow-up (2026-11-10)

This result predates separate logical and physical budgets and dispatch-time planning.
Only the new N=3/admission=32 candidate was run. The OFF and bypass32 values
below reuse the preceding calibration; input/configuration match, SST layouts
are independent. Same guarded `risedev d full`, dev build, MinIO and real Foyer.

| Metric | Saved OFF | Saved bypass32 | New wait32 |
| --- | ---: | ---: | ---: |
| GETs | 37,552 | 36,705 | 28,339 |
| Read MiB | 2,272.5 | 2,284.4 | 2,284.7 |
| Merged requests | 0 | 1,041 | 9,421 |
| Aggregate progress lag p95 (s) | 3.364 | 3.530 | 3.497 |
| Mean logical read (ms) | 21.40 | 53.66 | 60.62 |
| Reader CPU (s) | 83 | 80 | 76 |
| Sampled RSS peak (MiB) | 1,717.0 | 1,972.9 | 2,390.6 |

Admission bypass disappeared. Mean admission acquisition was 56.11 ms and
admitted queue time 1.24 ms; physical read mean fell from 54.00 to 2.89 ms.
Sampled physical inflight fell from 623 to 25, while total logical inflight
remained 631 versus 633 (603 waiting for admission at peak). This demonstrates
backpressure and coalescing, not less total logical wait or a faster workload.

RSS increased despite almost identical Foyer cache occupancy and similar initial
RSS. End allocator resident increased about 415 MiB, versus 28 MiB active and
44 MiB allocated: most difference is outside active pages. Allocator retention
is a lead, not a proven allocation-site diagnosis. This remains an open cost.

All 524,288 UPDATE rows and full-row equality passed; no discarded lookahead,
backend errors or stuck admission/pending gauges were observed. Runtime data and
owned services were cleaned. Raw evidence and qualification are in
`target/cross-db-changelog-admission/REPORT.txt`, with `memory-audit.json` and
per-arm metrics. Binary SHA256
`435c763866130c92de83d02aea4afcc5650a83b84d11e9995f59236e264bc084`.
This is a short historical comparison, not main, real S3, or rollout signoff.


## Dispatch-time planning with Q1024/P32 (2026-11-10)

Only the latest candidate was run, with logical admission Q1024, physical P32
and lookahead N3. The preceding wait32 and OFF results were reused. Same guarded
`risedev d full`, two default-size CNs, streaming-only reader, MinIO, real Foyer,
65,536 keys and 524,288 updates; fixed input but independent SST layouts.

| Metric | Saved OFF | Saved wait32 | Latest Q1024/P32 |
| --- | ---: | ---: | ---: |
| GETs | 37,552 | 28,339 | 13,834 |
| Read MiB | 2,272.5 | 2,284.7 | 2,284.7 |
| Merged requests | 0 | 9,421 | 23,926 |
| Mean logical read (ms) | 21.40 | 60.62 | 27.82 |
| Mean physical read (ms) | 21.39 | 2.89 | 2.53 |
| Aggregate progress lag p95 (s) | 3.364 | 3.497 | 3.539 |
| Reader CPU (s) | 83 | 76 | 72 |
| Sampled RSS peak (MiB) | 1,717.0 | 2,390.6 | 2,297.9 |

Compared with wait32, GETs fell 51.18% and logical read mean fell 54.11%, with
exactly equal logical reads (37,760) and read bytes. Admission mean fell from
56.113 to 0.00471 ms; admitted queue mean rose from 1.240 to 24.082 ms. Thus total
logical wait improved, rather than merely moving the same wait into the queue.
Average GET grew from 82.56 to 169.12 KiB. Q allows neighbors into the planner;
P bounds physical work. There is no minimum batch fullness or timer delay.

Aggregate progress latency did not improve. RSS peak fell 3.88%, but allocator
resident end increased from 3,204.59 to 3,227.84 MiB despite slightly lower active
allocation and equal cache occupancy; the memory issue remains unresolved.
All rows/full-row equality passed, no read errors or discarded lookahead were
observed, and scheduler gauges drained to zero. Owned services/data were cleaned.

This changes implementation and Q together; it does not isolate Q, establish a
universal default, or qualify rollout. One short dev/MinIO round; saved OFF is a
candidate toggle (including end clipping), not main. Evidence and detailed
memory accounting: `target/cross-db-changelog-dispatch/REPORT.txt`. Binary SHA256
`c48a8f8649505f9cb408732dc6922903883669814ff1b5c0a0859829e97b5272`.


## Changelog CPU and debug-check qualification (2026-11-10)

A read-only follow-up confirmed the saved OFF, wait32 and Q1024/P32 full-cluster
runs all enabled storage flush sanity checks: dev builds initialize the flag
from `debug_assertions`, and their effective `unsafe_disable_strict_consistency`
was false. `Materialize NoCheck` skips PK conflict handling, but retains
`ConsistentOldValue` flush validation here. This differs from the corrected
standalone replay, which explicitly disables storage sanity while retaining
strict consistency. No missing streaming-CONFIG warnings occurred in these full
runs; the earlier replay warning storm did not recur.

Reader CPU fell from 83 to 76 to 72 seconds across those three versions. Copied
MV data-block lookups stayed around 2.02 million and metadata lookups around
1.96 million; reducing physical GETs does not remove this work. Enabled sanity
reads are a confirmed additional path, but their CPU fraction is not measured.
The full benchmark deliberately sends at most one batch/second for 32 batches,
so its roughly 33-second wall time is not an unconstrained throughput measure.
Progress latency additionally includes committed-epoch and downstream visibility
boundaries, with roughly 0.5-second SQL observation granularity.

Existing metrics cannot provide an exact CPU decomposition: the write-batch
timer begins after sanity checks, actor poll metrics publish every 15 seconds,
and async duration sums include overlapping waits. Do not interpret the small
write-batch sum as proof that flush validation is cheap, or subtract actor poll
sums from process CPU. Evidence: `target/cross-db-changelog-dispatch/` files
`cpu-sanity-audit.txt`, `cpu-duration-audit.json`, and `cpu-read-path-audit.json`.


## Existing timing evidence decomposition (2026-11-10)

`target/read-timing-breakdown/REPORT.txt` and adjacent JSON/CSV decode all saved
changelog reader/writer/meta and distributed join-regroup before/after timing
series (956 labeled histogram series, 394 with observations), without reruns.
Metric-native units and histogram percentile intervals are retained.

Latest changelog admitted queue mean is24.082ms, logical read mean27.818ms,
physicalGETmean2.535ms. Versus saved OFF logical21.396ms this is a latency cost,
despite fewerGETs. Latest replay instead has logical1.073ms versus savedcandidate
1.089ms, and physicalGETmean0.940ms in both; increased queue delay does not explain
its flat runtime. Latest replay's tail-prefetch consumption waits total0.093833s,
excluding all demand/head blocks. Match-phase elapsed7.214s leaves8.705s for the
rest of the15.919s pipeline, without proving that residual is allHashAgg.

Source audit identified important timer boundaries: Foyer get_or_fetch miss
latency runs from handle creation until consumer poll completes, including time
an already-ready prefetch handle is retained; iterator scan measures lifetime;
Join alignment timer records after yielding thebarrier and can include downstream
handling. None is an exclusive CPUphase. No current per-operatorCPU profile or
sanity-onlytimer exists, and the earlier legacy512-wide profile is not reused.


## Release full-cluster changelog startup gate (2026-11-10)

The current release binary was built to repeat the saved cross-database workload:
65,536 rows, 524,288 updates, two CNs with parallelism 4, N3, Q1024/P32,
real MinIO/Foyer, and guarded `risedev d full`. Ordinary release compilation
(opt-level 3, debug assertions OFF, no default connector features) completed in
28m24s. Binary SHA256:
`710dac75d9b3069d9235988eb51dbb1f5e52a32f4847fc847e33a27bc1ce7b88`.

Full startup succeeded in 8.44s and both CN executables/configurations were
verified. SQL setup then failed before workload creation: the release default
license allows 4 CPU cores, while the cluster reports 12; ResourceGroup is
therefore unavailable. Release accepts only the production license issuer, so
the repository's debug test license is not applicable. An appropriate existing
license is required to repeat the original topology. No license checks, CPU
reporting, topology, or production code were changed to bypass this gate.

There are no release changelog performance samples from this attempt. The
previous dev results remain historical evidence, not release results. The source
snapshot remained unchanged through the build/startup attempt; owned services
and failed-startup runtime data were cleaned. The binary and diagnostic records
are retained under `target/cross-db-changelog-release/`, including `REPORT.txt`,
`artifact.json`, `guard.log`, source snapshots, and `cleanup-final.txt`.


## Debug changelog reader CPU profile (2026-11-11)

One guarded reader-only Time Profiler capture reused the exact saved debug
`c48a8f8649505f9cb408732dc6922903883669814ff1b5c0a0859829e97b5272`
binary, original two-CN `risedev d full` workload, parallelism4, N3/Q1024/P32,
real MinIO/Foyer, sanity ON, strict consistency ON, 65,536 rows and 524,288
updates at one batch/second. There was no compile or production code edit.
Sampling started after snapshot catch-up; analysis includes only reader PID8435
Running samples in the explicit incremental measurement window, with 10ms
trimmed from each boundary. Final correctness queries are outside this window.

71,706 samples cover 71.706 CPU seconds versus the coarse process counter's72s;
unresolved weight is0.0084%. Nearest executor attribution is disjoint:

| Owner | Sampled CPU seconds | Percent |
| --- | ---: | ---: |
| Materialize | 40.873 | 57.00 |
| Other storage/cache background | 12.707 | 17.72 |
| Snapshot/changelog backfill | 7.545 | 10.52 |
| Runtime/other | 6.479 | 9.04 |
| Scheduler task and nested IO | 3.798 | 5.30 |
| Aggregate | 0.298 | 0.42 |

Flush sanity-check ancestors account for37.692s/52.56%, overlapping Materialize
and its storage/cache calls. Metadata lookup6.579s includes6.395s under sanity;
Rust debug precondition checks8.449s also overlap. These are not additive phases.
This confirms debug flush validation as the largest identified CPU cost; it
cannot be converted directly into wall-time savings. Ordinary release already
defaults storage sanity off. The existing write_batch_duration excludes these
checks because it starts inside flush_inner, after validation.

Scheduler planning (`take_read_group`) accounts for0.256s/0.357%, whereas its
inclusive3.798s includes physical IO polling. Profiled-run queue mean26.132ms,
logical read29.374ms and physical GET2.356ms show that queue delay persists, but
CPU sampling cannot partition semaphore wait, wake/poll delay and burst timing.
Small total planner CPU does not prove low per-request queue latency or justify
raising P. No minimum batch timer exists. Precise queue attribution remains open.

The capture completed in33.009s with13,445 GETs and24,313 merged requests; these
profiled numbers must not replace the saved unprofiled performance baseline.
SQL confirmed all65,536 rows at revision8, sum524,288 and full-row symmetric
EXCEPT0. The original audit's exact UPDATE metric assertion failed (523,071):
iter-log counters are buffered per table/thread and flush only after more than64
reports. SQL completion does not force that flush. This false gate is retained
as a qualified failure; no lost-row claim or silent passing assertion is made.
An initial startup symlink-path check was corrected by cloning the exact binary;
it failed before workload execution. The captured workload was not rerun.
Owned services/data were cleaned and source snapshots matched through execution.

Complete evidence, sample classifiers, limitations and raw trace:
`target/cross-db-changelog-profile/REPORT.txt`, `analysis.json`, `stacks.folded`,
`COLLECTION.txt`, `audit-qualified.json`, and `n3q1024p32/profile.trace`.


## Scheduler queue stage trace and rejected priority candidate (2026-11-11)

A separate `object_store_read_scheduler=trace` target now allows scheduler
stage diagnosis without logging every Foyer hit. The old
`state_store_read_batch` target is preserved. The new target records existing
request IDs, enqueue/dequeue monotonic offsets, group selection, remaining
queue lengths, free P after selection, and physical read start/return. It adds
no scheduling state, capacity changes or batching timer. Its normal-path event
analysis must reject cancellations, range fallback, errors/retries or missing
ID pairs; it is not a complete cancellation/fallback lifecycle trace. Backend
return does not mean logical response delivery. TRACE is off by default.

Two guarded full debug runs used N3/Q1024/P32, original two-CN changelog SQL,
65,536 rows/524,288 updates at1batch/s, real MinIO/Foyer, sanity ON and reader-only
scheduler TRACE. Both passed10 existing scheduler tests, SQL full-row equality,
row/revision checks, no read errors/fallback/cancellation/discarded prefetch,
source snapshots and owned-runtime/data cleanup. No CPU profiler was enabled.

| Request-weighted mean stage | Original order | Prefer available P |
| --- | ---: | ---: |
| Enqueue to dequeue | 1.202ms | 1.517ms |
| Dequeue to selection | 31.197ms | 29.944ms |
| Selection to physical first poll | 0.069ms | 3.786ms |
| Total queue from event timestamps | 32.469ms | 35.247ms |
| Logical read mean from metrics | 35.563ms | 42.603ms |
| Physical GET mean from metrics | 2.319ms | 5.962ms |

Event membership reconciles exactly with37,838/37,633 scheduled logical requests.
The original request-queue wait overlapped approximate P32 reservation saturation
for only0.153%; the candidate increased that to67.322%, but physical32 overlap
was still only0.537%. Earlier selection mainly moved waiting into reserved,
unpolled futures. It did not provide a latency improvement. The branch-order
candidate was reverted exactly to the tested trace baseline; only independent
TRACE observability remains. Tests were not rerun on byte-identical restoration.

The observed bottleneck is pending-ready execution rather than insufficient P
alone. Scheduler planning and physical future polling share one task; execution
is the next boundary to investigate. These results do not yet separate runtime
wakeup delays, synchronous HTTP work, debug sanity contention and burst effects,
or establish that moving IO to separate tasks is beneficial. No such redesign
was implemented. Do not add batching delays or raise P based on these results.
Both runs contain TRACE overhead and independent SST layouts (bytes differ0.54%);
this is diagnosis, not a release/main throughput comparison.

Evidence and exact binaries:
`target/changelog-queue-trace/REPORT.txt` and
`target/changelog-dispatch-priority/REPORT.txt`, with raw traces, stage parsers,
metrics, baseline/candidate sources, snapshots and rollback record.

Storage flush sanity defaults follow `cfg!(debug_assertions)`: ordinary release,
production, thin-production and patch-production disable it; ci-release
explicitly enables debug assertions and defaults it ON. `debug = "full"`
controls symbols, not these assertions. Build overrides must be checked from
the actual artifact; strict consistency is a separate setting.


### 2026-11-11 scheduler poll attribution

Scheduler poll attribution, 2026-11-11
Binary f4f6e05137396f78bfe0957316d1fe12402b91dfd2cc1d10e5501b0bf3856610; source frozen during test, 10 scheduler unit tests pass.
Original double-CN risedev full, N3/Q1024/P32, batch16/256KiB, debug sanityON; reader stage+poll TRACE, no CPU profiler. Same logical input; independent SST layout.
33.122s, GET13475, bytes2298.197MiB, merged24497, CPU75s. Queue37.364ms, logical40.519ms, physical2.441ms. Trace queue37.405ms. Correctness full-row equality and counts pass, no error/cancellation/fallback/discard, cleanup complete.
Request-weighted ready wait: active scheduler poll99.8255%, nested physical FuturesUnordered poll65.7626%, off-task0.1745%. Total measured parent poll wall5.405s, nested3.904s. 5997 parent polls,137 ending with exhausted coop budget.
Conclusion: ready requests are waiting behind work inside the scheduler task, overwhelmingly not an unpolled dispatcher. Every physical IO future is currently executed by that same task; nested IO dominates request-weighted wait. Remaining active time includes selection, cancellation scans, metrics and diagnostic logs. Existing CPU profile supplies actual CPU stacks (3.798s scheduler+IO vs.256s take_read_group), while poll intervals include OS preemption/log overhead, not pure CPU.
This motivates separating bounded physical task execution from selection, not increasing P or rearranging branches. No claim yet of fixed latency; requires controlled candidate.
Evidence: poll-analysis.json, poll-summary.txt, analyze_poll.py, trace-analysis.json, raw logs/metrics, COLLECTION.txt. TRACE raised latency; not a production performance estimate. Runtime/data cleaned by harness.


### 2026-11-11 scheduler execution isolation and final attribution


Evidence: `target/changelog-independent-io/REPORT.txt`

Candidate stopped before build/workload: 10/11 scheduler tests pass, obsolete first-poll/dequeue timing proxy fails after independent-task boundary. Independent review also found JoinSet unavailable in pinned madsim. No runtime spawned. Candidate archived read_scheduler.rs.joinset-rejected. Next revision uses FuturesUnordered of abort-on-drop JoinHandles, one owner path for both runtimes. Removed proxy test after mapping no-batch-wait, physical cap, and queued neighbor merge to existing pending_neighbors test; bounded top-up retained in source.


Evidence: `target/changelog-spawned-io/REPORT.txt`

Independent physical execution diagnostic, 2026-11-11
Frozen binary a2b592149ac24fb98ebca28fcf8b238436769e403b812123c3db30ca63aa56eb
Same full two-CN workload, N3/Q1024/P32, debug sanityON; stage+poll TRACE. Test10/10, build2m06s; independent Astra owner/API/cancellation review passed. Original JoinSet candidate rejected before workload for madsim incompatibility; old first-poll timing proxy test replaced with actual owner cancellation coverage. Initial type-inference compile failure archived.
Queue37.364->30.689ms, logical40.519->35.026ms; GET13475->13577, bytes2298.197->2285.531MiB; wall33.122->33.367s, CPU75->75. SQL/full-row checks and cleanup pass.
Ready wait active98.282%, nested join poll0.562% (previous nested HTTP65.763%); dispatcher wall3.574s, join polling.034s. Physical tasks now progress independently, but TRACE version still spends considerable time in dispatcher. More importantly INFO pair is required because TRACE creates observer batching effects.
No normal-path error/cancellation/fallback/discard, complete trace IDs, Q/P caps and drained gauges verified. Independent SST layouts, paced input, not main/release/S3.


Evidence: `target/changelog-inline-untraced/REPORT.txt`

Untraced scheduler execution comparison, 2026-11-11
Variant changelog-inline-untraced; SHA f4f6e05137396f78bfe0957316d1fe12402b91dfd2cc1d10e5501b0bf3856610
Uses prior frozen diagnostic binary with all TRACE disabled, INFO logging, no profiler/build. Same deterministic logical workload, fresh independent SST layouts, risedev full twoCN, N3 Q1024 P32 batch16 maxread256KiB, sanityON.
Inline vs spawned: wall33.084/32.960s; GET14585/19726 (+35.25%); bytes2318.249/2284.688MiB (-1.45%); merges23716/18028; CPU71/73s; queue21.650/5.674ms (-73.79%); logical25.965/14.515ms (-44.10%); physical2.393/4.129ms (+72.56%); peakRSS2342.4/2244.3MiB.
No errors/retries/discards; full-row equality and counts pass; cleanup complete. Benefit is latency, not S3 request cost. Physical group count includes118 direct GETs in both. Group-size histogram: singles4923/9373; pairs1365/4243; >=3 8179/5992. Incoming batches<=1:3540/6585. This supports reduced merge opportunity as faster dispatch sees fewer ready neighbors, but without event traces it does not prove each missed pair's stage. Input is paced32batches, so wall remains~33s regardless of logical read latency. One run each, layout/CPU differences prevent precise production benefit estimates.
Raw evidence: summary.json,audit.json,before/after and250ms metrics,SQL,logs,binary.sha256. Candidate is not a safe IO-cost rollout as-is.


Evidence: `target/changelog-spawned-profile/REPORT.txt`

Spawned physical IO CPU attribution, 2026-11-11
Same frozen binary as INFO candidate, no TRACE; reader-only TimeProfiler after snapshot catchup, same full two-CN N3/Q1024/P32 logical input. 70.954 sampled CPU seconds versus process metric70s; unresolved0.00987%.
Disjoint owners: Materialize39.736s56.00%; otherstorage12.250s17.26%; changelog7.332s10.33%; runtime5.815s8.20%; physical IO tasks4.752s6.70%; dispatcher.565s.80%; agg.310s.44%; other scheduler.187s.26%.
Inclusive (overlap, do not sum): flush sanity36.686s51.70%, Foyer18.436s25.98%, debug preconditions8.592s12.11%, meta lookup6.230s8.78%. Dispatcher breakdown: poll_closed.160s, group planning.116s (sorting.059s), ready retain.048s. No residual large dispatcher CPU bug established; don't add queue state to optimize these small totals.
Queue5.816ms and logical14.070ms agree directionally with unprofiled candidate5.674/14.515ms; GET19259 vs candidate19726 (inline INFO14585). Remaining workload wall constrained by32 paced batches plus debug Materialize work, not primarily ready queue. CPU profile does not measure asynchronous waits.
SQL exactcounts/revisions/full-row symmetricEXCEPT0 pass. UPDATE metric522176 vs524288 is intentionally marked inexact (LocalIterLogMetrics batched/lazy flush, not rowloss). Source snapshots same; owned runtime/data cleaned. No read errors/discard.
Candidate rejected for default due substantial GET increase; archived rather than hiding tradeoff. Independent source review passed owner/lifecycle correctness, not performance acceptance. No release/full S3 inference; ordinaryrelease sanityOFF vs ci-release sanityON.
Evidence: analysis.json,analysis-summary.txt,stacks.folded,symbols.json,profile.trace and exportedXML,audit.json,summary.json,COLLECTION.txt.


Final disposition: restored exact pre-goal scheduler source; candidate and temporary poll instrumentation are archived, not retained in current production source. Stage TRACE retained. No correctness bug/deadlock or intentional full-batch timer found. Current execution coupling is an identified latency/coalescing tradeoff; separating physical execution alone fails IO-cost acceptance. Future work needs explicit batching visibility/latency policy rather than relying on serialized CPU work as accidental batching.

## 2026-11-11: explicit prefetch-window batches and ready-packet correction

Preserved existing Blockwise N/byte/range bounds, neighbor handles/hints/statistics, Foyer single-flight and fallback ownership, meta reuse/MultiGet, scheduler oldest/adjacent-overlap/byte limit and Q/P/EOF behavior. Foyer only exposes the authoritative registration role, pinned to `0d624ea20adc70f5f517c27b202d0a794d6daabb`. New window preparation defaults off (`batch_prepare_timeout_ms=0`); independent physical IO also defaults off.

Preparation uses a separate nonblocking reservation bounded by Q and retains it through admission. It takes no Q while waiting on cache decisions. Once complete/cancelled/timed out, first Q is awaited and only immediately available additional Q is acquired; the prefix is published atomically. Late/follower fallbacks remain ordinary reads. Cancellation dissolves the window without cancelling a Foyer producer.

Preflight evidence: 18 scheduler tests passed, 3 real Foyer/iterator tests passed, Foyer role and retained-fallback tests passed, generated configuration freshness passed, normal pinned-source builds passed. Final cross-review found no correctness/lifecycle blocker. The hot-head/slow-owned-disk-tail path was source reviewed; it is not claimed as a separately forced runtime test.

The initial implementation received only one packet when explicit batching was enabled. A deterministic ready A / unrelated object / ready adjacent B test failed with two GETs. The correction restores logical-request collection budget B, retains at most one whole overflow packet with its Q leases, and drains it on channel closure. Both the regression and packet-overflow/shutdown tests pass. No additional wait, N, Q, P, byte limit or timeout was introduced by this correction.

Runtime: `risedev d full`, two CNs, real MinIO/Foyer, dev/debug build with existing sanity checks, INFO only/no profiler. N3/Q1024/P32/B16, fixed65536 initial rows and524288 updates over32 paced batches. Snapshot warm-up excluded; incremental changelog measured. Baseline is the existing optimized read path with inline dispatch and explicit preparation disabled, NOT main. The two candidates use1ms preparation deadline and independent physical execution. Corrected candidate reuses the preceding measured baseline; SST layouts remain independent, one sample per arm.

|Metric|Existing scheduler baseline|Initial explicit batching|Corrected ready collection|
|---|---:|---:|---:|
|GET|13559|19236|18631|
|Logical reads|37748|38045|37559|
|Merged requests|24189|18809|18928|
|Read MiB|2284.670|2301.525|2272.782|
|Logical read mean ms|30.023|11.635|11.701|
|Physical GET mean ms|2.483|3.945|4.076|
|Queue mean ms|26.571|3.612|3.551|
|Prepare mean ms|0.000|0.501|0.492|
|Wall seconds|33.254|33.479|33.394|
|Reader CPU seconds|72|77|77|
|Peak sampled RSS MiB|2479.2|2353.4|2353.0|

Interpretation: correction has605 fewer GETs than initial batching, but logical requests also fell486 and merged-request count increased only119. Do not attribute the full605 reduction to the fix. Corrected logical latency is essentially unchanged (11.635→11.701ms). Compared with baseline, corrected GETs are37.4% higher despite61.0% lower full logical-read latency. This does NOT meet the combined GET/latency acceptance criterion; both new toggles remain disabled by default.

Corrected windows:16384 published packets,15789 complete,595 timeout (3.63%),552 late,34335 prepared cold members (~2.096 per window), no Q/budget/size splits. Thus Q exhaustion or widespread timeout is not the explanation. The per-window visibility contract works, but it has not recovered old cross-window accumulation/merging. The ready-collection bug is proven; the remaining quantitative attribution needs further evidence rather than another blind run.

All three runtime arms: exact SQL `65536|8|8|524288|2048|2048`, symmetric EXCEPT0, exact source-update counter524288, zero read errors/bypass/discarded prefetch. Owned services stopped, runtime data deleted. Pre-start disk-gate failures were not runs; copied old logs there are explicitly marked and excluded. Only unused compiler link/test caches were removed for space; frozen benchmark binaries and evidence retained.

Evidence: `target/read-window-batch/{PLAN.txt,REVIEW.txt,VALIDATION.txt,ready-collection-before.log}`, `target/changelog-window-batch/{COLLECTION.txt,audit.json,summary.json}`, `target/changelog-window-ready/{COLLECTION.txt,tests.log,audit.json,summary.json}`; each runtime root has frozen binary SHA and matching source-before/after manifests.

### Changelog cold-read origin attribution

A guarded `risedev d full` diagnostic pair traced inline IO versus independent
IO plus the 1 ms per-vnode preparation scope. Both used a single dev binary
(`117c62475c4b2ebd28d10cf04bc657fe01321757c55e8e6b71bdcc52ce5a9807`),
two compute nodes, real MinIO/Foyer, N3/Q1024/P32/B16, 65,536 seed rows and
131,072 updates over eight paced rounds. The initial snapshot was excluded.
SST layouts were generated independently. Raw logs, configs, source manifests
and cleanup checks are under `target/changelog-origin-trace/`.

| Metric | Inline | Independent IO and batch |
| --- | ---: | ---: |
| Reader physical GETs | 3,683 | 5,221 |
| Physical MiB | 591.75 | 597.19 |
| Changelog initial-window cold requests | 8,736 | 8,736 |
| Physical groups for those requests | 2,620 | 4,073 |
| Four-member groups, all cross-actor | 1,651 | 96 |

The four reader actors owned vnodes by modulo 4. Among 2,578 separated,
same-epoch, same-side cross-actor GET pairs that were adjacent on the same
object and fit the range cap, 2,536 later cold leader registrations occurred
after the first GET completed. These pairs can overlap. The observation is
about stage ordering, not INFO-mode latency or predicted GET savings.
Same-actor batching with the observed mapping cannot recover the cross-actor
groups. An offline geometry pass over the existing cold descriptors yielded
4,096 groups within that mapping, 2,208 with four contiguous vnode chunks,
and 2,192 without an actor restriction. This assumes all descriptors in the
given scope are available. See `target/changelog-origin-trace/COLLECTION.md`
for the bounded placement and registration plan.

Both arms passed exact row/revision counts and bidirectional equality.
Owned services and runtime data were cleaned up. The trace-only source
instrumentation was removed after capture.

### Changelog vnode assignment and lookahead matrix

Guarded `risedev d full` 2x2 with one dev diagnostic binary
(`3f52fb8d4478a43cd2ed59142077301c686db909888bd32998f2415b12277078`).
Two CNs, real MinIO/Foyer, 65,536 seed rows and 524,288 paced updates
per arm, four reader actors with 64 vnodes each. Independent IO,
Q1024/P32/B16, 1 ms preparation, 256 KiB max range in every arm.
Each arm wrote its own SST layout. The reader traced request origins and
group selection. The initial snapshot was excluded.

| Measure | Modulo N3 | Modulo N16 | Contiguous N3 | Contiguous N16 |
| --- | ---: | ---: | ---: | ---: |
| Reader physical GETs | 17,633 | 16,079 | 14,228 | 14,644 |
| Reader MiB | 2,287.06 | 2,277.47 | 2,268.20 | 2,257.38 |
| Changelog GETs per 1,000 traced cold reads | 424.30 | 384.00 | 335.67 | 352.32 |
| Same-actor cross-vnode merges | 0 | 0 | 7,698 | 6,664 |
| Cross-actor merges | 3,223 | 5,117 | 66 | 64 |
| Object-layer logical mean ms | 18.26 | 24.79 | 27.50 | 25.31 |
| Wall s | 33.92 | 33.97 | 33.91 | 34.06 |

Actual reader bitmaps confirmed 255 cross-actor adjacent vnode boundaries
under modulo placement and 3 with contiguous placement. At N3, contiguous
assignment saved 19.3% of reader GETs. N16 only read ahead three or four
blocks per range, and improved modulo placement by 8.8% but did not add
GET savings under contiguous placement. There was one sample per arm and
TRACE changes timing, so this is a mechanism test rather than a rollout
comparison. Exact counts and full-row equality passed, owned services/data
were cleaned, and the temporary Meta and trace changes were restored.
See `target/changelog-vnode-matrix/COLLECTION.md` for setup, raw artifacts
and full attribution.

### Changelog dispatcher TRACE overhead

One guarded two-CN dev/MinIO 2x2 used the same N3/Q1024/P32 workload
and diagnostic binary for modulo/contiguous vnode assignment with
request TRACE off/on. Each arm processed 524,288 updates over 32 paced
rounds and generated its own SST layout. The reader probe accumulated
durations in memory and exported them via Prometheus.

| Metric | Modulo INFO | Contiguous INFO | Modulo TRACE | Contiguous TRACE |
| --- | ---: | ---: | ---: | ---: |
| Queue mean ms | 3.78 | 3.58 | 19.86 | 17.93 |
| Object-layer logical mean ms | 12.44 | 11.29 | 27.53 | 26.06 |
| Dispatcher poll wall s | 0.58 | 0.56 | 3.32 | 3.51 |
| Group-selection log call wall s | 0 | 0 | 2.58 | 2.80 |

The group-selection TRACE path accounts for 77–80% of dispatcher poll
wall time with TRACE. Without TRACE the previous contiguous-placement
queue penalty did not reproduce. The probe records wall time, including
descheduling, and does not separate formatting from output. Use INFO and
the counter probe for latency comparisons. Each arm passed the exact row
and full-row equality checks. Services and runtime data were cleaned and
the temporary patches were reverted. Full binary identity, raw data,
and reproduction are in `target/changelog-dispatch-stalls/COLLECTION.md`.

### Changelog against clean main with request tracing disabled

A guarded two-CN `risedev d full` run compared clean main `88b9811433` with
the current dev candidate on MinIO/Foyer. Both arms used INFO and disabled
request tracing. Each arm applied 524,288 updates in 32 paced batches after
initial snapshot catch-up and generated its own SST layout. The candidate
used N3/Q1024/P32, 1 ms preparation, and independent physical execution.

| Reader metric | Clean main | Candidate |
| --- | ---: | ---: |
| GETs | 37,546 | 19,373 |
| Read MiB | 2,272.15 | 2,312.18 |
| Physical read mean ms | 17.76 | 3.79 |
| Downstream progress lag p95 s | 3.59 | 3.50 |
| Workload elapsed s | 33.01 | 33.27 |
| Peak sampled RSS MiB | 1,867.81 | 2,334.72 |

GETs fell by 48.4% with 1.76% more bytes. The candidate recorded 18,830
merged logical reads, with 3.12 ms mean scheduler queue wait and 10.73 ms
mean logical read latency. With paced input, the small elapsed time and
progress lag differences do not establish a throughput improvement. Both
arms passed exact counts and full-row equality, and owned services and
runtime data were cleaned up. Full identities and raw evidence are in
`target/changelog-perf-resume/COLLECTION.md`.

### Changelog scale and submission pacing

The same pinned clean main and dev candidate were rerun with request tracing
disabled, two CNs, and MinIO/Foyer. The paced workload applied 5,242,880
updates in 320 one-second batches. The unpaced workload used the original
524,288 updates in 32 batches with consecutive SQL submissions.

| Mode | Metric | Clean main | Candidate |
| --- | --- | ---: | ---: |
| Paced 10x | Elapsed, s | 321.90 | 321.04 |
| Paced 10x | Reader GETs | 377,119 | 198,032 |
| Paced 10x | Meta Reader barrier mean, ms | 399.88 | 226.33 |
| Paced 10x | Sampled downstream progress lag p95, s | 8.40 | 4.51 |
| Unpaced | Elapsed, s | 23.90 | 22.84 |
| Unpaced | Writer last acknowledgement, s | 15.35 | 15.42 |
| Unpaced | Last acknowledgement to all downstream revisions observed, s | 8.54 | 7.41 |
| Unpaced | Reader GETs per 1,000 observed changelog UPDATE rows | 76.93 | 32.00 |

The paced runs followed the same 320 s submission schedule. The unpaced
runs measured a finite burst from one sequential Writer. Multiple batches
could land within one checkpoint interval, so the two unpaced Readers
observed different numbers of changelog UPDATE rows despite identical final
states. All four arms passed exact counts and full-row equality. Each arm
used an independent SST layout. Full results, binary identities and raw
evidence are in `target/changelog-extended/COLLECTION.md`.

### Changelog with four concurrent unpaced writers

The same pinned dev builds processed 524,288 mutations without submission
delays. Four client threads each updated a disjoint key range and waited for
each statement before issuing the next revision for those keys. Both arms
observed four statements in flight and passed full-row equality.

| Metric | Clean main | Candidate |
| --- | ---: | ---: |
| Writer confirmed mutations/s | 75,770 | 53,763 |
| Writer final flush returned, s | 7.91 | 10.83 |
| Flush return to observed Reader catch-up, s | 6.89 | 3.66 |
| Elapsed, s | 14.82 | 14.51 |
| Reader GETs per 1,000 observed changelog UPDATE rows | 84.41 | 35.62 |

The candidate had a shorter observed tail after the Writer flush and a
longer Writer phase. The final visibility windows overlap at the polling
cadence. The workload uses independent SST layouts and one sample per arm.
Exact identities, Writer and Reader metrics, and the measurement protocol
are in `target/changelog-concurrent/COLLECTION.md`.

### Changelog at 10x scale with four paced writers

The same dev builds submitted 5,242,880 updates. Four client threads targeted
one 16,384-row statement each every 2 s for 80 waves, or 32,768 updates/s
nominal. Both writers fell behind that schedule, and both Readers accumulated
a backlog while writing continued.

| Metric | Clean main | Candidate |
| --- | ---: | ---: |
| Last Writer acknowledgement, s | 190.03 | 198.84 |
| Last acknowledgement to observed downstream catch-up, s | 61.08 | 46.40 |
| Elapsed, s | 251.12 | 245.26 |
| Reader revision progress/s during seconds 20–140 | 8,925 | 8,080 |
| Reader revision progress/s during seconds 5–35 after flush | 52,253 | 76,673 |
| Reader successful GETs | 375,707 | 180,042 |
| Reader GETs per 1,000 observed changelog UPDATE rows | 75.18 | 35.68 |

The measured Reader progress improved after the Writer flush. During the
active write window, the candidate processed fewer revisions per second.
Both arms passed exact row counts and full-row equality. The sample uses an
independent SST layout per arm. Exact binaries, stage measurements and raw
evidence are in `target/changelog-concurrent-10x/COLLECTION.md`.

### Changelog at 10x scale with four continuous writers

The same pinned dev builds ran 5,242,880 updates with four concurrent
clients. Each client submitted its next statement when the previous one
returned. Both runs used a revision version column in the temporary source
table and passed exact counts and full-row equality. Earlier unversioned
attempts failed the source checksum on both builds and are excluded.

| Metric | Clean main | Candidate |
| --- | ---: | ---: |
| Elapsed, s | 238.03 | 219.65 |
| Last Writer acknowledgement, s | 182.89 | 173.58 |
| Last acknowledgement to downstream catch-up, s | 55.13 | 46.05 |
| Reader revision progress/s during seconds 20–140 | 10,253 | 10,363 |
| Reader revision progress/s during seconds 5–35 after flush | 62,054 | 77,175 |
| Reader successful GETs | 369,383 | 175,370 |
| Reader GETs per 1,000 observed changelog UPDATE rows | 76.17 | 35.44 |
| Reader read MiB | 22,424.59 | 22,997.83 |

Reader progress was nearly unchanged while writing continued and increased
24.4% during the sampled drain window. The end-to-end time decreased 7.7%,
split between an earlier Writer acknowledgement and a shorter Reader tail.
Each arm used an independent SST layout, so the Writer timing difference
cannot be attributed to the read optimization. The versioned test table also
precludes a pacing-only comparison with the previous unversioned test.
Full identities and raw evidence are in
`target/changelog-unpaced-10x-versioned/COLLECTION.md`.

### Changelog scheduler configuration screening

A single frozen candidate dev binary ran the same versioned changelog
workload with 1,048,576 updates, four unpaced writers, ordinary Meta vnode
placement, and separate Reader/Writer CNs on MinIO/Foyer. All three arms
used N3/Q1024/P32/B16 and a 256 KiB range cap. Exact final counts and
bidirectional row equality passed.

| Reader metric | 0 ms, inline | 0 ms, independent | 1 ms, independent |
| --- | ---: | ---: | ---: |
| GETs per 1,000 observed changelog UPDATE rows | 36.12 | 40.32 | 32.17 |
| Scheduled logical read p95, ms | 40.20 | 23.15 | 20.97 |
| Queue p95, ms | 33.72 | 12.48 | 7.92 |
| Writer flush to Reader catch-up, s | 10.68 | 17.50 | 12.66 |
| End-to-end, s | 39.59 | 42.20 | 36.86 |

The 1 ms preparation window with independent execution is the starting
configuration for subsequent local validation. Inline had less backlog
when the Writer flush returned. With independently generated SSTs and
one sample per configuration, the elapsed figures include differences
in Writer completion and Reader progress during ingestion. See
`target/changelog-config-screen/COLLECTION.md` for raw evidence.

### Fixed-SST comparison of independent I/O and window preparation

A guarded dev `iter_log` replay shared one committed version and four SSTs
across all samples. Each configuration ran six times with a fresh Foyer
cache and rotating order. Every scan returned the same 65,536 changelog
rows and made 1,416 logical object reads totaling 92,043,320 physical
bytes. N3/Q1024/P32/B16 and the 256 KiB range cap were fixed.

| Median | 0 ms, inline | 0 ms, independent | 1 ms, independent |
| --- | ---: | ---: | ---: |
| Reader GETs | 562.5 | 839.5 | 516.5 |
| Scheduler queue mean, ms | 3.91 | 1.70 | 1.13 |
| Scheduled logical read p95, ms | 13.49 | 12.76 | 9.65 |

Independent execution lowered queue time and increased GETs when window
preparation was disabled. The 1 ms preparation deadline restored the
merge opportunities and lowered logical read p95. The replay isolates
the reader and has no concurrent Writer. Use 1 ms plus independent I/O
for local follow-up. Raw samples and evidence scope are recorded in
`target/changelog-fixed-scheduler/COLLECTION.md`.

### Current scheduler configuration on the Unaligned Join DELETE

One guarded full-cluster dev run used the existing 400,000-row Unaligned
Join fixture and the current candidate with prefix lookahead N3, Q1024,
P32, B16, 256 KiB caps, 1 ms preparation, and independent I/O. It passed the
same output and flush-count checks as the saved main and earlier candidate.

| Normal DELETE | Saved main mean | Earlier candidate mean | Current |
| --- | ---: | ---: | ---: |
| Drain, s | 29.268 | 33.628 | 27.899 |
| CN CPU, s | 114 | 131.5 | 110 |
| Physical GETs | 819 | 744.5 | 673 |
| Agg/MV metadata lookups | 5,575,693 | 6,889,488 | 7,556,158 |

The earlier normal DELETE slowdown did not reproduce in this sample.
SST layout differed, and metadata lookups still increased. The single
current sample does not attribute the timing change to the scheduler or
qualify the restart-cold phase. Details and raw evidence are in
`target/unaligned-current-config/COLLECTION.md`.

### Sustained changelog backlog and Reader memory

A guarded 5,242,880-update observation run added Writer, Meta, MinIO, and
host metrics to the unchanged four-producer dev candidate. During writes,
the Writer's committed epoch advanced 1.01 s per wall second, while the
Reader consumed epoch time at 0.23 s/s. The committed-to-consumed gap grew
from 14.7 to 106.7 s. Host CPU had at most 2% idle in 26 of 27 samples from
seconds 87–139. Reader queue and admission means were 1.04 and 0.009 ms.
After Writer flush, the Reader used 5.37 cores versus 2.21 during ingestion
and processed 54,631 versus 8,018 revisions/s. MinIO sent more bytes per
second in drain and showed no sampled server queue. These observations
point to contention and Reader work in the local dev deployment. They do
not attribute the throughput gap to a specific CPU site.

With exact output validated, Reader RSS fell from 3,743 MiB at catch-up to
3,350 MiB after the 30 s post-verification observation. Active jemalloc
bytes and Foyer size changed little. Scheduler gauges were already zero
at catch-up. The saved main/candidate endpoint difference is therefore
insufficient to identify a leak or a persistent cache-size increase.
A matched idle-main comparison is still open. See
`target/changelog-throughput-rss-audit/COLLECTION.md` for the phase data,
source identity, measurement limits, and cleanup.

### Reader heap profiles for sustained changelog

A guarded main/candidate pair reused the 5,242,880-update workload
with jemalloc sampling on the Reader CN. At about 5 million visible
revisions, candidate RSS was 3,682 MiB versus 3,305 MiB on main, while
allocator live bytes were 2,110 versus 2,280 MiB. Sampled heap stacks
show 231 MiB of candidate scheduler response copies, 90 versus 831 MiB
of HTTP/1 read buffers, and about 1 GiB of Foyer disk flusher buffers
on both sides. Scheduler window and request descriptors account for
less than 2 MiB in the selected candidate dumps.

Allocator resident-minus-active was about 417 MiB higher in the
candidate at that stage. Post-verification vmmap also locates about
0.4 GiB of additional resident memory in the mapping tag used by the
pinned jemalloc. The heap data identify live allocation sites and
separate them from the near-end RSS gap. They do not identify its
allocator size-class or page-decay mechanism. Both arms passed exact
row and equality checks. Raw heaps and symbolized stacks are in
`target/changelog-heap-attribution/COLLECTION.md`.

Rechecking the paired scrapes at that same milestone, allocator metadata
was 32 MiB on main and 34 MiB on the candidate. The candidate still had
about 415 MiB more in `resident - active - metadata`. The pinned jemalloc
includes unused dirty pages in `resident`; the residual is a proxy,
not a direct count of dirty pages or a measurement of process RSS.

The recorded compaction tasks used uncompressed output (232 on main and
236 on the candidate); the shared-buffer flush path also writes
uncompressed blocks. The block cache charges a block's logical payload
size, so letting one uncompressed block retain a slice of the whole GET
would undercount the allocation after the other blocks are evicted.
Decoding directly from a temporary shared slice can avoid a copy for
compressed blocks, whose decoder produces independent backing. It
does not remove the required ownership copy for this workload.

### Checked Materialize on a fixed SST version

The distributed replay enabled Overwrite with a downstream dispatcher to
exercise the checked old-row path. Three paired MinIO/Foyer samples reused
one version and manifest. Each arm processed 4,096 Materialize cache misses
and returned the same exact changes. MultiGet with metadata reuse reduced
Materialize meta lookups from 4,096 to 16. Median total physical reads were
four in both arms, with 248,364 bytes in every sample. Median replay elapsed
time was 219.08 ms OFF and 193.13 ms ON. The small fixture validates the
metadata path but has little remote I/O. See the guarded run and exact
configuration in `target/local-read-cross-review/checked-mv-o0dufuuo/`.

### MultiGet metadata reuse at fixed key concurrency

Both arms enabled MultiGet with concurrency 10 and reused the same committed
fixture. A 128-SST fixture with overlapping object ranges exposed the old
admission cost: meta reuse was 22.8% slower while about 801,000 batch cells
were initialized and rejected per ON sample. Checking the budget before
allocating cells removed that regression. The final implementation retains
only completed metadata handles and uses Foyer for loading and single-flight.

| Fixture | Pairs | Median elapsed OFF / ON | Materialize meta lookups OFF / ON | Median GETs OFF / ON | Bytes per arm |
| --- | ---: | ---: | ---: | ---: | ---: |
| 1 SST | 5 | 203.54 / 189.48 ms | 4,096 / 16 | 4 / 4 | 248,364 |
| 128 SSTs | 3 | 2,981.99 / 2,934.02 ms | 1,056,768 / 811,008 | 384 / 384 | 1,024,384 |

MultiGet defaults to retaining metadata when enabled. The comparison uses
local MinIO and measures reuse independently of the point-read concurrency
and scheduler. See `target/local-read-cross-review/meta-reuse-isolation.md`
for identities, all samples and limitations.

### MultiGet enabled versus legacy point reads

The same dev binary replayed HashAgg and checked Materialize separately against
two frozen 256-vnode fixtures, five alternating OFF/ON pairs per operator and
fixture. Each pair toggled only `storage.read_batch.enabled`. Both arms used
10 concurrent point reads, `meta_reuse = true`, and the same enabled scheduler
with Q1024/P32/B16 and a 1 ms window preparation timeout. Each sample started
with fresh Foyer caches and used local MinIO.

| Fixture | Operator | Median input rows/s OFF / ON | Pooled chunk P99 OFF / ON | Median physical GETs OFF / ON |
| --- | --- | ---: | ---: | ---: |
| 5 SSTs, 65,536 groups | HashAgg | 116,921 / 127,016 | 5.63 / 4.66 ms | 28 / 29 |
| 5 SSTs, 65,536 groups | Materialize | 34,708 / 44,995 | 14.71 / 12.94 ms | 40 / 40 |
| 128 SSTs, 16,384 groups | HashAgg | 87,934 / 90,842 | 8.57 / 9.58 ms | 256 / 256 |
| 128 SSTs, 16,384 groups | Materialize | 6,990 / 7,023 | 83.00 / 81.34 ms | 256 / 256 |

Read bytes matched within each operator and fixture. In the 128-SST HashAgg
case, one ON run had a chunk P99 of 18.35 ms, against 7.90–8.32 ms in the
other four ON runs and 7.95–9.18 ms in the OFF runs. That pair had the same
GET count, bytes and mean scheduler queue delay (19 us). The test did not
establish a repeatable tail regression or an I/O saving. The global
MultiGet switch remains opt-in after this dev replay. See
`target/local-read-cross-review/multiget-default-isolation.md` for the exact
binary, per-pair samples, measurement boundaries and raw artifacts.

The 128-SST HashAgg outlier's first 16 chunks took 133.63 ms OFF and
188.63 ms ON. The remaining 240 chunks took 513.13 ms OFF and 507.46 ms
ON. Every physical GET in the original pair fit in the 4 ms histogram
bucket and scheduler queue P99 fit in the 160 us bucket in both arms.
Ten more pairs with the same binary and identical SST content fingerprints
produced a pooled P99 of 8.305 ms OFF and 8.066 ms ON. Across all 15 pairs,
including the outlier, P99 was 8.386 ms OFF and 8.399 ms ON, with 256 GETs
and 495,744 read bytes in every sample. The spike did not recur. This
follow-up found no persistent tail regression on this fixture.

The longer replay uses a shuffled delete for each existing group, so every
input chunk restores state across the full measurement. Each arm uses the
same pinned version and starts with fresh Foyer caches. HashAgg and checked
Materialize run separately. The sampler captures input rows, physical GETs
and read bytes at chunk boundaries about once per second. Chunk percentiles
come from the original per-chunk samples. End-to-end throughput includes
the final barrier. For the middle of the run, use complete windows after
the first two seconds and report how many contain physical GETs. Report
long operator flushes separately and include them in the run totals.

On the 1,048,576-group cache-fit fixture, HashAgg took 33.29 s OFF and
34.05 s ON, and checked Materialize took 70.38 s OFF and 75.26 s ON
(medians of three pairs). Median physical GETs were 377 / 377 and
522 / 521 respectively. Read bytes matched and there were zero GETs in
the complete windows after the first two seconds. All five SSTs had
metadata larger than the default 1 MiB batch retention budget. In the
first ON samples, retaining or replacing the oversized handle occurred
839,356 times in HashAgg and 2,945,777 times in Materialize. The resulting
batch meta hit rates were 20.0% and 6.6%. HashAgg's seconds-long input
stalls at 441,600 and 883,200 rows match the 64 MiB dirty-group flush
threshold and occur with no physical GETs. See
`target/local-read-cross-review/multiget-long-replay.md` for the paired
measurements and raw evidence.

The 262,144-group, 128-SST fixture used a 5 MiB block cache. All 7 middle
windows in each HashAgg sample and all 38–39 in each Materialize sample
contained physical GETs. Across three pairs, median time was 11.36 s OFF
and 11.13 s ON for HashAgg, and 42.42 s OFF and 41.59 s ON for Materialize.
Median physical GETs were 36,792 / 36,271 and 100,857 / 100,595. Physical
GETs equalled logical reads in all pressure samples. The savings appear
in Foyer's outer-fetch count, before the scheduler. The sustained local
replay shows modest gains under cache pressure alongside repeatable
regressions in the large, cache-fit fixture. The global switch remains
opt-in.

The oversized-slot correction keeps the first oversized metadata handle
until the end of each MultiGet and uses the weight already computed by
Foyer. On the same 1,048,576-group Materialize fixture, with MultiGet
enabled on both binaries, elapsed times fell from 75.36 to 66.42 s and
from 75.25 to 66.52 s in two order-balanced pairs. Chunk P99 fell from
29.54 to 27.25 ms and from 29.48 to 27.12 ms. Oversized admissions fell
from about 2.95 million to 4,096 per run. All four runs had identical
read bytes and 522 logical object reads, with 521–522 physical GETs.
See `target/local-read-cross-review/multiget-oversized-fix.md` for raw
evidence and the comparison's scope.

### Fixed-binary metadata reuse replay

The corrected dev binary replayed both long fixtures with MultiGet enabled,
concurrency 10, and the same scheduler in every sample. Only `meta_reuse`
changed. Each case has two alternating pairs and shares one committed version
across its arms. Both fixtures reproduced their archived SST byte manifests.

| Fixture | Operator | Elapsed OFF → ON by pair | Median Foyer meta lookups OFF / ON | Median chunk P99 OFF / ON |
| --- | --- | --- | --- | --- |
| 5 SSTs, cache fit | HashAgg | 31.98 → 31.73 s, 31.95 → 32.15 s | 1,048,576 / 839,947 | 7.69 / 7.79 ms |
| 5 SSTs, cache fit | Materialize | 66.93 → 52.32 s, 52.06 → 51.22 s | 3,153,920 / 2,109,449 | 24.57 / 21.61 ms |
| 128 SSTs, cache pressure | HashAgg | 11.43 → 11.28 s, 11.25 → 11.58 s | 262,144 / 230,427 | 12.78 / 12.83 ms |
| 128 SSTs, cache pressure | Materialize | 42.31 → 43.93 s, 42.52 → 44.10 s | 16,908,288 / 12,976,269 | 48.93 / 53.69 ms |

In the cache-fit fixture, every retained entry used the oversized slot.
Materialize consistently bypassed 1,044,471 Foyer meta lookups, with the same
read bytes and 521–522 physical GETs per sample. The first OFF run was slower
throughout the input than the second OFF run despite identical metadata and
logical read counts. The elapsed benefit varies materially between pairs.
Under cache pressure, Materialize rejected 12,959,744 retention attempts per
ON run. Whole-process CPU increased 3.2–3.9% and elapsed time increased
3.7–3.8%. The scheduler merged no requests, and the small GET difference
matched the Foyer block outer-fetch difference. HashAgg showed no consistent
elapsed improvement in either fixture.

The comparison isolates metadata retention with the current MultiGet
implementation. The HashAgg replay uses COUNT and does not measure how moving
group initialization after the batch read affects overlap with materialized
input scans. See `target/local-read-cross-review/current-meta-reuse-replay.md`
for identities, measurements and raw evidence.

### Rolling HashAgg group restoration

The experimental variant lets each group initialize as soon as its point
read returns, under one concurrency bound. It keeps the current chunk's SST
metadata reuse. Both versions use concurrency 10, meta reuse enabled and
the same scheduler settings. Each fixture uses two alternating pairs with
one shared SST version and input stream.

| SSTs and block cache | Elapsed old → rolling by pair | Physical GETs old / rolling, median | GETs saved by the scheduler across both repeats |
| --- | --- | --- | --- |
| 128, 5 MiB | 10.544 → 10.678 s, 10.345 → 10.644 s | 36,517 / 37,058 | 0 / 0 |
| 5, 64 MiB | 28.086 → 28.940 s, 28.201 → 28.834 s | 376.5 / 376.5 | 1 / 1 |
| 5, 22 MiB | 33.024 → 34.340 s, 33.518 → 34.301 s | 69,365 / 70,725 | 5 / 6 |

The 128-SST fixture has one HashAgg block per SST. The 5-SST fixture has
372 HashAgg blocks, and the 22 MiB cache exposes cold reads in steady state.
It uses the same SST bytes as the 64 MiB case. Metadata lookups differ by
about 0.1% or less across the three cases. In the 22 MiB case the candidate
adds about 2% physical GETs. The increase comes from Foyer block external
fetches, while scheduler merges are effectively unchanged. All 12 output
and state checks passed. This COUNT replay does not cover group
initialization that reads materialized inputs. See
`target/hash-agg-rolling-read/round-summary.md` for timings, CPU, raw
evidence and the experimental source patch.

### Grouped HashAgg point reads

This trial drove bounded HashAgg group futures while Hummock grouped the
SST and block requests they reached in one poll. Keys targeting the same
block shared one Foyer lookup. The existing scheduler window accepted
adjacent block requests on the same SST. A ready point read could start
group initialization while the other reads were pending.

| Replay | Elapsed ordered → grouped by pair | HashAgg block queries, median | Physical GETs, median | GETs saved by the scheduler across both repeats |
| --- | --- | --- | --- | --- |
| 512-wide pipeline | 30.251 → 29.561 s, 29.781 → 29.781 s | 524,482 → 62,478 | 11,590 → 11,590 | 0 → 0 |
| 5 SSTs, 22 MiB block cache | 36.495 → 45.163 s, 36.486 → 44.746 s | 1,048,943 → 1,046,798 | 69,136 → 71,149 | 9 → 48 |

Both versions use concurrency 10, meta reuse, the same scheduler
configuration and one pinned snapshot per fixture. All eight output and
state checks passed. The isolated 22 MiB replay's chunk P99 rose from 9.96
to 11.89 ms and whole-process CPU rose from 63.59 to 73.37 s. Scheduler
merges increased while Foyer external fetches increased by about 2,000 per
sample. The clustered case saves cache lookups with little wall-time
improvement. Both timed workloads use COUNT. The storage regression
checks early delivery and the cross-block seek fallback. See
`target/hashagg-grouped-point-run/round-summary.md` for binary identities,
configuration, accounting and raw results.

The tested point-read prototype used one MultiGet per set of missing keys. Hummock
refills the configured concurrency as keys complete, returns results in
input order and retains reusable SST metadata for the call. Foyer handles
block cache lookups and single-flight. The object-store scheduler can merge
ready reads that miss Foyer.

### Retractable MIN recovery and 128-SST metadata reuse

One dev replay binary compared both paths on each fixed committed snapshot
with real MinIO/Foyer, one actor owning 256 vnodes, C10, and storage sanity
off. The MIN replay restored 65,536 existing groups with eight input
values each. The scalar path bounded complete group restores, while the
ordered MultiGet bounded point reads and returned before group restoration.
Both arms returned the same deletes and drained both state tables.

| Two alternating pairs, median | Scalar restore | Ordered MultiGet |
| --- | ---: | ---: |
| Elapsed, s | 62.764 | 63.614 |
| Chunk P99, ms | 10.792 | 11.654 |
| Process CPU, s | 66.300 | 65.619 |
| GETs | 104,910 | 105,452 |
| Intermediate-state Foyer metadata lookups | 65,536 | 10,244 |

A separate small trace saw point-read and prefix-scan lifetimes overlap in
16 of 32 chunks on the scalar path and zero on the ordered path. The
formal fixture spent about 49 seconds on the final barrier, and the two
arms also differ in metadata reuse. Keep the existing selection until
other materialized-input workloads show a sufficient net benefit.

In the 262,144-group checked Materialize replay, only `meta_reuse`
changed. OFF/ON medians were 41.789/41.240 seconds and chunk P99
46.341/47.457 ms. GETs stayed around 101,000 with zero scheduler merges.
A separate running-thread Time Profiler pair attributed 4.305 fewer CPU
seconds to the Foyer metadata API and 4.063 extra to the batch metadata
path outside Foyer. Candidate and version traversal stayed near 4.6 CPU
seconds. The old elapsed regression did not reproduce. The current
results do not justify new SST coordination or a default change.

See `target/multiget-goal/round-summary.md` for binary identity, exact
settings, evidence paths, CPU sampling method, and cleanup checks.

### Locality Backfill snapshot prefetch

Two order-balanced pairs used `risedev d full`, one compute node, real
MinIO/Foyer, four LocalityProvider actors, and 524,288 identical fact rows
with 512-byte bodies. All four runs passed the same output checks. Both
modes used a 32 MiB block cache and the same scheduler. Blockwise used
three blocks per window with a 256 KiB cap.

| Pair | Locality snapshot, Range → Blockwise | CREATE MV | CN GETs | CN read GiB |
| --- | ---: | ---: | ---: | ---: |
| Range first | 45.44 → 44.92 s | 101.60 → 89.46 s | 182,631 → 188,117 | 11.55 → 11.82 |
| Blockwise first | 51.16 → 47.16 s | 100.85 → 105.68 s | 188,821 → 192,559 | 11.93 → 12.08 |

Snapshot time covers the first start through the last finish among the
four backfill actors. GETs and bytes cover the whole CREATE MV interval.
Blockwise merged more reads and consumed every registered neighbor. Only
14 of 3,242 and 21 of 3,259 neighbors were pending when consumed. Both
pairs show a shorter snapshot phase, but the differences are smaller
than the 5.72-second spread across the Range repeats. Each arm rebuilt
its SSTs. Total CREATE MV time moved in opposite directions as time before
the Locality scan varied. See `target/locality-backfill-ab/round-summary.md`
for exact settings, measurements and cleanup.

### Locality ordering and downstream HashJoin reads

Two order-balanced pairs kept HashJoin Blockwise prefetch enabled and
toggled LocalityProvider on the fact side. Both plans built the same
dimension first. The test used 2,048 keys, 96 dimension rows per key,
an 8 MiB block cache and a 32-row per-key Join cache limit. All 12,288
fact probes missed the Join cache in each arm. Locality grouped repeated
probes by key within each vnode.

| Pair | Sampled probe window, OFF → ON | Right-table Foyer misses | CN GETs in window | CREATE MV |
| --- | ---: | ---: | ---: | ---: |
| OFF first | 7.07 → 3.05 s | 38,217 → 4,142 | 25,064 → 2,886 | 26.68 → 12.54 s |
| ON first | 7.07 → 3.04 s | 35,033 → 4,218 | 21,969 → 2,960 | 14.54 → 12.58 s |

The probe window uses roughly one-second metric samples around the first
Blockwise neighbor registration and the barrier reporting the last probe.
Right-state misses fell by 88–89% in this cache-pressure case. CN GETs
cover all reads in that interval. The DDL comparison also includes varying
time before probes and a different Materialize PK order. See
`target/locality-join-order-ab/round-summary.md` for the exact setup,
raw evidence and cleanup.

### Larger Locality ordering and downstream HashJoin reads

Two order-balanced pairs repeated the same plan comparison with
786,432 dimension rows and 98,304 fact probes, up 4x and 8x respectively.
Each arm started a fresh `risedev d full` cluster with four HashJoin
actors, real MinIO and an 8 MiB Foyer block cache. Both arms kept the
scheduler and HashJoin Blockwise prefetch enabled. All 98,304 probes
missed the Join cache in each arm, and all output checks passed.

| Pair | Sampled probe, OFF → ON | Right-table block misses | CN GETs in window | CN read GiB in window | CREATE MV |
| --- | ---: | ---: | ---: | ---: | ---: |
| OFF first | 51.49 → 19.22 s | 283,364 → 13,757 | 169,657 → 10,490 | 17.27 → 0.83 | 60.08 → 28.72 s |
| ON first | 48.47 → 19.23 s | 245,395 → 14,266 | 131,263 → 10,741 | 14.96 → 0.87 | 56.05 → 28.73 s |

Right-table Foyer miss rate fell from about 99% to 5%, while scheduler
merge counts also fell. The repeated-key probes reuse recently read
blocks before requesting more object-store reads. The formal pairs
disabled debug storage consistency checks in both arms after the
initial dev-build OFF spent about 140 seconds and 510,007 right-table
block misses before its first fact probes. The test deliberately uses
a 32-row Join cache admission limit, and SSTs were built independently.
See `target/locality-join-order-large/round-summary.md` for the binary,
configuration, sampled windows, interrupted preflight and cleanup.

### TPC-H SF1 Q18 Locality Backfill reproduction

The workload from PR #23275 ran on a 2 GiB CN with four-way streaming
parallelism and real MinIO under `risedev d full`. Both arms restored
the same stopped SST and Meta snapshot and passed the independent
100-row output oracle. Both used the default Join cache admission
limit. Debug storage sanity checks, CN async stack trace, the Locality
sort buffer, Blockwise prefetch and the object read scheduler were off.

| Metric | OFF | ON |
| --- | ---: | ---: |
| CREATE MV | 853.1 s | 335.8 s |
| CN GETs | 307,880 | 51,333 |
| CN read GiB | 20.17 | 5.49 |
| CN streaming upload GiB | 2.85 | 3.61 |
| HashJoin cache miss rate | 76.8% | 37.5% |
| Hummock data block accesses | 22,918,021 | 3,195,497 |

Both arms ran to completion. Locality reduced Join cache misses and
the number of block accesses reaching Hummock. Object-store counters
cover CN reads and uploads during CREATE MV. See
`target/locality-tpch-q18/round-summary.md` for exact inputs, timing,
observations, and cleanup.

#### Q18 with Blockwise prefetch and the I/O scheduler

One follow-up run kept Locality ON and restored the same Q18 seed.
The same binary, SQL and execution plan were used. The follow-up enabled
Blockwise for both prefix and Locality Backfill scans and enabled read
coalescing with the binary's default scheduler limits. The output matched
the independent oracle.

| Metric | Locality ON | Locality ON with both optimizations |
| --- | ---: | ---: |
| CREATE MV | 335.77 s | 345.70 s |
| CN GETs | 51,333 | 53,214 |
| CN read GiB | 5.488 | 5.642 |
| Mean physical GET latency | 35.01 ms | 23.92 ms |
| Peak sampled CN RSS | 2,432 MiB | 3,153 MiB |

The scheduler combined 19,939 of its 70,876 logical requests. Physical
GETs over the whole CREATE MV window still increased by 3.7%. The
single combined run took 3.0% longer. The source scan checkpoint,
admission wait, prefetch counters and cleanup evidence are in
`target/locality-tpch-q18/round-summary.md`.

#### Q18 with 16-block prefetch

The same Locality ON plan ran from the same seed with Blockwise set to
16 blocks, a 1 MiB Blockwise window budget, and a 1 MiB scheduler range
cap. The output matched the original 100 rows.

| Metric | Range | Blockwise 3 / 256 KiB | Blockwise 16 / 1 MiB |
| --- | ---: | ---: | ---: |
| CREATE MV | 335.77 s | 345.70 s | 342.84 s |
| CN GETs | 51,333 | 53,214 | 53,695 |
| CN read GiB | 5.488 | 5.642 | 5.484 |
| Peak sampled CN RSS | 2,432 MiB | 3,153 MiB | 2,379 MiB |

Actual multi-block windows averaged 4.82 blocks in the 16-block arm,
up from 2.61. The scan range limited about 92% of its multi-block
windows. GETs after the source scan checkpoint fell by 40.5% versus the
3-block arm. Total GETs rose by 0.9% versus the 3-block arm and 4.6% versus
Range. See `target/locality-tpch-q18/round-summary.md` for the exact
configuration, measurement windows, and raw evidence.

#### Q18 cached-gap trace

A diagnostic repeat of the 16-block arm linked 5,116 LocalityProvider
windows and 4,176 HashJoin windows to their Foyer registrations and
scheduler GET groups. It found zero confirmed cached gaps within the
linked windows. In LocalityProvider, 244 adjacent cold pairs went into
different GETs. In 243 cases the later request had not been dequeued at
the earlier group selection. In 75 of those cases admission itself
occurred after selection. The run completed with the same 100 rows.

The trace run issued 46,284 physical CN GETs compared with 53,695
without tracing. Use the result to identify what happened in the linked
windows. See `target/locality-tpch-q18/round-summary.md` for coverage,
timing limits and raw evidence.

#### Q18 admission-capacity sensitivity

Two further untraced runs restored the same cold seed and frozen binary,
with 16-block / 1 MiB windows, P=32, B=16 and a 1 ms preparation timeout.
Both completed and passed the independent 100-row oracle. Q was changed
from 32 to 128; this also increases the separate preparation budget.

| Metric | New Q32 control | Q128 |
| --- | ---: | ---: |
| CREATE MV | 287.81 s | 354.20 s |
| CN GETs | 43,925 | 55,444 |
| Scheduled logical requests | 70,360 | 83,045 |
| Requests saved by merging | 26,828 | 28,063 |
| Admission prefix-split events | 552 | 0 |
| Preparation budget rejections | 89 | 0 |
| Mean admission wait | 21.037 ms | 8.457 ms |
| Mean admitted-to-dispatch queue time | 0.052 ms | 0.697 ms |
| External compactor streaming-read GiB | 9.701 | 15.938 |

Q128 removed the two capacity-pressure events but did not reduce GETs
or wall time in this pair. An increase of 12,685 scheduled logical
requests exceeded the additional 1,235 merge savings. Compaction and
cache activity differed, so this single pair does not establish a
causal performance regression from Q128. The new Q32 control itself
issued 9,770 fewer GETs than the historical identical configuration,
while merge savings changed by only 31. Do not treat the earlier
single-run 4.6% difference from Range as a stable regression.

Prepared windows can publish an admitted prefix while the tail waits
for Q; the worker may dispatch that prefix before its neighbor becomes
visible. Preparation rejection, timeout and producer polling are also
separate visibility boundaries. The evidence does not support changing
defaults or adding unconditional merge waiting.

The earlier trace's 244 pairs include seven pairs in partially resolved
windows. Restricting to fully resolved LocalityProvider windows gives
237 split pairs, all with later dequeue; 71 prove later admission.
Neither scope has a confirmed cached gap, and late admission alone is
not proof of time spent blocked on Q. See
`target/locality-tpch-q18/round-summary.md`, `admission-comparison.json`
and per-arm summaries for the full metrics, coverage, source/binary
identity, limitations and verified cleanup.

#### Q18 with Q64

Luna ran one untraced Q64 arm from the same cold seed and frozen binary
under guarded `risedev d full`. P32, B16, the 16-block / 1 MiB window
and 1 ms preparation timeout matched the Q32 repeat and Q128. The
100-row oracle passed. Q also changes preparation capacity.

| Metric | Q32 repeat | Q64 | Q128 |
| --- | ---: | ---: | ---: |
| CREATE MV | 287.81 s | 281.84 s | 354.20 s |
| CN GETs | 43,925 | 51,744 | 55,444 |
| CN read GiB | 5.753 | 5.628 | 5.621 |
| CN GETs after source scan checkpoint | 11,320 | 9,676 | 9,218 |
| Scheduled logical reads after checkpoint | 37,724 | 37,721 | 36,801 |
| Reads saved by merging after checkpoint | 26,660 | 28,356 | 27,910 |
| Admission prefix splits after checkpoint | 537 | 15 | 0 |
| Preparation rejects after checkpoint | 68 | 0 | 0 |

The checkpoint is the first five-second sample showing all source scan
rows. Later activity includes Locality and other CN work. Q64 and the
Q32 repeat had almost identical scheduled read counts in that phase.
Q64 saved 1,696 more requests by merging and issued 1,644 fewer CN GETs
in that interval. The early phase issued 9,463 more GETs, dominating
the whole-run total. This supports an admission-pressure effect in the
later phase without identifying every split or establishing a default
Q. See `target/locality-tpch-q18/on-q64/summary.txt` for the exact
run, metrics and oracle evidence.

#### Q18 Q64 preparation permit trim

Luna ran one untraced Q64 arm with the preparation permit trim, the same
cold seed, and guarded `risedev d full`. Effective TOML, SQL and plan
match the previous Q64. The candidate uses the same dev profile and
feature set. The 100-row oracle passed.

| Phase and metric | Previous Q64 | Permit trim Q64 |
| --- | ---: | ---: |
| Through source scan checkpoint, seconds | 40.233 | 45.292 |
| Through checkpoint, preparation rejects | 5 | 4 |
| Through checkpoint, admission splits | 14 | 29 |
| Through checkpoint, CN GETs | 42,068 | 40,367 |
| After checkpoint, seconds | 241.612 | 322.821 |
| After checkpoint, preparation rejects | 0 | 3 |
| After checkpoint, admission splits | 15 | 59 |
| After checkpoint, CN GETs | 9,676 | 9,904 |
| After checkpoint, scheduled logical reads | 37,721 | 37,424 |
| After checkpoint, reads saved by merging | 28,356 | 27,889 |

The split uses the same first five-second sample with all source rows.
The later phase has 228 more GETs and 81.209 more seconds, with 467 fewer
merge savings. It also has 301 more CN CPU seconds and 14.46 million more
SST metadata cache lookups on the fragment 23 HashJoin state tables, all
hits. These phase observations do not establish a causal effect of the
trim on workload time or demonstrate a reduction in preparation rejects
for Q64. See `target/locality-tpch-q18/on-q64-prep-trim/summary.txt` and
the raw metrics beside it.

#### Q18 Q64 whole-window submission

Luna completed one untraced SF1 Q18 run with whole-window submission
under guarded `risedev d full`. It used the same cold seed, SQL, plan and
effective config as the preparation-trim Q64 arm. Both start at Hummock
version 392 with one 2 GiB CN, streaming parallelism 4 and MinIO. Both
use Q64, P32, B16, a 16-block / 1 MiB prefetch window, a 1 MiB scheduler
read limit and a 1 ms preparation timeout. The first attempt was stopped
by the guard when another worktree started a test. Its samples were
archived separately and excluded from this comparison. The completed
retry passed the same exact 100-row oracle.

| Phase and metric | Preparation trim Q64 | Whole-window Q64 |
| --- | ---: | ---: |
| CREATE MV wall time | 368.113 s | 367.399 s |
| CN GETs | 50,271 | 53,914 |
| CN read GiB | 5.742 | 5.610 |
| Mean scheduler admission wait | 15.981 ms | 11.854 ms |
| Mean admitted-to-dispatch queue time | 0.198 ms | 0.197 ms |
| Mean physical GET latency | 1.820 ms | 1.952 ms |
| Through source scan checkpoint, seconds | 45.292 | 45.346 |
| Through checkpoint, CN GETs | 40,367 | 44,609 |
| Through checkpoint, scheduled logical reads | 40,397 | 44,563 |
| Through checkpoint, merge savings | 165 | 89 |
| Through checkpoint, admission splits / preparation rejects | 29 / 4 | 0 / 0 |
| After checkpoint, seconds | 322.821 | 322.053 |
| After checkpoint, CN GETs | 9,904 | 9,305 |
| After checkpoint, scheduled logical reads | 37,424 | 37,140 |
| After checkpoint, merge savings | 27,889 | 28,171 |
| After checkpoint, admission splits / preparation rejects | 59 / 3 | 0 / 4 |

The checkpoint is the first sample with all 13,652,430 source rows
scanned. The later interval contains Locality and downstream CN work.
Eight complete windows in that interval used preparation permits when
the atomic Q acquisition failed. This counter measures admission events,
and does not measure GETs saved. The 599 fewer later GETs reconcile to
284 fewer scheduled reads, 282 more merge savings and 33 fewer reads
outside the scheduler. The 4,242 additional GETs through the checkpoint
reconcile to 4,166 more scheduled reads and 76 fewer merge savings.
By the approximately 40 s samples, before the first LocalityProvider
start in either arm, the candidate already had 3,088 more GETs despite
having scanned fewer source rows. Overall GETs increased 7.2% and wall
time changed by 0.2%. The restored seed controls the starting SST state.
Compactor streaming reads were 18.046 versus 16.472 GiB, so the pair does
not hold subsequent compaction or cache activity fixed.

Both binaries were built from HEAD `817b276bcf69917c61b093f4251d080035d2e618`
with the same dev profile and empty feature set. The whole-window binary
SHA-256 is `dc3cd7db780e88b511f89e1dd96d37c61441b9f48a6c02a0aa3a143e4626efbc`.
The preparation-trim binary SHA-256 is
`7e449f2726c732761106a9fe471387142eb7108b5c43ccd703665d78ed3d578c`.
See `target/locality-tpch-q18/on-q64-whole-window/summary.txt` for raw
evidence, binary identity, and cleanup records.

#### Q18 Q64 I/O distribution repeat

The repeat used the same frozen whole-window binary, cold seed at Hummock
version 392, SQL, plan and Q64 config under guarded `risedev d full`.
The five-second scrapes also retained the relevant I/O histogram buckets.
The first startup failed before DDL because the isolated RiseDev binary
directory lacked MinIO. The next attempt stopped when the guard detected
another Docker test. Its partial samples are archived separately. The
completed arm passed the same exact 100-row oracle.

| CN metric | Through source scan | After source scan | Full run |
| --- | ---: | ---: | ---: |
| Elapsed seconds | 50.384 | 371.907 | 422.291 |
| Physical GETs | 38,944 | 8,564 | 47,508 |
| Read GiB | 3.301 | 2.285 | 5.586 |
| Physical GETs at most 64 KiB | 36,037 | 4,391 | 40,428 |
| Physical GETs larger than 256 KiB | 1,543 | 2,706 | 4,249 |
| Scheduled logical reads | 39,085 | 36,390 | 75,475 |
| Successful scheduler GETs | 38,810 | 8,208 | 47,018 |
| Scheduler GETs with one request | 38,668 | 4,273 | 42,941 |
| Logical requests saved by merging | 275 | 28,182 | 28,457 |
| Selected group boundaries with a positive gap | 1,370 | 6 | 1,376 |
| Positive gaps at most 64 KiB | 4 | 3 | 7 |
| Positive gaps larger than 1 MiB | 738 | 1 | 739 |

The source checkpoint is the first scrape with all 13,652,430 source rows.
Some downstream work had already started at that scrape. The later
interval includes multiple operators, and the histograms are CN-wide.
Both intervals reconcile scheduled requests to successful groups plus
merge savings, and physical GETs to scheduled groups plus unbounded
reads. Seven windows used preparation permits to enter intact. There
were no admission prefix splits or preparation rejects. These observations
check aggregate behavior. The successful read-size histogram does not
have a 1 MiB bucket or a dispatch-route label. The gap histogram records
selected boundaries and does not measure eligible merges or wasted bytes.

Compared with the previous whole-window Q64, the repeat had 6,406 fewer
physical GETs, with 5,665 of the reduction through the source checkpoint.
Scheduled logical reads fell by 6,228 while merge savings increased by
197. After the checkpoint, the two runs saved 28,171 and 28,182 requests
by merging. The earlier Range baseline had 51,333 total GETs and 9,126
after its checkpoint. The two whole-window totals fall on opposite sides
of that baseline. Only three gaps at most 64 KiB were observed after the
repeat's checkpoint, so small gaps are unlikely to explain the differences
in GET counts across these runs.

The frozen binary SHA-256 is
`dc3cd7db780e88b511f89e1dd96d37c61441b9f48a6c02a0aa3a143e4626efbc`.
See `target/locality-tpch-q18/on-q64-whole-window-io-distribution/summary.txt`
and `io-distribution.json` alongside it for raw evidence, disjoint
histograms, and cleanup. The analysis script is
`target/locality-tpch-q18/analyze-io-distribution.py`.

#### Q18 Q64 window identity trace

The checkpoint diagnostic joined the Foyer, admission, and physical dispatch
records. All 770 fully cold 16-block LocalityProvider windows prepared
completely, entered in one packet each, and issued one successful GET each.
The sole admission split followed a deadline in a three-block window.
The [request identity trace](./read-optimization-evidence/q18-window-identity.md)
records the counts, matching rules, and request ranges.
