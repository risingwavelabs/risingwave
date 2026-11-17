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
snapshot_backfill_enabled = false
locality_backfill_enabled = false
max_blocks = 3
max_bytes = 262144

[storage.object_store]
enable_hummock_read_coalescing = true

[storage.object_store.read_scheduler]
use_dedicated_io_runtime = false
dedicated_io_runtime_workers = 2
max_batch_requests = 16
max_admitted_groups = 64
max_inflight_reads = 32
max_read_bytes = 262144

[storage.read_batch]
enabled = false
meta_reuse = false

[batch]
point_get_concurrency = 1
```

The counts and byte budgets must be positive, except that `max_gap_bytes`
accepts zero. The `blockwise_prefetch`, `read_batch`,
and `read_scheduler` sections reject unknown fields. Keep `point_get_concurrency` at 1 for
streaming-only experiments. A value of 1 for `max_blocks` disables speculative
neighbors, while still permitting demand reads.

| Control | Owner and effect | Evidence for tuning |
| --- | --- | --- |
| `blockwise_prefetch.enabled` | Bounded blockwise lookahead for HashJoin row/degree reads and fully consumed forward scans in Temporal Join, AsOf Join, and OverWindow. | Window distribution and consumed/discarded handles. |
| `changelog_enabled` | Independent opt-in for Hummock change log scans; the vnode/key upper bound is passed to SST iterators before lookahead. | Long cold sequential scans are candidates; verify old/new value rows and epoch/range boundaries. |
| `snapshot_backfill_enabled` | Opt in to Blockwise for ordinary Snapshot Backfill source scans; the default is Range. Independent of prefix scans, LocalityProvider and changelog catch-up. | Compare source scan progress, physical GETs and prefetch memory against Range. |
| `locality_backfill_enabled` | Switches LocalityProvider snapshot scans from contiguous range reads to blockwise prefetch. | Compare backfill duration, physical GETs and bytes with the same scheduler and backfill rate. |
| `max_blocks` | Per-iterator window including the demand block. Later windows are issued as consumption advances. | First-poll `consume_pending` together with material wait duration and consumed windows can motivate an N comparison. Single-block ranges cannot benefit. |
| `max_bytes` | Window sum of max(compressed, decoded) block bytes. An oversized demand block reads alone. This is not a CN-wide memory cap. | `limit="bytes"`, window bytes, RSS and discarded handles. |
| `max_batch_requests` | Maximum members in one group. | Dispatched group sizes. |
| `max_admitted_groups` | Groups retained from queue admission through completion. Compatible reads can join a pending group at capacity. | Group depth, waiting producers and admission latency. |
| `use_dedicated_io_runtime` | Opt in to a process-shared runtime for the dispatcher, scheduled backend reads, range-error fallback, one-yield plug flushes and deferred admission. Foyer producers stay on their existing runtime. Disabled and madsim runs keep the existing runtime. This option is disabled by default. | Physical reservations, active reads and reservation-to-start latency. |
| `dedicated_io_runtime_workers` | Worker threads in the shared runtime, when enabled. All schedulers in the process that opt in must agree. | Compare with the same streaming workers, G and P. |
| `max_inflight_reads` | Concurrent scheduled physical reads, including retry loops and range-error fallback children. One merged GET uses one slot. | Scheduled physical in-flight gauge and enqueue-to-dispatch latency. Oversized/unbounded direct reads are excluded. |
| `max_read_bytes` | Maximum eligible input and merged range span. | `route="oversized"`, actual operation bytes and latency. |
| `max_gap_bytes` | Maximum gap between merged ranges from the same object. Zero still permits adjacent and overlapping ranges. | GET count, read bytes and overread. |
| `reuse_inflight_reads` | Allow fully covered reads to share a scheduled GET, including readers waiting for group admission. | `inflight_reused`, GET count and latency. |
| `read_batch.enabled` | Opt-in point MultiGet for missing state in HashAgg and Materialize. | `state_store_get_batch_keys`, `state_store_get_batch_duration`, and `state_store_get_batch_outcomes`. |
| `read_batch.meta_reuse` | Reuse Foyer SST metadata handles within one MultiGet or the initial iterator seeks of one HashJoin chunk. | Compare `meta_total` and `meta_batch_ready` for the same table and workload. |

Blockwise submits lookahead through Foyer. Actual outer reads can first collect
in a window plug; closed-plug and ordinary reads use normal admission. Admission
can reuse a covering flight or merge into a compatible pending group. Reads that
need a new group retain descriptors in the scheduler's waiting map while their
submitting futures await G; compatible waiters can be absorbed before obtaining
G. G does not bound these waiters or reads staged in plugs. An ordinary isolated
request can dispatch as soon as G and P are available.
Oversized and unbounded reads go directly to the backend. A dropped iterator
handle does not necessarily cancel its Foyer producer. Dispatcher failure
returns an error and wakes admission waiters.

For an in-image control, disable Blockwise for prefix, source snapshot, changelog
and Locality Backfill paths, read coalescing, and point MultiGet. For a main comparison,
use a separate main image built with the same architecture, features and profile.

To isolate metadata reuse, set `storage.read_batch.enabled = true` in both
arms and toggle only `meta_reuse`. `concurrency` limits concurrent keys per
call. `meta_max_entries` and `meta_max_bytes` bound regular retained handles,
with one extra slot for an oversized SST. Retained handles live until the
MultiGet call returns or the HashJoin chunk ends. HashJoin metadata reuse
does not require point MultiGet to be enabled. Foyer handles metadata loads
and single-flight. Historical results identify the builds and settings they
measured.

For a new SF10 Q18 run, use the [read-path test overlay](./read-optimization-evidence/q18-read-path.toml)
on a dedicated test cluster. It enables ordinary source Snapshot Backfill,
Locality Backfill and prefix Blockwise, read coalescing, in-flight reuse,
MultiGet and SST metadata reuse. It retains G128/P32, N16/B16, 1 MiB, zero
gap and two dedicated I/O workers from the recorded Mac SF10 read-path
profile. The former 100 µs candidate timer has been removed; the current plug
flushes after one yield from the first actual outer read. The ordinary source scan now uses the typed startup config rather
than a process environment flag. Changelog scanning is independent and
remains off in this overlay. The overlay captures read-path settings; cluster
topology, seed and SQL conditions are documented with the archived runs.
The [SF10 Q18 comparison](./read-optimization-evidence/q18-sf10-scheduler-simplify.md) records all three simplified runs and both three-run historical cohorts.
The [historical Q64](./read-optimization-evidence/q18-q64.toml) controls apply
only to their archived binaries. The earlier `max_admitted_requests` counted
logical requests and `batch_prepare_timeout_ms` used a different preparation
policy; neither key is accepted by the current parser.

## Metric meanings

Metrics below are available without TRACE. Storage per-table counters use the usual
Info metric level. Aggregate all relevant CN instances; retain instance labels when
investigating skew. Take deltas over a common interval and drain in-flight operations
before reconciling counts. Counts from different layers are not interchangeable.

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
| `object_store_read_scheduler_group_size` | Logical members per dispatched group. Sum minus count is *planned* consolidation, not exact net GET savings under cancellation/fallback. |
| `object_store_read_scheduler_queue_duration_seconds` | Logical admission to initial physical dispatch, including time waiting for physical capacity. Excludes retry/fallback after that initial dispatch. |
| `object_store_read_scheduler_waiting` | Caller futures waiting for admission; excludes admitted requests. Returns to zero on admission, cancellation or worker failure. |
| `object_store_read_scheduler_admission_duration_seconds` | Admission acquisition duration, including immediately admitted requests and cancelled/failed waits. Logical read latency includes this interval. |
| `object_store_read_scheduler_queued` | Admitted logical requests awaiting their initial physical dispatch; belongs to pending groups. |
| `object_store_read_scheduler_in_flight` | Scheduled physical reads, including retry loops and fallback children; excludes direct reads. Bounded by `max_inflight_reads`. |
| `object_store_read_scheduler_pending` | Admitted queued/in-flight logical requests. Should return to zero when quiescent. |
| `object_store_read_scheduler_groups{stage}` | `admitted` holds a group credit, `queued` is not yet selected, and `ready` excludes groups already claimed by a dispatch reservation. |
| `object_store_read_scheduler_reserved` | Group reads holding a physical permit before the first backend poll. These are not yet in `object_store_read_scheduler_in_flight`. |
| `object_store_read_scheduler_physical_duration_seconds{stage}` | Backend read time including retries (`group` or `fallback`), physical permit wait for fallback (`fallback_wait`), or time from dispatch reservation to backend start (`reservation_to_start`). |
| `object_store_read_scheduler_events{event}` | Group outcomes, queued and pre-dispatch cancellations, successful merged members saved, gap groups, range fallback and nonempty plug flush sizes (`sealed_batch_single`, `sealed_batch_multiple`). |
| `object_store_read_scheduler_overread_bytes` | Gap bytes returned by successful merged reads, including gaps left by cancelled members. |
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
`request_id` to `request_ids` on scheduler selection and dispatch spans to follow a
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

## Local Foyer/batch boundary probe (2026-11-16)

This probe uses Foyer revision `0d624ea20adc70f5f517c27b202d0a794d6daabb`
and the object-store library based on `3842b4d83fb6`, with the local scheduler
module split and test-import fix. It uses real memory and Psync disk caches,
an in-memory object backend, a current-thread caller runtime, and optionally
two dedicated I/O workers. G64/P32/B16 and the 256 KiB range limit are unchanged.
The public-API registration helper mirrors `SstableStore`'s presence hint and
`need_await` logic; it does not execute SST decoding or the iterator itself.

Correctness assertions passed for adjacent cold-range merging, memory hits,
mixed hot/cold registration, disk hits, and stale positive disk-presence hints.
A waiter behind a disk-only `get` successfully supplied its external fallback.
Dropping the lead consumer while another fetch waited preserved the producer;
each cold key still generated exactly one backend GET. These observations
support keeping Foyer's atomic get-or-fetch and fallback ownership intact.
A `Wait` registration cannot in general be treated as incapable of supplying
an external read.

Two controlled cases each ran 100 iterations per setting on macOS ARM64 in
the dev profile, without TRACE or a profiler:

- `shared_missing`: the neighboring key already has a Foyer producer outside
  the window. Its response is gated until the other cold read completes.
  The window's follower does not supply another outer read, so a nonzero
  collection window reaches its deadline. Every setting issued 200 GETs.
- `ready_neighbors`: both cold neighbors register before the batch closes
  and their producers run without an artificial gate. Every setting issued
  100 GETs. Nonzero collection settings released on arrivals without waiting
  for the timer in the observed run.

| Case | Configured wait | Caller runtime batch wait p50 / p99 | Dedicated runtime batch wait p50 / p99 |
| --- | ---: | ---: | ---: |
| Shared missing neighbor | 0 us | 0 / 0 us | 0 / 0 us |
| Shared missing neighbor | 100 us | 1,156.0 / 2,311.7 us | 1,153.5 / 2,309.4 us |
| Shared missing neighbor | 1,000 us | 2,303.6 / 2,386.1 us | 2,297.5 / 2,378.4 us |
| Ready neighbors | 100 us | 3.2 / 11.6 us | 4.3 / 12.3 us |
| Ready neighbors | 1,000 us | 3.2 / 8.9 us | 6.9 / 17.6 us |

Batch wait comes from the scheduler histogram sum delta. For ready neighbors,
each observation averages the two reads; for the shared case it measures the
one held cold read. At zero wait the collector does not record a batch wait.
The JSON also records caller elapsed times, all configurations, source hashes,
sample order, and quantile definitions. Timing is reported, not asserted.

The deadline path uses Tokio `sleep_until`, whose timer has millisecond
granularity. A 100 us setting therefore must not be interpreted as a maximum
100 us delay, even with dedicated workers. This experiment demonstrates a
cost when a candidate does not produce an outer read for this batch. It does
not establish how frequently this happens in SF10, nor measure S3 throughput
or justify changing defaults. Arrival-ready batches do not inherently incur
the same millisecond delay. The earlier fixed-SST preparation experiment above
also remains evidence that waiting can be beneficial when neighbors arrive.

With the normal repository toolchain and native libraries configured, run:

```sh
python3 docs/dev/src/design/read-optimization-evidence/run-foyer-batch-probe.py
```

The [runner](./read-optimization-evidence/run-foyer-batch-probe.py) builds the
locked object-store library and links the [probe](./read-optimization-evidence/foyer-batch-probe.rs)
against those exact Cargo artifacts without changing manifests or the lockfile.
See [recorded results](./read-optimization-evidence/foyer-batch-probe-results.json).

### Lead-only expected candidates follow-up

The RW registration path now calls `batch.expect` only for Foyer `Lead`
registrations. All applicable registrations retain their fallback future and
batch member; `Wait` still counts as a pending cache lookup. Foyer's dependency,
single-flight ownership, and fallback selection are unchanged. A waiter that
later supplies an external read may still join an open batch or enter the
ordinary scheduler path. The policy can lose a late merge opportunity but
avoids holding other reads for a shared producer outside their window.

The following actual storage tests compiled and passed on macOS ARM64:

- `shared_block_fetch_does_not_hold_cold_neighbor_batch`: a cold `Lead` read
  completes within a one-second guard despite a 60-second collection deadline
  and a gated adjacent shared fetch. Dropping the original consumer preserves
  the shared producer. The two cold blocks issue exactly two GETs.
- `disk_cache_presence_is_only_a_batching_hint`: the existing disk-hit and
  stale-positive-hint checks still pass. The extended test registers a `Wait`
  behind a disk-only `get`; it supplies the external fallback after the miss
  while an adjacent cold block also completes.

All 21 object-store scheduler tests also passed. Reproduce with:

```sh
cargo test --locked -p risingwave_storage --lib shared_block_fetch_does_not_hold_cold_neighbor_batch -- --nocapture
cargo test --locked -p risingwave_storage --lib disk_cache_presence_is_only_a_batching_hint -- --nocapture
cargo test --locked -p risingwave_object_store --lib read_scheduler::tests -- --nocapture
python3 docs/dev/src/design/read-optimization-evidence/run-foyer-batch-probe.py --wait-leaders-only
```

The probe's default retains the historical policy for comparison. Its Lead-only
mode passed the same Foyer boundary checks and ran 100 iterations per setting
for both controlled scenarios. With a configured 100 us wait, the shared-missing
case recorded no batch wait or deadline release, versus roughly 1.15 ms median
batch wait in the archived baseline. Caller latency medians were 36.4 us on the
caller runtime and 40.7 us with dedicated I/O. GETs remained 200 per 100 iterations.
Ready neighbors still issued 100 GETs per 100 iterations. The same GET counts
held for the 0 us and 1,000 us settings.

These are sequential local dev experiments, not an interleaved throughput
benchmark. They establish the intended release behavior and preserve the tested
sharing/fallback cases; they do not establish an SF10 speedup or guarantee
unchanged GET counts for delayed waiter-supplied fallbacks. No default G, P,
window, or timer setting changed. Full values and the storage-source hash are
in the [Lead-only results](./read-optimization-evidence/foyer-lead-only-results.json).

### IO lifecycle review follow-up (2026-11-16)

Reviewed iterator registration and consumption, the pinned Foyer producer/follower
handoff, batch sealing and timeout, G admission and waiter absorption, P dispatch,
in-flight reuse, cancellation, range fallback, and backend length/error handling.
Reference implementation checks covered Linux v6.12 `block/blk-merge.c`
(`blk_attempt_bio_merge`, plug merging), `block/blk-mq.c` (plug flush and request
completion), and Seastar `src/core/io_queue.cc` (queued cancellation, dispatch,
and completion accounting). These are lifecycle references, not evidence that
object-store futures have the same cancellation semantics as kernel IO.

1. **RW: pressure incorrectly crossed scheduler instances.** Batch release used
   the exported `read_scheduler_waiting` gauge. Its labels are only media type,
   so independent schedulers sharing the metrics registry also shared the
   pressure signal, despite owning separate G/P budgets. A paused-time regression
   creates two schedulers with G=2: one unrelated staged read plus one local
   staged read incorrectly forces the local batch out before its neighbor
   arrives. The original code failed with only one local G permit remaining
   instead of two. The fix keeps an RAII-owned per-scheduler waiting counter;
   exported metrics continue to aggregate. This changes pressure attribution,
   not the threshold or scheduling policy. Regression:
   `batch_pressure_is_local_to_its_scheduler`.

2. **Pinned Foyer: inflight closure did not reach its producer.** At revision
   `0d624ea20adc70f5f517c27b202d0a794d6daabb`, `InflightManager::enqueue`
   allocated separate `Arc<AtomicBool>` values for the inflight entry and
   `Enqueue::Lead`. `take` marked one closed; `RawFetch` checked the other.
   A real public-API probe starts/gates a fetch, explicitly inserts another value,
   observes the waiting reader receive that insertion, then releases the old
   producer. Both before-first-poll and already-started cases resumed the obsolete
   producer and replaced the inserted value. The same probe using an isolated
   copy with one shared Arc reports zero obsolete resumes and retains the
   insertion. Two regression tests also check that dropping a reader still
   preserves a producer needed by a shared follower. Closure is checked on the
   producer's next poll; this small fix does not add a cancellation wakeup or
   promise atomic last-writer ordering against all concurrent insertions.

   RW has an applicable insertion path in Range prefetch's
   `SstableStore::prefetch_blocks`. For immutable SST blocks the established
   concern is redundant fetch/decode/cache replacement; the synthetic unequal
   values demonstrate the dependency defect, not observed SST corruption.
   The fix and tests live in an isolated Foyer review copy and a separate patch.
   RW's Cargo manifest, lockfile, and cached dependency source are unchanged.

The earlier Lead-only expected-candidate fix remains in place. These findings do
not require moving cache ownership into the scheduler, adding fairness classes,
or waiting for more candidates. No SF10 benefit or Linux replay result is claimed.

The grouped storage run also exposed a test-isolation error in the previously
added shared-neighbor regression: `ObjectStoreMetrics::unused()` clones global
metrics, so its exact GET assertion counted concurrent tests' reads. The fixture
now uses a private registry. The initial grouped run was 10 passed / 1 failed;
the corrected grouped run is 11 passed / 0 failed. This was a regression-test
bug, not evidence of an extra GET in that store.

Final local validation (macOS ARM64, pinned RW toolchain):

- `cargo test --locked -p risingwave_object_store --lib read_scheduler::tests`:
  22 passed, including the new pressure regression.
- `cargo test --locked -p risingwave_storage --lib hummock::sstable_store::tests`:
  11 passed with the private-registry fixture.
- `cargo test --locked -p risingwave_storage --lib hummock::sstable::forward_sstable_iterator::tests`:
  12 passed after the pressure fix.
- Isolated Foyer integration target `inflight_close`: 2 passed. This is focused
  regression coverage, not a run of Foyer's entire workspace test suite.
- `rustfmt --check`, `git diff --check`, and reverse-apply checks for both focused
  patches passed. No dependency revision was changed in RW.

### Remove the scheduler-specific Foyer role API (2026-11-16)

The existing Li0k recovery fork is intentional and is retained. Comparing its
`02d6b5e` revision with `0d624ea` shows five changed files, 55 insertions and
5 deletions, consisting of the registration-role enum, stored leader flag,
public accessors/re-exports and role assertions in tests. No cache/recovery
algorithm changed in that delta. RW now restores `02d6b5ebc7516121ab0ad84ad8780623cfa1c8c5`;
the five Foyer package source entries are the only lockfile changes.

The earlier Lead-only policy used that added role solely to predict batch
candidates. It was not needed for Foyer sharing or fallback. The replacement
is deliberately ready-only: SstableStore retains the fallback and batch token
but does not call `ReadBatch::expect` for cache responses. Actual outer reads
can collect before registration closes, join pending groups through the existing
physical-task handoff, or reuse covering in-flight reads when enabled. No new
RW cache-ownership map, timer, yield, or parallel fetch layer was added.

This removes speculative candidate waits, including disk hits that a Lead
registration cannot predict. A neighbor arriving after the first GET starts
can require another GET; the change does not promise the same merge rate for
staggered arrivals. `prefetch_batch_wait_us` remains a lower-level explicit-
candidate experiment setting, but no longer adds a wait in the current Hummock
path. Its earlier measurements, including Lead-only results, remain historical.

Public-boundary probe, 100 pairs per case, configured wait=100us, macOS ARM64:

| Runtime | Policy | Shared-neighbor batch wait p50 | Shared-neighbor GETs | Ready-neighbor GETs |
| --- | --- | --- | --- | --- |
| Caller | All-pending candidates (diagnostic baseline) | 1157.0us | 200 | 100 |
| Caller | Current ready-only | 0us | 200 | 100 |
| Dedicated, 2 workers | All-pending candidates (diagnostic baseline) | 1166.3us | 200 | 100 |
| Dedicated, 2 workers | Current ready-only | 0us | 200 | 100 |

The probe uses the restored fork for both policies. The baseline is the old
all-pending policy, not Lead-only: the restored dependency has no role API.
Current ready-only also records zero batch waiting with wait=0/1000us and
100 GETs per 100 ready pairs in these runs. Source hashes and all percentiles
are recorded in `read-optimization-evidence/foyer-ready-only-results.json`.
No conclusion about SF10 throughput or staggered-production merge rates follows
from these small fixtures.

Final validation on the restored dependency: scheduler 22/22, SstableStore
11/11, forward iterator 12/12. Real Foyer disk hits, stale positive hints,
disk-only-get fallback, and a shared gated producer all pass. Exact GET counts
and actual SST contents still validate sharing without inspecting role(). The
probe's default is current ready-only; `--legacy-candidates` selects the paired
baseline. The old `--wait-leaders-only` argument is rejected rather than silently
running a different policy. The isolated close-flag patch remains unapplied and
is not needed by these tests. No Foyer source edit or new fork commit was made.

### Remove unused candidate waiting and pressure release (2026-11-16)

Historical intermediate state; registration-close behavior is superseded by the
first-IO plug follow-up below.

The original reviewed RW HEAD (`3842b4d83fb6`) already contained the pressure
heuristic in `ReadBatchInner::release_reason`: stop collecting if the exported
waiting gauge reached G or if no G permit was immediately available. It was
an escape from optional candidate waiting, not enforcement of G/P capacity.
The older 2026-11-11 preparation experiments explain why such waiting was added:
asynchronous cache producers could otherwise miss each other's merge window.
The fixed-SST comparison above did find a benefit from a 1ms preparation window.
That evidence is retained; it does not establish a benefit for the pressure
heuristic or justify keeping an uncalled production state machine.

After the role-API rollback, no production call registered an expected candidate.
Only scheduler tests and the diagnostic baseline called `ReadBatch::expect`.
The ready-only Hummock path therefore did not use candidate grouping, deadlines,
or pressure-based release. These mechanisms are now removed together rather
than retaining a private counter solely for an unused policy:

- `ReadBatch` has collecting/closed states only, represented by an optional
  vector of actual external reads. Closing registration submits that vector.
- Late arrivals use ordinary admission. Window bounds, pending-group merging,
  covering-flight reuse, G admission and P dispatch/fallback limits are retained.
- Candidate lists, per-group timers, expected-range API, arrival timestamps,
  pressure counters and `batch_ready/deadline/pressure` events were removed.
  The batch-wait histogram was removed too; the probe no longer reports a
  missing histogram as a measured zero wait. `waiting` is monitoring only.
- `prefetch_batch_wait_us` was removed from the schema, checked-in profile and
  Linux replay fixture. Config parsing rejects unknown keys, so external configs
  that still set it must remove it. `./risedev generate-example-config` regenerated
  the example and documentation, and a separate freshness test passed.
- The current diagnostic runs ready-only. Historical candidate comparisons and
  source hashes remain in the earlier evidence files; the current binary rejects
  the removed diagnostic modes rather than emulating a retired policy.

`batch.rs` shrank from 298 to 96 lines. Three candidate/pressure tests were
replaced by two tests for partial-window seal/late admission and bounded
registration/cancelled members. The dedicated-runtime test now verifies seal
progress with a parked caller, retaining its deferred admission, cancellation,
fallback and shutdown checks.

Local validation: scheduler 21/21, SstableStore 11/11, forward iterator 12/12,
and generated-config freshness 1/1. The public Foyer boundary probe passes
memory/disk hits, stale hints and disk-only-get fallback. With both caller and
dedicated runtimes, 100 ready-neighbor pairs issue 100 GETs; the gated shared-
neighbor scenario issues the expected 200 GETs with one producer per key.
These are local macOS ARM64 checks, not new SF10 or Linux replay measurements.
The Foyer dependency remains the pre-scheduler `02d6b5e` recovery fork.

### First-IO plug: merge/latency tradeoff (2026-11-16)

This supersedes registration-close flushing in the preceding cleanup. Only RW
changes: Foyer remains the `02d6b5e` recovery fork. The first actual external read
starts a flush task on the scheduler runtime; after one yield it takes the
collected reads exactly once. Count/byte thresholds flush immediately. Dropping
the registration owner does not close the plug. No candidate prediction, timer,
pressure heuristic or configuration knob was added. One-member windows submit
immediately; ordinary point reads never enter a plug.

The public Foyer probe now covers same-runtime and separate two-worker Foyer
producers, with RW dedicated IO both off and on. A final identity assertion checks
the actual producer runtime. Two-read scenarios use an eight-member window to
represent remaining members being cache hits/shared fetches. The second producer
is ready immediately, or yields one/eight times before its outer read. The mixed
scenario additionally registers an independent point read (range 4..5, without a
plug). Its latency is recorded by a separate task, not after awaiting prefetch.
Demand-first completion is also recorded independently of total pair completion.

Measurements use real pinned Foyer and an immediate in-memory backend on macOS
ARM64, with unoptimized workspace libraries. Five alternating before/after
trials each discard 50 warmups and retain 500 samples per scenario. The table
reports the median of five per-trial GET counts and percentiles, not a pooled
percentile. Baseline is the immediately preceding local ready-only code, not the
original remote branch. Full raw trial results and binary hashes are in
[`plug-flush-results.json`](read-optimization-evidence/plug-flush-results.json).

| Scenario | Dedicated IO | Separate Foyer runtime | GETs / 500, before → after | p50 µs, before → after | p99 µs, before → after |
| --- | --- | --- | --- | --- | --- |
| point | false | false | 500 → 500 | 31.17 → 32.88 | 39.54 → 44.50 |
| single_cold_window | false | false | 500 → 500 | 31.92 → 46.67 | 39.83 → 59.96 |
| single_slot | false | false | 500 → 500 | 30.75 → 31.96 | 38.62 → 40.12 |
| pair_ready | false | false | 500 → 500 | 38.33 → 51.67 | 47.21 → 65.25 |
| pair_yield1 | false | false | 1000 → 500 | 62.25 → 54.29 | 75.88 → 65.58 |
| pair_yield8 | false | false | 1000 → 1000 | 154.25 → 156.92 | 175.38 → 181.62 |
| pair_yield1_with_point | false | false | 1500 → 1000 | 78.88 → 72.46 | 97.38 → 87.79 |
| point | false | true | 500 → 500 | 38.00 → 40.25 | 50.33 → 53.00 |
| single_cold_window | false | true | 500 → 500 | 34.88 → 46.17 | 48.33 → 63.75 |
| single_slot | false | true | 500 → 500 | 33.88 → 34.04 | 49.58 → 50.88 |
| pair_ready | false | true | 500 → 500 | 47.83 → 54.38 | 56.54 → 70.00 |
| pair_yield1 | false | true | 538 → 518 | 51.38 → 63.12 | 71.50 → 74.46 |
| pair_yield8 | false | true | 999 → 986 | 93.96 → 104.33 | 152.00 → 173.04 |
| pair_yield1_with_point | false | true | 1410 → 1007 | 71.54 → 66.04 | 83.92 → 82.29 |
| point | true | false | 500 → 500 | 38.67 → 38.75 | 52.04 → 51.08 |
| single_cold_window | true | false | 500 → 500 | 39.88 → 48.04 | 50.25 → 62.17 |
| single_slot | true | false | 500 → 500 | 39.71 → 39.96 | 51.08 → 51.46 |
| pair_ready | true | false | 500 → 500 | 54.96 → 55.08 | 63.42 → 71.00 |
| pair_yield1 | true | false | 999 → 973 | 69.29 → 68.04 | 80.58 → 79.42 |
| pair_yield8 | true | false | 1000 → 1000 | 147.04 → 154.12 | 172.79 → 175.92 |
| pair_yield1_with_point | true | false | 1499 → 1020 | 69.62 → 74.92 | 83.79 → 85.96 |
| point | true | true | 500 → 500 | 40.21 → 40.71 | 48.58 → 50.50 |
| single_cold_window | true | true | 500 → 500 | 40.17 → 41.79 | 50.25 → 59.75 |
| single_slot | true | true | 500 → 500 | 40.29 → 41.79 | 48.83 → 49.12 |
| pair_ready | true | true | 500 → 500 | 47.12 → 48.46 | 57.00 → 65.17 |
| pair_yield1 | true | true | 532 → 536 | 53.08 → 51.96 | 70.33 → 73.25 |
| pair_yield8 | true | true | 998 → 838 | 82.92 → 78.29 | 164.00 → 172.54 |
| pair_yield1_with_point | true | true | 1363 → 1008 | 64.75 → 68.79 | 85.54 → 87.00 |

The one-yield handoff merges the same-runtime one-yield pair (1000 → 500 GETs),
but does not universally improve cross-runtime merging: with dedicated IO and
separate Foyer, the one-yield case is 532 → 536 GETs, while the eight-yield case
is 998 → 838. Additional independent point work changes the scheduling order
and can give the neighbors more opportunity to join. These are measured runtime
interleavings, not a guarantee that any given pair will merge.

Pair completion and demand-first latency must not be conflated. In the
same-runtime one-yield case, total-pair p50 improves 62.25 → 54.29 µs, while
first-demand p50 worsens 33.08 → 53.96 µs (p99 41.38 → 65.08 µs). With separate
Foyer plus dedicated IO, the eight-yield case improves total-pair p50
82.92 → 78.29 µs but worsens first-demand p50 43.50 → 50.04 µs. Reducing GETs can
therefore improve batch completion while delaying the first needed block.

Ordinary point p50 shifts by 0.08–2.25 µs across topologies; its code path adds no
plug wait. Mixed-point p50/p99 improve in these trials. Known one-member windows
avoid the task/yield. However, an incomplete window with only one cold read pays
1.62–14.75 µs extra p50 and 9.50–20.13 µs extra p99. Ready pairs in incomplete
windows can also pay this cost without reducing GETs. This is a real tradeoff,
not a zero-regression result, and debug-build microseconds do not predict SF10.

A simpler spawn-only variant was also measured (three trials). It lowers the
same-runtime single-cold p50 to 33.33 µs, but loses the staggered-pair benefit
(1000 GETs) and the same-runtime mixed-point benefit (1500 total GETs). The
implementation retains one yield, without tuning a timer or adding an adaptive
policy. A yield is neither a wall-clock deadline nor a cross-runtime barrier.

A new shutdown test exposed a lifecycle bug in the first implementation: dropping
the flush runtime before the task's first poll left another runtime's reader
waiting while its member retained the staged sender. A small RAII guard, created
before spawning, now closes the plug and drops senders if the task is cancelled.
The regression failed before this fix and passed after it. Concurrent count/byte
closure is tested for exactly-once submission. Collection also leaves G free for
an independent point read under G=P=1.

The dedicated-runtime test previously assumed two raw `stage` calls always joined
the same window. Autonomous flush makes a late `None` legal (the production caller
already falls back to ordinary admission). Its deferred-admission/fallback checks
now submit explicitly collected groups; its parked-caller plug progress check
remains. Local final validation: scheduler 24/24, SstableStore 11/11, iterator
12/12, plus real Foyer memory/disk/stale-hint/shared-fallback/runtime-identity
checks. Foyer dependency sources and lockfile are unchanged by this plug change.
No SF10, Linux replay, real device/backend, or saturated-runtime claim is made.

### Iterator workload experiment: batching versus latency (2026-11-16)

This follow-up tested alternatives to the first-IO plug against actual RW call
sites. **No experimental production policy was retained.** The original plug and
fixed-window behavior remain; the opt-in fixture and results are retained.

Call-path audit:

- HashJoin (`src/stream/src/executor/join/hash_join.rs`), temporal join and some
  over-window callers request prefix-scan blockwise prefetch. Hummock version
  resolution applies the startup enable switch and the scan's hard upper bound.
- Snapshot Backfill, Locality Backfill and changelog have separate switches.
  Backfill without blockwise enabled still uses Range prefetch.
- Point reads retain `PrefetchMode::Disabled` in SST read options. They share
  the underlying Foyer/object-store path with scans.
- Compaction block streams use `get_stream_for_blocks` / `streaming_read`.
  Oversized/unbounded reads can bypass scheduled bounded-range reads. This is
  not a global test of every RW IO path.

The fixture in
`src/storage/src/hummock/sstable/forward_sstable_iterator/local_io_experiment.rs`
uses real SST files, `rewind` / row-by-row `next`, exact key/value checks, Foyer,
decode and the scheduler. It covers a two-block hard bound, 32-block scans,
500µs of CPU work with a cooperative yield per block, early consumer termination,
four scans on distinct SSTs competing with point gets on a fifth SST, and an
unchanged Range control. Both Foyer and IO runtime placements are varied.
G=4, P=2, lookahead=8 blocks/64KiB, target SST block size=4KiB. A **test-feature
only** InMem backend delay injects a 2ms Tokio sleep after physical dispatch.
Foyer has no disk engine in this fixture; disk-hit/fallback correctness remains
covered by existing tests. This tests the storage leaf path, not whole-executor
or LSM/MVCC performance.

Each invocation discards one warmup and measures five trials. Baseline,
rolling-with-plug and demand-first-with-plug received two more runs in reversed
order. The final cleaned-up demand-first candidate received separate before/after
and after/before runs. Full results, topology, source hashes and statistical
limitations are in
[`iterator-io-results.json`](read-optimization-evidence/iterator-io-results.json).

Six policies were compared: baseline, no plug, no plug plus demand-first,
no plug plus demand-first and half-window refill, rolling refill with the original
plug, and demand-first within the same plug. No Foyer changes were made.

Observations:

1. Removing artificial waiting is not sufficient to reduce first-read latency.
   In the initial dedicated-IO/caller-Foyer long-work run, GETs grew 31→75 and
   first-read p50 grew 4.86→8.20ms. Neighbors were still registered before demand;
   extra split GETs can consume scarce physical capacity.
2. Rolling refill helps isolated long scans but hurts other relevant workloads.
   With dedicated IO and separate Foyer, median-of-three run summaries show:
   long-work scan p50 36.56→27.39ms, GETs 20→35; mixed-load point p50
   4.54→8.99ms, mixed GETs 132→180. Early-stop bytes grew 161945→242685
   (about 50%), and discarded speculative handles 10→30. These are five-trial
   totals; the peak handle count stayed at seven, so the change is additional
   total speculative IO, not a larger retained-window budget.
3. Demand-first registration is not IO priority. Initial results looked better,
   but the final candidate's paired runs were not uniformly better. In the
   dedicated-IO/separate-Foyer long-work case, first p50 was 4.29→4.17ms but
   scan p50 36.94→37.60ms, with 20 GETs on both sides. Mixed scan first p50
   was 4.60→6.04ms while point p50 was roughly 4.60→4.54ms. Registration order
   cannot guarantee producer execution, credit acquisition or completion order.

Consequently all experimental production changes were restored. The unadopted
candidate patch is archived outside the repository in the review outputs. The
retained fixture is executable with:

```sh
cargo test --locked -p risingwave_storage --lib --features test   iterator_io_experiment -- --ignored --nocapture --test-threads=1
```

The candidate passed 43 object-store tests (two local TCP tests required a sandbox
exception and then passed), 12 iterator tests, 11 SstableStore tests and the
backfill-mode routing test. After restoring the original policy, iterator and
SstableStore tests were rerun. A separate 30-scan warm-cache check issued no
GETs: baseline p50 1924.9µs, final candidate 1923.3µs. The candidate seek future
was reduced from 1168 to 1040 bytes by keeping new-window state in the existing
boxed future; baseline was 1000 bytes. None of that candidate is active now.

All timings are local macOS ARM64 unoptimized-build measurements. Timer
quantization, small tail samples and runtime scheduling variability limit their
interpretation. No S3, Linux replay, disk-cache traffic or SF10 benefit is claimed.
The experiment rejects a universal default change; it does not establish that
the retained plug is optimal.


## Plug naming and inactive metric cleanup (2026-11-16)

Removed the unobserved `sealed_batch_empty` event. Nonempty flush counters retain
`sealed_batch_single` and `sealed_batch_multiple` labels for existing queries.
Renamed the window handle/token to `ReadPlug`/`ReadPlugToken`, with `token()`,
`read_plug()` and `read_with_plug()` at the object-store boundary; Hummock call
sites and the executable Foyer probe use the same terminology. Dropping either
handle still does not flush. Historical experiment sections retain their original
API names.

The first-read yield, count/byte closure thresholds, late-read admission,
registration order, cache hint, G/P limits and cancellation paths are unchanged.
No Foyer dependency or configuration changed in this cleanup.

Validation on macOS ARM64, using the locked workspace toolchain:

- Scheduler unit tests: 24 passed, including concurrent closure, runtime shutdown,
  cancellation, merging and credit management. Single/multiple flush counts are
  checked by the existing plug tests.
- SstableStore tests (`--features test`): 11 passed.
- Forward iterator tests (`--features test`): 12 passed.
- Public Foyer/object-store probe: compiled and exited successfully, including
  memory/disk hits, stale hints, shared fallback and interleaved/cross-runtime IO.
- Rust formatting and `git diff --check`: passed.

This is a naming/observability cleanup, not a new performance experiment. No
Linux replay, SF10 run or production-latency improvement is claimed.


## Q18 merge attribution and next comparison design (2026-11-17)

The remote branch was verified at `3842b4d83fb6`. Its archived Q18 datasets are
explicitly SF1; the SF10 overlay is not accompanied by an identified SF10 result
archive. Do not relabel the SF1 evidence as SF10. This review runs no new benchmark.

The identity trace separates three mechanisms:

- Source visibility: 40,471 of 41,124 small StreamScan GETs have an adjacent read
  submitted after the earlier GET completed. SST bounds and the Range fallback
  path identify buffer pressure as the dominant non-tail explanation, without
  per-request buffer-occupancy samples. Snapshot Blockwise routing addresses this
  mechanism when enabled; it already exists in the original branch. A short plug
  cannot merge requests that the iterator has not issued.
- Late visibility: the earlier Locality trace has 244 split adjacent cold pairs;
  243 have late dequeue and 75 have provably late admission. No confirmed cached
  gaps were found in the matched windows. Pending merging helps only before
  physical selection; cross-runtime arrivals may still miss the plug.
- Complete windows: the later Q64 trace has 770 complete cold 16-read windows,
  all in one packet and one GET. The current one-yield plug does not guarantee
  retaining this cohort's merge rate. Removing nonexistent-candidate waits is a
  latency simplification, not proof of a whole-window coalescing improvement.

The next small comparison should use the current Foyer and scheduler in all arms:
R = Range; B = fixed-window Blockwise with only plug creation bypassed in a test
copy; P = current fixed-window Blockwise with the plug. R versus B diagnoses
request production, and B versus P isolates the plug. Do not disable the scheduler
in B, introduce production toggles, or compare different Foyer revisions in this
mechanism experiment.

Use identical SSTs, approximately 64 KiB blocks with actual encoded/decoded sizes
recorded, N16/B16, a 1 MiB limit, G128/P32, zero gap and two dedicated IO workers.
Exercise (1) four scans with Range buffer pressure held by a live stream/tracker,
(2) cold windows with natural and controlled cross-runtime arrivals, and
(3) partial-cold/shared windows plus independent point reads and an early stop.
A zero capacity alone does not force the `usage > capacity` fallback. Four scans
alone also do not prove physical saturation.

Rotate arm order for five rounds after warmup. Verify keys/values and drained
requests. Record actual outer reads, logical scheduled reads, GETs/bytes, group
sizes, first-block and point latency, scan time and retained memory/RSS. Report
p99 only with sufficient independent samples (at least 1,000 per round here).
Keep request TRACE off for timing and use a separate small diagnostic for arrival,
flush and selection ordering. A lower GET count without acceptable first-read,
point-read and memory cost is not a successful optimization.

An eventual original-branch/current Q18 comparison is a separate whole-solution
A/B with frozen binaries, matching cold seeds and effective read-path settings.
The old candidate timer and Foyer role patch differ and must be recorded. Split
source and post-source phases; do not reuse the SF1 source-row cutoff for SF10.


## Executed 16-block R/B/P comparison (2026-11-17)

This is a local mechanism experiment, not an SF10 or historical-binary A/B.
The current implementation and Foyer recovery fork `02d6b5e` were used throughout.
R uses Range prefetch; B uses Blockwise with only `ObjectStoreImpl::read_plug`
bypassed in an isolated diagnostic build; P uses the current first-IO plug.
The bypass is not retained in production. Each arm ran in a fresh process in five
rotating rounds, without request TRACE, profiling or concurrent compilation.

The ignored `sstable_store::plug_experiment::plug_window_io_experiment` uses six
real SSTs, validates scanned keys/values, and checks drained scheduler gauges.
All 16-block cold P/B samples assert exactly 16 actual outer fetches. The first
16 blocks occupy 981,698 encoded bytes, with uncompressed sizes also below the
1 MiB window bound. G128/P32, N16/B16, zero gap and in-flight reuse are fixed.
In these original five rounds the caller is current-thread; Foyer has two
separate workers, and dedicated IO has two workers. The mixed case polls four
scans and the point in one parent task, including synchronous scan validation. The backend is in-memory with a synthetic 2 ms Tokio delay.
This is an unoptimized macOS ARM64 build, with no S3/device bandwidth model.

Each round records 100 cold-window, long-scan and early-stop samples, and 1,000
single-cold, point and mixed-point samples after 20 warmups. Tables below use
medians of five run-level percentiles; counts can be summed across rounds.

| Real SST result | B: no plug | P: current plug |
| --- | ---: | ---: |
| GETs for 500 cold 16-block windows | 1,747 | 918 |
| Windows using exactly one GET | 98/500 | 303/500 |
| Cold-window first-block p50 | 3.762 ms | 3.755 ms |
| Single-cold window first-block p50 / p99 | 3.760 / 4.374 ms | 3.787 / 4.384 ms |
| Isolated point p50 / p99 | 4.164 / 4.759 ms | 4.167 / 4.803 ms |
| Point under four scans, p50 / p99 | 4.365 / 7.186 ms | 4.857 / 7.175 ms |

The plug reduces cold-window GETs by 47.5%, but 197/500 windows still split.
The historical SF1 770/770 complete-window cohort is not a matched control, and
this result cannot establish no regression relative to that older strategy.
There is a consistent small single-cold p50 cost (19–31 microseconds per paired
round), and mixed-point p50 increases in all five rounds (355–741 microseconds).
Do not describe this as a zero-latency-regression result. The original experiment did not attribute the mixed-point cost to a particular
stage. The diagnostic follow-up below narrows that boundary.

With a live Range stream retaining more than the deliberately small 512 KiB
prefetch buffer, four scans of 64 blocks use exactly 256 GETs in R. Median
per-round GET averages are 60.46 in B and 48.60 in P. The respective scan-time
statistics are approximately 271/22.54/22.69 ms (maximum per-scan duration in
each trial). All read 15,707,188 bytes. This reproduces request-visibility loss
in the Range fallback; it is not an estimate of a production speedup. The
unpressured R case means no externally held stream: its concurrent scans can
still contend with each other for that buffer.

Stopping after five blocks reads the same 3,926,792 bytes across R/B/P in this
fixture. Discarded response counts are not a measurement of wasted physical
bytes. Whole-process peak RSS medians are about 99.9/103.4/103.6 MiB, dominated
by fixture/cache/allocator state; this does not establish production memory use.
Four scans do not by themselves establish P32 saturation.

A separate public-Foyer boundary probe uses exact 64 KiB ranges and an immediate
backend, without SST decode. It ran five rounds of 1,000 samples per arm, scenario
and Foyer placement, after 50 warmups. IO stays dedicated in both placements;
`cross=false` means Foyer shares the caller, not that IO shares its runtime.
Natural, simultaneously released and staggered producers, single-cold windows,
shared missing neighbors, shared point followers and independent points passed.
A gated shared point is asserted pending before its existing producer is released.

With separate Foyer workers, natural cold-window mean GETs are 3.35→2.51
(median of run means), and the one-GET fraction is 12.3%→35.0%. Single-cold
first-read p50/p99 is 52.17/62.25→54.79/73.54 microseconds. Shared-point p99 is
177.12→186.46 microseconds, whereas the independent-point case improves in this
small probe. This confirms the overhead without a millisecond timer floor and
shows that isolated-point results cannot stand in for shared/mixed traffic.
Releasing late neighbors only after the head completed never restores one GET,
as expected: a selected/finished physical range cannot expand.

Retained policy: fixed windows, one-yield first-IO plug, existing capacity flush
and pending merge; no timer, prediction, pressure heuristic or Foyer patch was
added. Product opt-in defaults are unchanged. Production cleanup collapses a redundant nested flush guard. The subsequent
warm-neighbor fast path is described below. Experiments are explicit ignored
tests or standalone evidence tools. The performance tradeoff remains unresolved for
latency-sensitive mixed loads; no broader default enablement follows.

Reproduce P (R uses `RW_PLUG_ARM=R`):

```sh
RW_PLUG_ARM=P RW_PLUG_WORKERS=1 RW_PLUG_TASKS=coupled \
RW_PLUG_SAMPLES=1000 cargo test --locked \
  -p risingwave_storage --lib --features test plug_window_io_experiment \
  -- --ignored --nocapture --test-threads=1
RW_PLUG_ARM=P RW_PLUG_SAMPLES=1000 python3 \
  docs/dev/src/design/read-optimization-evidence/run-foyer-batch-probe.py --window16
```

The standalone probe can select B with `RW_PLUG_ARM=B` in the same binary.
The real iterator's B arm instead requires the archived diagnostic-only patch in
an isolated build; the fixture asserts that the chosen binary actually bypasses
plug creation. [Structured results and identities](read-optimization-evidence/plug-window-results.json).
A stale Cargo artifact caught by that assertion and an unavailable system RSS
utility were excluded setup attempts; final processes used `getrusage`.


### Latency follow-up and warm-neighbor fast path (2026-11-17)

The retained iterator fixture now defaults to eight caller workers and separate
scan/point tasks. `RW_PLUG_WORKERS=1 RW_PLUG_TASKS=coupled` reproduces the original
caller model above. `RW_PLUG_CASE` selects a scenario and `RW_PLUG_DELAY_US=0`
removes the synthetic timer. These controls are confined to an ignored test.
Changing the model does not invalidate or erase the original single-thread result.

Three diagnostic rounds of 1,000 mixed points per arm separated the workload
models. Median run-level point p50 values, in milliseconds:

| Caller model / backend | B: no plug | P: plug |
| --- | ---: | ---: |
| One thread, coupled task / 2 ms timer | 4.452 | 4.830 |
| One thread, separate tasks / 2 ms timer | 4.148 | 4.717 |
| Eight workers, separate tasks / 2 ms timer | 4.916 | 4.684 |
| Eight workers, separate tasks / immediate | 1.307 | 0.896 |

Separate tasks alone do not remove the single-thread penalty. Eight workers also
change arrival/completion interleaving; this is not a production thread-count
recommendation or proof that mixed-load latency is solved.

Temporary timestamps, removed after measurement, divided the point path into
fetch admission, backend work, result delivery, decode and caller resumption.
In two further single-thread/coupled runs, the mean backend-complete to Foyer
fetch-resume interval grew from approximately 649 to 882 microseconds (B to P).
Fetch-start to backend-entry instead fell from about 45 to 37 microseconds;
backend timer/wakeup was about 3.47 versus 3.48 ms, and point decode about
64 versus 66 microseconds. The configured 2 ms sleep is not a precise 2 ms IO
service time. Stage means are additive; stage percentiles are not.

The increased interval includes scheduler response delivery and Foyer task
rescheduling. A burst of completed scan blocks competing for execution is a
plausible explanation, not a measured separation of those two components.
RW currently decodes blocks synchronously after the object read resumes in the
Foyer producer. Point requests themselves carry no prefetch plug. These results
do not justify adding a timer, priorities, or a scheduler-specific Foyer API.

A small RW-only fast path is retained: after registering neighbors, if every
retained Foyer response is an immediate memory hit (`!need_await()`), the demand
read receives no plug. Those owned hit responses cannot later issue outer IO.
Any pending response (including shared/disk work) or error keeps the existing
plug. This uses the existing Foyer API, does not poll/wait for candidates and
short-circuits at the first pending neighbor in a fully cold window. Foyer still
owns sharing, disk lookup and fallback. The new regression test fails before the
change, passes afterwards, and also checks that a cold window still flushes.

Clean paired binaries differ only in this fast path. Three alternating rounds
use 2,000 single-cold samples per backend, 200 cold-window samples, and 1,000
point/mixed samples per arm after 20 warmups. No diagnostic timestamps, TRACE,
profiling or concurrent compilation are present. Foyer and IO each retain two
separate workers. Cold/single-cold use one caller thread; points use eight workers
and separate tasks. Values below are median run-level p50/p99 unless counts:

| Case | Before | After |
| --- | ---: | ---: |
| Single cold, immediate backend | 133.54 / 155.08 us | 131.75 / 151.75 us |
| Single cold, 2 ms timer | 3.779 / 4.406 ms | 3.735 / 4.342 ms |
| GETs for 600 fully cold 16-block windows | 1,100 | 988 |
| Exactly one GET in those windows | 367/600 | 400/600 |
| Cold-window head | 3.756 / 4.591 ms | 3.745 / 4.512 ms |
| Isolated point | 4.119 / 4.668 ms | 3.948 / 4.673 ms |
| Point with four cold scans | 4.680 / 6.158 ms | 4.719 / 6.171 ms |

The immediate-backend single-cold gain is small (about 1.8 us at p50).
The 2 ms case has a paired p50 change from about -46 to +87 us, so the aggregate
must not be advertised as a stable 43 us saving. The cold and point paths do not
activate the new branch; their variation is not evidence of an algorithmic
improvement. There is no observed cold-window merge degradation, but neither
zero latency regression in all workloads nor 16-to-1 merging is guaranteed.

[Latency results, stages and binary identities](read-optimization-evidence/plug-latency-results.json)
retain per-run statistics. Original R/B/P results remain separately identified.
Foyer remains at recovery fork `02d6b5e`, with no scheduler-specific patch.

Final validation: 13 iterator tests and 11 SstableStore tests pass after the fast
path; the object-store suite passed all 43 tests earlier in this round and its
scheduling behavior was unchanged afterwards. Storage `cargo check`, object-store
and storage Clippy, formatting, and diff whitespace checks passed. Diagnostic
timestamps and the B bypass are absent from the retained source.


### Scheduler send/receive attribution (2026-11-17)

Two additional diagnostic timestamps bracket the point result channel: immediately
before the scheduler's `oneshot::Sender::send`, and immediately after the receiver
returns inside `MonitoredObjectStore::read_with_plug`. The point uses its own SST,
so it cannot merge with the scan requests. A reset per trial and exactly-one-marker
assertions verify the single-response path; every expected timestamp must exist.
The nine stage durations sum to the point latency in every sample (verified to
within 1e-5 microseconds). No Foyer source or dependency was changed.

Three alternating B/P rounds ran 1,000 measured points after 20 warmups for each
caller model. B bypasses only public plug creation in an isolated binary; P uses
the retained plug and warm-neighbor fast path. Foyer and IO each have two workers,
the backend uses a synthetic 2 ms timer, and four independent SST scans compete
with one independent-SST point. Timestamp collection is not request TRACE. No
compilation or profiling ran alongside timings. This remains an unoptimized
macOS ARM64 mechanism experiment, not a production latency measurement.

Stage values below are means over equal-size runs; end-to-end values are medians
of run-level p50 values. Do not subtract percentiles to attribute stage costs.

| Caller model | Backend done to send, B / P | Send to receive, B / P | Point p50, B / P |
| --- | ---: | ---: | ---: |
| One thread, coupled task | 7.48 / 6.80 us | 645.85 / 967.95 us | 4.169 / 4.860 ms |
| One thread, separate tasks | 7.32 / 6.86 us | 657.14 / 866.97 us | 4.241 / 4.582 ms |
| Eight workers, separate tasks | 3.97 / 3.87 us | 964.60 / 912.84 us | 4.879 / 4.657 ms |

The single-thread increase is after result sending, not in scheduler completion
processing before sending. The receive-to-object-read-return wrapper is under
1 us on average. In the coupled model, send-to-receive increases by about 322 us;
end-to-end mean increases by 226 us because other stages partly offset it. The
point's own decode changes only from about 64 to 68 us. The measured waiting
boundary is the Foyer-hosted producer resuming/polling its channel, including
runtime scheduling/cooperative-budget effects; this does not establish a Foyer
correctness defect. RW decodes other completed scan blocks synchronously in
those producer tasks, so competing completion/decode work is a plausible source
of executor contention. CPU residency within that interval was not profiled.

A control used the exact same binaries with eight Foyer workers, one caller
thread, and separate caller tasks (three further B/P rounds). Send-to-receive
falls to 193/333 us, but decode-done-to-caller-return grows to 565/957 us; this
last interval includes Foyer result/cache publication and caller scheduling.
Point p50 becomes 3.824/5.013 ms. Increasing runtime workers shifts contention
and is not a validated end-to-end fix. No worker count, timer, priority rule or
production decode-placement policy was changed.

The path review covered source policy resolution, iterator window bounds and
consumption, Foyer registration/fallback, plug flush ownership, G admission and
P dispatch, active-read reuse, range-error fallback, retry wrappers, delivery and
cancellation. One separate correctness/resource-lifetime bug was confirmed in
Range prefetch: `prefetch_blocks` spawned its object read and then awaited the
join handle. Cancelling prefetch dropped that handle without aborting the child.
The buffer tracker was released while the physical read, G/P permits, and retry
loop could remain live. This path is not exercised by the Blockwise mixed-point
diagnostic and is not an explanation for those latency numbers.

An isolated candidate fix awaits the owned Range object-read future directly. It removes the
unnecessary task and cancellation-error branch; dropping prefetch now drops its
logical receiver. Existing scheduler cancellation preserves a physical read if
another reader still needs it. Foyer-owned shared producers are unchanged.
The candidate regression `test_cancelled_range_prefetch_releases_backend_read`
failed on the old implementation and passed with that fix. It covers direct reads and scheduled
reads with in-flight reuse both off and on, verifies the backend has started,
then checks tracker/backend release and successful subsequent demand at G1/P1.
The existing scheduler shared-reader cancellation test also passes.

Candidate validation before separating scope: 12 SstableStore, 13 iterator,
and 24 scheduler tests passed;
storage `cargo check` and Clippy pass. A clean, uninstrumented seven-case P smoke
(100 point/single/mixed samples and 10 samples for each other case, plus warmups)
checked values, outer-fetch counts and drained resources with the candidate fix. That smoke
is a correctness check, not an additional performance A/B. No new correctness
failure was found in the reviewed Blockwise/plug path. Linux release, real object
storage, SF10 and production saturation still require separate validation.

[Structured stage results and binary/source identities](read-optimization-evidence/plug-delivery-results.json)
record all 24 diagnostic runs. Diagnostic hooks and bypasses were removed from
the retained source. The following optional patches are evidence only, for an
isolated checkout of this code; do not apply them to a production build:

```sh
git apply docs/dev/src/design/read-optimization-evidence/plug-delivery-diagnostic.patch
RW_PLUG_ARM=P RW_PLUG_WORKERS=1 RW_PLUG_TASKS=coupled \
RW_PLUG_FOYER_WORKERS=2 RW_PLUG_DELAY_US=2000 RW_PLUG_CASE=mixed_points \
RW_PLUG_SAMPLES=1000 cargo test --locked -p risingwave_storage --lib \
  --features test plug_window_io_experiment -- --ignored --nocapture --test-threads=1
```

For B, additionally apply `plug-delivery-no-plug-control.patch` and rebuild with
`RW_PLUG_ARM=B`; the fixture rejects a mislabeled binary. Use separate frozen
binaries and alternating processes for paired timing. For independent tasks,
set `RW_PLUG_TASKS=separate`; caller models use `RW_PLUG_WORKERS=1` or `8`.
Only the separate-task/one-caller control sets `RW_PLUG_FOYER_WORKERS=8`.


Scope decision: Range prefetch is the legacy path, distinct from the Blockwise
mixed-point experiment. The Range cancellation candidate and its new regression
test were removed from the main working tree at the user's request to keep the
changes separate. The bug and successful isolated validation above remain recorded;
they are not a claim that the retained code fixes Range cancellation. The standalone
patch is archived with the review outputs. No Range routing, lifetime, or behavior
change from this turn remains. When coalescing is already enabled, legacy Range
reads can use the common scheduler if their bounds/size are eligible; this is an
existing shared object-read entry, not use of Blockwise's plug.

After separating the Range candidate, all 11 retained SstableStore tests pass
again. The retained execution sources match their state at the start of this
delivery-attribution turn; only evidence/documentation from this turn remains.


### Local decode-contention ablation and rejected yield candidates (2026-11-17)

Decision: **neither candidate is retained**. The decode contention is confirmed
in this local workload, but cooperative yielding redistributes waiting between
independent and shared readers. Foyer, Range, and the execution sources are
unchanged from the start of this experiment. Earlier warm-neighbor/plug changes
remain as before; no new production scheduling rule was added here.

The existing send/receive instrumentation was extended with scan decode start,
end and thread IDs. An ablation still performs all reads and delivers all results,
but substitutes clones of blocks decoded before timing for the four scans.
The independent point still decodes normally. Scanned keys/values remain checked;
the ablation deliberately omits scan decode/checksum work and is not a production
optimization. Normal and ablated cases use the same frozen B/P binaries selected
by a diagnostic environment setting. Each model/arm/mode ran three alternating
rounds of 1,000 samples after 20 warmups (24 processes). B disables public plug
creation; P is the turn-start implementation.

All mixed samples assert 65 outer reads and fixed byte totals. The decoder
records exactly 64 scan blocks on at most two Foyer threads. The nine stage
intervals sum to point latency per sample; overlap cannot exceed the interval
length per worker. First-three-sample raw thread/interval data and all run-level
statistics are retained in the structured evidence.

With P, one caller thread/coupled tasks, send-to-receive mean drops from 908 to
35 us when scan decode is removed; with eight caller workers/separate tasks it
drops from 928 to 57 us. Decode calls overlap about 93% of the two-worker capacity
within that wait (sum of decoder interval intersections divided by twice total
wait). This is wall-time overlap, not a CPU profile. The 64 real decodes consume
about 4.3 ms summed wall time per trial, versus 33–53 us for predecoded clones.
The ablation supplies causal evidence that decode competition drives the observed
receive delay, rather than a scheduler send delay or a Foyer sharing defect.

End-to-end latency still depends on the caller. In the eight-worker P model,
point p50/p99 improves from 5.041/6.690 to 4.556/4.982 ms. In the single-thread
coupled model, p50 instead increases from 5.151 to 6.035 ms: decode-end to caller
return grows from 365 to 1,604 us. That last stage also includes Foyer cache/result
publication and notification; synchronous scan consumption/validation in the
caller makes faster data availability insufficient to guarantee lower point
latency. Do not optimize only the receive-wait metric.

Two small RW-only candidates were then tested without diagnostic timestamps:

- Window yield: after successful IO, yield once before decoding a request that
  carried a plug token. This includes the window's demand head. It improves mixed
  independent points, but shared-head p99 increases in all three single-thread
  rounds by 51–358 us. This version was rejected.
- Neighbor yield: the iterator explicitly marks only registered neighbors as
  read-ahead; those producers yield once before decode, while demand heads and
  point-created producers retain their original path. This uses one internal
  boolean, not a Foyer role API or cache-priority inference. It was also rejected
  after testing points that share a neighbor rather than the head.

The neighbor candidate's primary matrix uses three alternating rounds, 1,000
samples per scenario/arm (and 1,000 full-cold windows per round), plus warmups.
Single-cold/cold-window callers have one thread; standalone points have eight
workers. Mixed and shared-head cases cover both caller models. Foyer and IO have
two separate workers, G128/P32, 16-block/1 MiB windows, zero gap and active reuse.
There are no new runtime workers, timers, priorities, queues, or Foyer patches.
Values are medians of run-level p50/p99, except summed GET counts:

| Case | Turn-start baseline | Neighbor-yield candidate |
| --- | ---: | ---: |
| Single cold, immediate backend | 131.67 / 154.00 us | 131.54 / 149.33 us |
| Single cold, 2 ms timer | 3.751 / 4.339 ms | 3.754 / 4.360 ms |
| Independent point, immediate backend | 106.88 / 123.50 us | 106.17 / 122.29 us |
| Independent point, 2 ms timer | 4.032 / 4.643 ms | 4.092 / 4.698 ms |
| Mixed independent point, one caller thread | 4.702 / 7.069 ms | 3.804 / 6.124 ms |
| Mixed independent point, eight caller workers | 4.692 / 6.127 ms | 4.000 / 5.139 ms |
| Shared head, one caller thread | 3.647 / 6.918 ms | 3.629 / 5.527 ms |
| Shared head, eight caller workers | 3.800 / 5.876 ms | 3.765 / 4.501 ms |
| GETs for 3,000 fully cold cross-runtime windows | 5,224 | 5,136 |
| Exactly one GET in those windows | 1,918/3,000 | 1,991/3,000 |
| Cold-window first block | 3.753 / 4.671 ms | 3.735 / 4.349 ms |

The isolated-point timer differences warranted a follow-up using three rounds
of 2,000 samples per arm: p50/p99 is 4.082/4.639 versus 4.084/4.632 ms. Together
with the immediate-backend control this does not show a stable added point cost;
it also does not establish strict statistical equivalence in all workloads.
There is no observed cold-window merge degradation; small GET-count changes do
not establish an algorithmic merge improvement.

Shared-point testing gates the point until the first scan rewind has registered
its window, asserts the point is pending, and asserts exactly 64 outer reads
(no extra producer for the shared point). A further three paired rounds of 1,000
samples use eight caller workers and share blocks 8 or 15 (zero-based):

| Shared block | Baseline p50 / p99 | Neighbor-yield p50 / p99 |
| --- | ---: | ---: |
| 8 | 4.499 / 5.930 ms | 4.718 / 5.959 ms |
| 15 | 4.979 / 5.959 ms | 4.768 / 5.962 ms |

Block 8 regresses by 187–254 us at p50 in all three paired rounds. A point can
share a producer originally registered as a prefetch neighbor; treating that
producer as deferrable also defers its foreground followers. No data loss or
single-flight correctness failure was observed. This is a latency counterexample
to the simple yielding policy, not a reason to alter Foyer or add reader-priority
state. Both candidates were removed rather than retained as another policy.

Final restoration checks compare all touched execution files byte-for-byte with
the turn-start snapshots. The original Range body and Foyer dependency are
unchanged. All 11 SstableStore, 13 iterator and 24 scheduler tests pass on the
restored baseline, as do storage `cargo check`, Clippy and formatting. The local
causal result does not require Linux; production performance is not inferred
from the unoptimized macOS/synthetic-timer numbers.

[Results, overlap samples and source/binary identities](read-optimization-evidence/plug-decode-results.json)
contain all cohorts, including the rejected first candidate. The following
patches are **unapplied experimental evidence**, for a separate checkout only:

- `plug-decode-diagnostic.patch`: timestamp/interval and predecoded ablation.
- `plug-decode-window-yield-rejected.patch`: rejected whole-window policy.
- `plug-decode-neighbor-yield-rejected.patch`: rejected neighbor-only policy.
- `plug-decode-shared-head.patch`: gated shared-head fixture.
- `plug-decode-shared-neighbor.patch`: shared fixture selecting `RW_SHARED_INDEX`.

To reproduce attribution, apply the diagnostic patch to this retained baseline,
then use the explicit ignored test with `RW_PLUG_ARM=P`, `RW_DECODE_MODE=normal`
or `skip`, `RW_PLUG_CASE=mixed_points`, `RW_PLUG_SAMPLES=1000`,
`RW_PLUG_FOYER_WORKERS=2`, and `RW_PLUG_DELAY_US=2000`. Caller models use
`RW_PLUG_WORKERS=1 RW_PLUG_TASKS=coupled` or
`RW_PLUG_WORKERS=8 RW_PLUG_TASKS=separate`. B additionally needs the existing
`plug-delivery-no-plug-control.patch` and a separately built binary. Candidate
comparisons instead start without diagnostic instrumentation and apply exactly
one rejected-policy patch. For shared tests also apply one shared-fixture patch;
the neighbor fixture requires `RW_SHARED_INDEX=8` or `15`. Do not apply both
shared fixtures or both candidate policies together. Freeze binaries and rotate
process order; do not compile while timing.


## Caller-runtime decode comparison (2026-11-17)

The minimal caller-runtime decode candidate was **rejected**. It only moved external
`CachePolicy::Fill` block decoding to the captured caller handle when runtime IDs
differed, leaving one decoded result per Foyer producer. Same-runtime, NotFill,
Disable, and Range execution remained unchanged. Neither this policy nor the
fixture hooks are applied to the retained execution sources.

This comparison corrects the scope of the preceding two-worker Foyer experiment.
With file cache disabled, Foyer captures the runtime that builds the cache; it does
not automatically use each reader's runtime. RW constructs the state store during
node initialization and runs batch/streaming callers on their own runtimes. The
fixture therefore built both the scheduler and cache in a node runtime with Tokio's
local default of 10 workers. It tested (1) a default-sized 10-worker caller and no
dedicated scheduler runtime; (2) the branch overlay's 8-worker caller and dedicated
2-worker scheduler; and (3) a same-runtime 8-worker control. There was no dedicated
Foyer two-worker runtime. Coalescing, G128/P32, and 16-block/1-MiB windows stayed
fixed, so “defaults” describes runtime topology, not all product switches.

The real SST/iterator/cache/scheduler fixture does not start a full compute server,
SQL execution, disk cache, or S3. It uses macOS ARM64 debug and immediate or 2-ms
Tokio-delayed in-memory reads. Three alternating-order rounds have 20 warmups and
300 samples per arm/case; cold16 has 300 complete windows per round. Calls run on
caller worker tasks. No builds, TRACE, or profilers run during measurement.

An initial ungated shared-point assertion failed on the baseline because the
producer finished before the point registered. Those exploratory samples were
excluded. A test-only backend completion gate now holds reads until the point
registers; each shared case asserts pending status and exactly 64 outer producers.
Independent mixed points assert 65; single-cold/point assert 1; cold16 asserts 16.
Shared points cover zero-based blocks 0, 8, and 15. Values and keys are validated.

Results below are medians of per-round p50 values, in microseconds:

| Runtime topology | Scenario | Baseline | Caller decode |
| --- | --- | ---: | ---: |
| Default-sized | Single cold, immediate backend | 129.54 | 142.42 |
| Default-sized | Isolated point, immediate backend | 105.00 | 117.46 |
| Default-sized | Shared head point, delayed backend | 4484.58 | 4800.29 |
| Default-sized | Shared block 8 point | 4732.67 | 4854.00 |
| Default-sized | Independent mixed point | 4300.88 | 4850.42 |
| Default-sized | Mixed scan completion, independent point | 6100.46 | 6238.38 |
| Branch overlay | Single cold, immediate backend | 136.12 | 149.79 |
| Branch overlay | Isolated point, immediate backend | 110.50 | 123.42 |
| Branch overlay | Shared head point | 3947.42 | 4399.79 |
| Branch overlay | Shared block 8 point | 4394.42 | 4463.62 |
| Branch overlay | Independent mixed point | 4392.88 | 4684.96 |
| Branch overlay | Mixed scan completion, independent point | 5822.54 | 5940.96 |
| Same-runtime control | Isolated point, immediate backend | 101.88 | 101.00 |

Every paired round regressed cross-runtime immediate point p50 by 10.67–15.79 us.
Shared-head p50 regressed by 240.79–315.71 us with default-sized runtimes and
406.00–539.92 us with the branch overlay. Shared middle-block p50 also regressed
in all paired cross-runtime rounds. Scan completion had no consistent benefit;
timer-backed absolute timings vary, including on the same-runtime control.
Full-cold GET totals across 900 windows per arm were 1244 -> 1240 (default-sized),
974 -> 965 (overlay), and 921 -> 927 (same-runtime). These counts show no meaningful
merge improvement or consistent regression, and do not establish exact equivalence.

This rejects unconditional decode migration under these local conditions; it does
not establish a production bottleneck or invalidate the narrower earlier ablation.
No Foyer/Range/dependency/config change was retained. Both comparison binaries
passed 11 store and 13 iterator tests; restored execution sources passed 48 related
tests and `cargo check` and matched the pre-experiment bytes.

Compact per-round evidence and identities are in
`read-optimization-evidence/decode-caller-results.json`. The sibling
`decode-caller-candidate.patch`, `decode-caller-fixture.patch`, and
`decode-caller-backend-fixture.patch` are unapplied archival patches. To reproduce,
build the baseline with only the two fixture patches, freeze its test binary, then
build a second binary with the candidate patch. Run `plug_window_io_experiment`
with `--ignored --nocapture --test-threads=1` and `RW_PLUG_ARM=P`,
`RW_PLUG_TASKS=separate`, `RW_PLUG_CROSS=1`, and `RW_PLUG_SAMPLES=300`
(`3000` for cold16, which divides by 10). For default-sized topology unset
`RW_NODE_WORKERS` and `TOKIO_WORKER_THREADS`, use `RW_PLUG_WORKERS=0` and
`RW_IO_DEDICATED=0`; for the overlay use workers=8 and IO=1; same-runtime uses
workers=8, IO=1, CROSS=0. Select `RW_PLUG_CASE=single_cold|point|cold16|mixed_points`,
`RW_PLUG_DELAY_US=0|2000`, and optionally `RW_SHARED_INDEX=0|8|15` for mixed points.
Unset the shared index for independent cases. Alternate binary order between rounds.


## Locality storage-path plug diagnosis (2026-11-17)

Production scheduling remains unchanged. Only opt-in plug stage/late/flush trace
points and an ignored experiment are retained. Foyer remains at `02d6b5e`, with
no disk cache in this experiment; Range and decode are unchanged.

The fixture calls `HummockVersionReader::iter` with
`PrefetchOptions::prefetch_for_locality_backfill()`, a committed real SST and a
hard upper bound. It consumes and verifies every key/value and the end boundary.
It exercises Locality mode routing, the SST iterator, Foyer and the scheduler;
it does not instantiate the Locality/Join executors or deserialize SQL rows.
Point probes time the SstableStore path independently of scan completion.

The cache is initialized on an eight-worker runtime, scans run on a separate
eight-worker runtime, and dedicated IO has two workers. The initialization worker
count is a local choice: the SF10 record does not identify that runtime's worker
count. G128/P32, 16 blocks/1MiB and zero gap match the optimized read limits.
Target SST blocks are 4KiB; the first 16 contiguous blocks total 64,682 bytes.
The backends are immediate InMem and InMem with a 2ms Tokio sleep after admission.
These unoptimized macOS timings do not estimate production S3 latency.

Three alternating process pairs (current/plan, plan/current, current/plan) each
measure 50 samples per case after five warmups. Tracing is disabled for these
7,200 samples. Both arms use the same test binary; the range plan is a test-only
prototype. The [results](read-optimization-evidence/locality-plug-results.json)
contain pooled p50/p95 and per-round counts/medians;
[full samples](read-optimization-evidence/locality-plug-samples.json.gz) retain
every measurement. The separately collected
[diagnostic trace](read-optimization-evidence/locality-plug-trace.txt) is buffered
in memory and is not used for latency or merge-rate statistics.

| Case, 150 scans | Immediate GETs, current → plan | 2ms GETs, current → plan |
| --- | ---: | ---: |
| 16 cold blocks | 237 → 150 | 210 → 150 |
| 32 cold blocks | 629 → 300 | 363 → 300 |
| Eight contiguous cold blocks, eight warm | 253 → 150 | 220 → 150 |
| Existing point producer on block 7 | 534 → 303 | 544 → 300 |
| Alternating cold/warm blocks | 1,200 → 1,200 | 1,200 → 1,200 |

The current immediate-backend cold16 rounds issue 64/80/93 GETs; the plan issues
50/50/50. All-cold byte counts are identical. The trace records registration
completion, a handoff flush with fewer than 16 reads, late arrivals and separate
physical ranges. It also shows that pending merging sometimes rescues an early
flush; late-arrival counts alone are not lost-merge counts. Registration completion
is insufficient because Foyer producers can enter outer reads later. This
mechanism reproduces without a disk cache or cross-probe dependencies. Its share
of the historical SF10 GET increase remains unmeasured.

The rejected range prototype plans contiguous memory-cold runs before registering
Foyer handles. The first actual outer fetch starts a shared range read; later
callbacks take slices. Each block still follows Foyer get-or-fetch ownership and
decode. Known warm gaps and singleton cold runs use ordinary reads. There is no
collection timer. Its scheduler descriptor is a planned range, so group-member
histograms cannot be compared directly with the current per-block descriptors;
physical GETs and bytes are the comparison boundary.

This prototype duplicates bytes when another Foyer producer already owns a block.
In the 2ms existing-point case, all 150 plan samples read 68,734 bytes for 64,682
unique cold bytes: block 7's 4,052 bytes are read twice (6.26% amplification).
The immediate-backend case duplicates bytes in 147/150 samples. Foyer sharing and
row correctness pass, but physical sharing becomes less efficient. This is a
range-prefetch policy change, not a transparent repair to plug closing.

Immediate-backend point p50/p95 changes from 55.9/60.9 to 55.5/61.5 microseconds;
single-block cold scan first-row p50/p95 changes from 75.9/93.8 to 75.5/83.9.
A point sharing an already registered block improves from 127.3/173.7 to
94.8/138.7, but a point racing registration regresses from 80.0/160.7 to
96.6/226.4. All-cold first-row p50/p95 improves from 170.5/258.4 to 151.4/232.4.
With the 2ms timer backend, cold16 scan p50 changes from 5.140 to 5.312ms. There is
no blanket latency-nonregression claim.

An earlier diagnostic changed only the one-yield close to a 100us Tokio sleep.
Full-window collection improved, while incomplete windows incurred roughly a
millisecond of additional wait locally. It was rejected before the paired range
experiment. Both the [timer change](read-optimization-evidence/locality-timer-rejected.patch)
and the [range prototype](read-optimization-evidence/locality-range-plan-rejected.patch)
are archived, not retained in production. The latter includes the fixture and
applies to clean `c69c12b`; its applicability was checked with `git apply --check`.

Run the retained baseline fixture explicitly:

```sh
RW_LOCALITY_TRIALS=50 cargo test --locked -p risingwave_storage --lib --features test \
  locality_plug_experiment -- --ignored --nocapture --test-threads=1
```

Use `RW_LOCALITY_SCENARIO=cold16 RW_LOCALITY_TRACE=1 RW_LOCALITY_TRIALS=3` for a
short diagnostic. Cache counters are observed only by the test harness; no
metric-driven scheduling policy is introduced. The archived prototype uses
`RW_LOCALITY_PLAN=1` after applying its patch in an isolated clean checkout.

Final restored-source checks: scheduler 24/24, version reader 4/4, SST iterator
13/13 and SstableStore 11/11. All twelve fixture cases on both backends were also
rerun with three measured samples per case, plus warmups. Exact key/value, bounds,
outer-fetch counts and byte-accounting assertions passed. No timer, range plan,
Foyer patch, Range change or decode change is retained.


## Locality scoped range experiment and read-pattern audit (2026-11-17)

This is a storage-boundary mechanism experiment, **not a faithful Locality/Join
workload replay**. No candidate execution code is retained. The ignored fixture
and evidence remain; Foyer, Range and decode are unchanged. The standalone
`read-optimization-evidence/locality-scoped-prototype.patch` reproduces the
measured sources against `c69c12b` (apply on a separate clean checkout). Its
application was checked against the relevant HEAD files.

The measured setup uses four independent tasks, one vnode each, on an
eight-worker streaming runtime; cache initialization uses another eight-worker
runtime, dedicated IO uses two workers. There is no disk cache. A single
committed SST contains four vnodes with 4,097 rows each, 512-byte values, 133
approximately 64KiB blocks and 8,640,079 total block bytes. Three shared physical
vnode-boundary blocks are deliberately ensured. Prefetch and merging are bounded
by 16 blocks/1MiB, zero gap, G128/P32. Immediate InMem and a constant 2ms read-delay
backend isolate scheduling; they do not model bandwidth or MinIO service times.

The arms are current scheduling, a Locality-only planned-range prototype, and
that prototype limited to initial vnode-prefix bounds (resumed scans use current
scheduling). A planned range shares raw bytes within one window; Foyer continues
to own individual block fetch/decode/cache operations. The initial-only variant
is an experiment, not a proposed permanent heuristic. Three rotated arm orders
measure 10 samples after two warmups for each of 12 scenarios and two backends:
2,160 measured groups. Each group includes four readers. The summary and all
samples are `locality-scoped-results.json` and `locality-scoped-samples.json.gz`
in the evidence directory. Build with `cargo test -p risingwave_storage
--features test --lib --no-run`; run `locality_scoped_experiment --ignored
--nocapture --test-threads=1` on the resulting binary. Set
`RW_LOCALITY_SCOPE_TRIALS=10`; baseline unsets both flags, full sets
`RW_LOCALITY_SCOPED_PLAN=1` and `RW_LOCALITY_INCLUDE_RESUMED=1`, initial-only sets
just the first flag. The flags apply only with the archived prototype patch.

For 30 groups of full scans, immediate physical GET counts are 699/360/360
(current/full/initial-only), and 2ms counts are 544/360/360; bytes match.
Immediate scan p50/p95 milliseconds are 10.246/14.324, 9.699/11.205 and
9.823/15.382; 2ms values are 22.665/24.185, 22.919/24.183 and 23.337/24.343.
GET reductions do not imply proportional wall-time reductions. In forced
staggered starts both variants read 1.513% more bytes than baseline. For forced
restart after row 128, full reads 0.807%/3.682% more bytes (immediate/2ms);
initial-only matches baseline. These are reproducible boundary risks, not
measurements of their frequency in SF10. The `demand_bytes` field is the union
of blocks holding consumed rows, not a minimal required IO oracle: seek can
also read a predecessor block, and early-stop cases include existing speculative
prefetch. Compare bytes between arms before attributing duplication.

### What the real callers require

- `locality_provider.rs::make_snapshot_stream` scans each actor's vnode bitmap
  sequentially, keeping one iterator per current vnode. It requests Locality
  prefetch, resumes after the last key using an excluded lower bound, deserializes
  SQL rows and rate-limits row output. Four one-vnode tasks model only a subset.
- The executor accumulates rows into chunks and yields them downstream. Exchange
  uses permits for backpressure. Scan and downstream work can overlap while
  buffers have space; they are not independent unbounded tasks and not globally
  lockstep scan/probe pairs.
- An ordinary barrier retains `snapshot_stream`. It is rebuilt only when the
  upstream chunk buffer was nonempty and its writes have been committed. Fixed
  row 1/128/1024 restarts in this fixture do not execute those trigger conditions
  or update the read version. They cannot establish normal restart frequency.
- Real `FlushedStateTableReader` enters `LocalHummockFlushedSnapshotReader`, which
  selects a read snapshot from the live read version before calling
  `HummockVersionReader::iter`. The fixture supplies one committed SST directly;
  it omits immutable batches, multiple/overlapping SSTs and evolving snapshots.
- HashJoin handles input rows serially within a chunk. A Join-cache miss calls
  `fetch_matched_rows_and_get_degree_table_ref`, then the state-table prefix
  iterator with `prefetch_for_prefix_scan()`, consuming matches before proceeding.
  The fixture's independent table-32 `get` calls are not that read pattern.
- Adjacent logical vnodes may share a physical block (SST builder does not always
  cut a block at vnode boundaries), but actual SST/compaction partitions and
  actor vnode assignment determine whether this matters. The fixture forces
  shared boundaries and gates start timing; neither was observed in SF10 traces.

Consequently the measurements establish the intra-window batching mechanism and
some physical-overlap risks. They neither explain the entire SF10 GET increase
nor accept/reject a production policy on expected end-to-end benefit. A useful
next acceptance experiment must connect actual Locality chunk production to
HashJoin prefix reads through bounded exchange, preserve per-actor sequential
vnode traversal and barrier semantics, and distinguish reads by table and window.
Use the existing Q18 configuration (fixed4, eight streaming workers, two IO
workers, no disk, sort buffering off) and a small Q18-shaped data set; keep row
correctness, physical GETs/bytes, first-row and total latency as joint outcomes.
Small synthetic restart/overlap cases remain separate regression checks.

After restoring execution code, the storage test binary rebuilt successfully.
Version (4), forward iterator (13), and SST store (11) tests passed. The retained
scoped fixture also passed all 12 scenarios on both backends, with two measured
samples per scenario/backend (48 groups), plus warmups. `git diff --check` passed.
These are correctness/cleanup checks, not an executor-level read-pattern test.


## Generic Locality/exchange/HashJoin pressure experiment (2026-11-17)

This first harness uses four left rows per key for both joins and a composite
right stream key even in the one-row-per-key Semi case. It exercises real
operators but is **not matched to Q18's side-specific uniqueness/multiplicity**.
Its prototype patch preserves that generic harness; the live ignored fixture is
corrected in the follow-up below. Do not use this table as the Q18-shaped result.

The archived `locality_join_read_pattern` test runs the real LocalityProvider and
HashJoin executors, real Hummock local/flushed snapshot readers and SSTs, SQL row
serde, Foyer, read scheduler and the production local exchange permit channel.
It does not run the SQL optimizer, meta barrier scheduler, full Q18 graph or
MinIO. The source rows, vnode assignment and barriers are controlled inputs.
This is a representative steady Locality-to-Join fragment, not an SF10 replay.

Four Locality tasks each own four of 16 vnodes, traversed sequentially by the
actual executor. Their chunks drive four HashJoin tasks through the permit
channel; Join probes are not independent tasks. Table 34 contains 16,384 keys
with four rows/key (65,536 rows), ordered by join key and row id within vnode.
Right table 32 has one row/key for LeftSemi and three rows/key for Inner.
512-byte deterministic payloads yield multiple 4MiB SSTs with 64KiB block targets.
Each arm starts from newly committed identical seed data and a cleared block
cache. Per-Join-type SST manifests (object IDs, table IDs and sizes) match in all
runs. Result key/row tuples, uniqueness, actor ownership and row counts are
checked: 65,536 output rows for Semi and 196,608 for Inner.

The initial and start-backfill barriers execute normally. A completion barrier
flushes Join counters after the measured interval. No fixed-row restart, forced
cross-reader arrival gate or per-key I/O timer is inserted. This fixture does
not cover active upstream updates or a mid-scan snapshot refresh. The initial
gate synchronizes measurement start across actors, without controlling individual
Foyer or scheduler callbacks. Scans and probes thereafter run at their natural
executor pace under backpressure. Streaming task-local configuration is installed
for every task. Default chunk size is 256, exchange permits are 2048, batched
permits 256, concurrent barriers one; 512 permits is a sensitivity case.

The cache/main runtime has eight workers, streaming has eight, dedicated IO has
two. G128/P32, 16 blocks/1MiB, zero gap, metadata reuse and in-flight reuse are
enabled. There is no Foyer disk cache, no Range change, no decode migration, and
Locality sort buffering is disabled. InMem immediate and constant 2ms backends
isolate mechanisms; the latter is a backend-delay control, not a batching timer
or a bandwidth model. Neither these unoptimized timings nor the chosen cache
sizes are calibrated estimates of production Q18 performance.

Both arms run the same binary. The candidate is the archived Locality-only
planned-range prototype; it does not change Foyer ownership. Baseline leaves its
environment flags unset. A 64MiB cache is a pressure experiment, not a claim
about SF10's actual memory allocation. The 256MiB control fits this working set.
The first set ran 64 groups (eight scenarios, two arms, four rounds); round zero
is warmup and round three is conservatively excluded because an auxiliary build
overlapped its tail. The 256MiB set ran 32 groups (four scenarios, two arms, four
rounds), excluding round zero. Thus 56 groups are included in comparisons, with
24 warmup groups and 16 excluded groups. No compilation ran during the included
rounds. All 96 groups passed result and lookup-count assertions. Each group has
exactly 65,536 right-side lookups and 16,384 Join-cache misses: repeated-key cache
reuse is exercised, with actual prefix reads on misses.

### Working-set-fits result

Values below are medians of three groups with default 2048 exchange permits.
Byte changes compare each arm's median; the full per-round values remain in the
JSON. GET and bytes are sampled before the completion barrier, not during seed
creation. End-to-end means this fragment's scan-to-final-Join-output interval.

| Join | Backend | Physical GET current → plan | Fragment ms current → plan | First output ms current → plan | Read bytes change |
| --- | --- | ---: | ---: | ---: | ---: |
| LeftSemi | Immediate | 201 → 191 | 693.6 → 646.6 | 15.19 → 13.03 | +0.144% |
| LeftSemi | 2ms | 200 → 191 | 734.0 → 640.5 | 21.72 → 20.48 | +0.288% |
| Inner | Immediate | 484 → 467 | 915.0 → 955.8 | 11.50 → 15.64 | +0.103% |
| Inner | 2ms | 472 → 466 | 1083.7 → 1087.7 | 17.62 → 17.42 | +0.103% |

Foyer outer-fetch counts are identical between arms in all fit-cache groups:
701 for Semi, 977 for Inner. The extra physical bytes therefore do not reflect
more Foyer outer-fetch invocations. Per group, the candidate adds 65,263 to
195,797 bytes for Semi, and 65,263 to 130,535 for Inner. This is consistent with
the known planned-range overlap risk, but this experiment does not map each
extra physical GET to a vnode boundary. Do not claim a proven per-GET attribution.

The small-case trace/metric check also shows why pure-scan results overpredict
mixed-chain benefits: 4,096 unique Join keys plus Locality scanning produced
4,114 Blockwise windows totaling 4,287 blocks. Many prefix windows are very small.
Real scan chunk production, serial per-key probe consumption and backpressure
must remain in the model; do not treat every request as a full 16-block window.

### Cache pressure and interpretation

At 64MiB, Semi outer fetches remain 701, but Inner varies. With default exchange
and immediate backend, the two included baseline Inner groups have 1,442/1,413
outer fetches and 928/909 GETs; the plan has 1,562/1,483 outer fetches and
1,034/957 GETs. At 256MiB the same shape settles to 977 outer fetches for both
arms. This capacity control establishes that the pressure-case comparison mixes
re-fetch/cache residency effects with batching. It is not a clean plug-only
measurement, nor evidence of those precise eviction rates in SF10.

Latency varies substantially across runs; three groups do not establish a tail
latency guarantee. Semi's medians improve locally; Inner does not show a
consistent end-to-end improvement. Fewer GETs alone do not justify retaining
this candidate as a transparent plug fix. The candidate is archived and execution
code restored; the ignored actual-operator fixture remains. There is no newly
established baseline correctness bug or claim that all SF10 regression is caused
by plug closing. The result supports evaluating explicit read-ahead as a policy
with byte/latency tradeoffs, rather than assuming it is lossless merging.

Evidence: `locality-chain-results.json`, `locality-chain-samples.json.gz`,
`locality-chain-build-identities.json` and the unapplied
`locality-chain-prototype.patch` in the evidence directory. The standalone patch
was checked against the corresponding files at `c69c12b`. On a separate clean
checkout, apply it, build `cargo test -p risingwave_stream --features test --lib
--no-run`, then run the emitted test binary with `locality_join_read_pattern
--ignored --nocapture --test-threads=1`. Set `RW_CHAIN_KEYS=16384`,
`RW_CHAIN_JOIN=semi|inner`, `RW_CHAIN_DELAY_US=0|2000`,
`RW_CHAIN_PERMITS=2048|512` and `RW_CHAIN_CACHE_MB=64|256`.
Baseline unsets `RW_LOCALITY_SCOPED_PLAN` and `RW_LOCALITY_INCLUDE_RESUMED`;
candidate sets both to 1. Rotate pair order; keep warmup and concurrent-build
rounds out of comparisons. Without the candidate patch, the retained fixture
exercises only the unchanged scheduler.


## Q18-shaped uniqueness/multiplicity correction (2026-11-17)

This follow-up supersedes the generic test as the main operator-pattern result.
The repository TPCH schema has orders keyed by orderkey and lineitem keyed by
(orderkey, linenumber); Q18's aggregate/IN side is unique on orderkey. The test
now represents these constraints separately. It still does not generate or
verify the exact historical SF10 physical plan/catalogs, payload distribution,
SST overlap/version history, 53-actor concurrency or full-query barrier flow.

- Semi: 16,384 keys, four left rows/key with composite stream key, one right
  row/key with stream/state key containing only the join key. The right-side
  deduplicated non-join PK list is empty. Each run has 65,536 Join lookups,
  16,384 Join-cache misses and 65,536 correct output rows.
- Inner: 65,536 keys, one left row/key with a unique join key, three right
  rows/key with composite key. Each run has 65,536 lookups and 65,536 Join-cache
  misses, producing 196,608 correct output rows. This replaces the misleading
  75% Join-cache-hit rate of the first generic Inner test.

Both have 65,536 Locality rows, real multi-vnode executor traversal and multiple
prefetch windows, a 256MiB block cache that fits the generated working set, and
default 256-row chunks/2048 exchange permits. Other settings match the previous
section. Thirty-two groups run on one binary: four scenarios, two arms, one
warmup and three measured rounds, rotating pair order. No build overlaps these
runs. All result/lookup assertions pass, SST manifests match across arms/runs,
and measured-end scheduler pending/waiting/inflight gauges are zero.

| Join | Backend | Median GET current → plan | Fragment ms current → plan | First output ms current → plan | Median bytes change |
| --- | --- | ---: | ---: | ---: | ---: |
| LeftSemi | Immediate | 201 → 189 | 638.6 → 696.3 | 14.51 → 15.58 | +0.144% |
| LeftSemi | 2ms | 207 → 188 | 742.8 → 723.1 | 21.50 → 21.94 | +0.288% |
| Inner | Immediate | 1740 → 1718 | 3141.4 → 3116.8 | 17.41 → 17.93 | +0.045% |
| Inner | 2ms | 1729 → 1718 | 3095.0 → 3177.8 | 28.62 → 27.86 | +0.045% |

Every measured Semi group has 699 Foyer outer fetches in both arms; every Inner
group has 2,222. Bytes added by the candidate are physical read amplification,
not additional outer-fetch invocations. The test does not locate every extra
byte at a specific boundary. GET reductions are real here; stable total/first
output latency improvements are not established. The three-pair unoptimized
local timings are not a production tail-latency guarantee. The plan remains an
archived read-ahead policy experiment, not retained production scheduling.

Use `locality-chain-shaped-results.json`, `locality-chain-shaped-samples.json.gz`,
`locality-chain-shaped-source-sha256.json` and
`locality-chain-shaped-prototype.patch`. The standalone patch was checked against
`c69c12b`. Build/run as above, with `RW_CHAIN_CACHE_MB=256`,
`RW_CHAIN_PERMITS=2048`, `RW_CHAIN_KEYS=16384` for Semi or `65536` for Inner.
The live ignored fixture now uses these per-side constraints. The generic patch
and results remain explicitly separate historical pressure experiments.

### Follow-up design boundary: unused fetch lifetime is not a completion signal

Reading the pinned Foyer `02d6b5e` source rules out a tempting no-timer counter:
`HybridCache::get_or_fetch` eagerly constructs the external future, then passes
its builder to memory-cache registration. `InflightManager::enqueue` may retain
a follower's required-fetch builder in `inflight.f` as a fallback instead of
dropping it immediately. The leader can already hold its own required builder.
Thus a shared request's captured Drop guard need not run until the existing
fetch resolves. A plug that waits for all candidate guards to resolve before
releasing its own reads can wait on a shared request whose owner is another
waiting plug; interleaved windows can form a cycle. Memory hits and some other
followers may drop immediately, but that is not sufficient for a universal rule.

This is a reason to reject that prospective design, not a newly observed
baseline deadlock. No such counter or Foyer patch was added. Current
opportunistic merging does not require a complete candidate inventory. Before
changing dispatch again, use existing window/stage/late/physical-dispatch trace
relationships to classify actual SF10 reads: same-window split opportunities,
serialized prefix dependencies, different objects and configured limits. Late
arrival counts alone do not measure missed physical merging. This experiment
does not provide evidence for a new simple production dispatch fix.

Final cleanup validation: restored execution files byte-match their pre-prototype
snapshots; the test-only range helper is removed. The stream test binary rebuilt
successfully. Three Locality tests, 26 HashJoin tests and both corrected-shape
restored-scheduler smoke tests passed (31 checks). `git diff --check` passed.
The only new executor source hook is an ignored test module. No commit/push was
performed in this experiment.


## Follow-up: window pipelining versus registration scope (2026-11-17)

Two isolated test-feature prototypes were evaluated against their own same-binary
baseline. Both used the corrected real Locality -> local permit channel ->
HashJoin fixture described above: 4+4 actors, 16 vnodes, streaming/cache/IO
workers 8/8/2, memory-only Foyer, 256 MiB cache, G128/P32, N16/1MiB,
zero gap, 2048 exchange permits and 256-row chunks. Each arm/case has one
excluded warmup and three measured runs, with alternating execution order.
There are 32 runs per prototype (24 measured). Backend delay 2ms is a fixture
IO delay, not a batching timer. These are macOS debug local measurements,
not a repeated SF10/MinIO run or statistical proof of a production speedup.
Compilation and performance measurement did not overlap.

### Entering the last block to launch the next window

This Locality-only, no-disk prototype starts the next complete bounded Foyer
window when the last retained block of the current window is entered. It
retains the current block plus at most one next window. Every block still
uses Foyer get-or-fetch; there is no planned physical-range read or decode
relocation. It is distinct from the previous half-window rolling experiment.

| Chain / backend delay | GET median baseline -> early | Total ms baseline -> early |
| --- | ---: | ---: |
| Semi / 0 | 206 -> 210 | 357.924 -> 360.592 |
| Semi / 2ms | 205 -> 199 | 461.628 -> 466.098 |
| Inner / 0 | 1738 -> 1741 | 1163.693 -> 1163.508 |
| Inner / 2ms | 1727 -> 1734 | 2641.479 -> 2638.804 |

Full-chain physical bytes were exactly equal within each pair, as were outer
fetches: 699 for Semi and 2222 for Inner. The real exchange send-wait samples
show substantial downstream backpressure; their sums across concurrent actors
are overlapping wall time, not additive critical-path CPU or IO attribution.
There is no stable chain speedup. A targeted storage-path stop after entering
block 15, with a logical range of 32 blocks, reads 64,682 bytes in the baseline
versus 129,493 in the early prototype. Speculative IO is settled after the
measured scan before accounting. The stop-case per-row boundary checks are
for byte/cancellation diagnosis, not latency comparison. This prototype is
rejected and its two execution files are restored exactly.

The storage control matrix covers 13 scenarios x two delays x 30 measured
trials per arm (1560 samples excluding warmups): cold16, cold32, single cold,
warm neighbors/head/gaps, cold8, independent point, mixed/shared/racing/existing
point and stop_last. It verifies values, outer fetch counts and registered
counts. A full first window or point does not exercise the new early-refill
branch; apparent differences there must not be presented as its causal benefit.

### Hold collection until bounded RW registration finishes

The second prototype adds a test-only registration-open flag to ReadPlug.
Before the owning handle drops, actual reads are collected without scheduling
the one-yield flush (capacity still flushes immediately). On drop, existing
reads schedule the normal one-yield flush on the IO runtime; if none have
arrived, the first later outer read schedules it. Foyer callbacks and unused
fallback futures do not own this registration flag. This affects Blockwise
plugs, including Join prefix windows, not just Locality windows.

| Chain / backend delay | GET median baseline -> registration | Total ms baseline -> registration |
| --- | ---: | ---: |
| Semi / 0 | 213 -> 194 | 358.673 -> 358.925 |
| Semi / 2ms | 205 -> 199 | 463.895 -> 461.083 |
| Inner / 0 | 1746 -> 1733 | 1177.701 -> 1171.099 |
| Inner / 2ms | 1732 -> 1723 | 2634.386 -> 2599.802 |

Bytes and outer fetch counts remain exactly equal within every pair. For
Semi, submitted batch members increase 480 -> 602 (0 delay) and 456 -> 607
(2ms), substantially more than the physical GET reduction. Late reads can
already merge in the pending scheduler queue. One zero-delay Semi pair worsens
GETs 198 -> 204; the three-run median is not proof of a uniform 9% saving.
End-to-end changes range from +0.07% to -1.31%, too small to claim a reliable
production speedup from these samples.

The storage controls use A/B/B/A order, 15 measured trials per process per
scenario/delay, for another 1560 measured samples. Cold16 median GETs remain
1 -> 1 in both delay cases; cold32 improves 5 -> 2 at zero delay and remains
2 -> 2 at 2ms. Independent point zero-delay median/p95 is 54.980/61.333us ->
55.208/60.917us; shared point is 119.730/158.167us -> 104.167/134.000us.
At 2ms, independent point medians are 4032.249 -> 4105.791us and shared point
3990.562 -> 3998.313us. Single-cold zero-delay median varies 308.729 -> 348.250us
with p95 falling 740.250 -> 578.875us; that path creates no plug and is unchanged,
so this noisy control does not establish a causal latency regression or a
universal non-regression guarantee. Stop_last bytes remain 64,682 -> 64,682.

An existing scheduler progress test rejects generalizing handle lifetime into
a registration barrier: with the prototype enabled,
`collecting_plug_does_not_reserve_point_read_capacity` times out in 5 seconds.
The independent point finishes, but the test holds the plug while awaiting its
prefetch result, so the new gate cannot close. This is a prototype regression,
not a newly found baseline bug. The experiment is archived and the production
batch implementation restored byte-for-byte to its pre-experiment snapshot.

A cleaner future design, if this small merge saving merits implementation,
is a private synchronous RW `register_window` helper that registers the head
and neighbors, closes a non-escaping registration guard, and returns Foyer
responses. Only after returning may the caller await the head. Its Fill-only
registration path must be synchronous by construction, with non-Fill/failpoint
bypass outside the guard. Tokens captured by Foyer must not capture that guard.
Generic ReadPlug handle lifetime remains unrelated to flushing. This needs no
Foyer API or disk behavior changes, no extra read-ahead window, no timer and no
expected-candidate completion counter. It still cannot guarantee a complete
batch when Foyer producers arrive after registration; missing/shared candidates
must never block dispatch. This scoped production implementation is a design,
not implemented or validated by the above general-handle prototype.

Evidence: `locality-pipeline-*` and `locality-registration-*` in
`read-optimization-evidence/` contain paired results, summaries, driver scripts,
prototype patches and compressed raw logs. The pipeline driver's historical
arm name `plan` denotes early next-window registration here, not the previous
planned-range prototype. Only the stop-at-window-boundary ignored test addition
is retained from these execution experiments; Foyer, Range and decode are unchanged.

Final verification after restoring both prototypes: storage and object-store test
binaries rebuilt successfully. Scheduler 24, forward SST iterator 13, SST store
11, backfill routing 1 and the retained stop-boundary diagnostic 1 passed
(50 checks total). The earlier held-handle timeout passes again in the restored
scheduler suite. `git diff --check` passed. No Foyer, Range, decode, production
scheduling change, commit or push is retained from this follow-up.


## Operator review and capability ablation (2026-11-17)

The proposed private synchronous registration scope has NOT been implemented.
Before further scheduler work, this follow-up reviews where independent reads
actually exist and disables selected capabilities in the real executor fixture.
No operator execution ordering, Foyer behavior, Range path or decode changes
are proposed by this experiment.

### Call-site findings

- `executor/hash_join.rs:994` builds all input keys before the per-row loop at
  996. It consumes `handle_match_rows` for a row before advancing; on a miss,
  1186 fetches the matching prefix. The handler updates cached state, cleans
  matched rows and applies insert/delete/degree effects before returning.
  This is serial execution in the current implementation, NOT proof that the
  next key's address inherently depends on the preceding IO result. Distinct
  keys can be known in advance; changing execution/prefetch across keys is an
  operator-level design and cannot be achieved by holding an IO plug longer.
- `executor/join/hash_join.rs:237` optionally fetches degrees before the row
  stream. `into_stream:292` uses `iter_with_prefix_respecting_watermark` with
  Blockwise prefix prefetch. A unique/short prefix frequently has just one
  block per SST. Large-fanout keys can span many blocks and must not be ruled
  out by the HashJoin operator label alone.
- A prefix seek may initialize several SST children concurrently through
  `merge_inner.rs:260`. Those are often different object paths, so their
  concurrency does not imply byte-range coalescing. Same-block cache sharing
  belongs to Foyer; it is not additional physical-range merging by a plug.
- `locality_provider.rs:338` scans each vnode's remaining range through
  `FlushedStateTableReader`, with Locality prefetch requested at 374. This
  exposes multiple independent same-SST blocks. It also has chunk/channel
  backpressure. Only buffered upstream updates cause snapshot replacement at
  995; an ordinary barrier alone does not necessarily abandon the iterator.
- `aggregate/hash_agg.rs:308` restores independent missing group states through
  `get_rows` when native point batching is enabled; fallback work uses bounded
  concurrency at 356. `mview/cache.rs:390,418` similarly batches missing keys
  or uses concurrency 10. The native `local_hummock_storage.rs:351` batch
  advances keys independently with bounded concurrency and shared SST metadata;
  each key still preserves version/candidate visibility. This creates potential
  candidates, but random nonadjacent ranges do not become mergeable at gap=0.
- TopN's bounded cache fills use disabled prefetch; unbounded fills select the
  existing Range mode (`top_n_state.rs:148,211`). Aggregate materialized-input
  fills similarly use `PrefetchOptions::new` (`aggregate/minput.rs:224`). These
  are not new Blockwise plug consumers and should remain outside this change.
- `forward_sstable_iterator.rs:159` clips the scan upper bound, and at 464 a
  single-block window returns before creating a plug. At 505 all memory-ready
  neighbors also avoid plugging the demand. This existing behavior already
  covers the common single-candidate case; no new operator-specific filter is
  required to avoid waiting for a nonexistent second block.

### Experiment

The corrected Semi and Inner fixtures, runtime topology and read limits are
unchanged from the previous section. Same-binary arms use a rotating order:

1. baseline: current Blockwise windows, plug and scheduler queue coalescing;
2. no_prefix: disable only ordinary/prefix Blockwise via the existing config;
3. no_locality: a test-only mode override maps Locality to Disabled, explicitly
   NOT to its legacy Range fallback;
4. no_plug: return no plug while keeping Foyer window registrations, scheduler
   G/P limits, dedicated IO runtime, queue coalescing and in-flight reuse.

Two join shapes x two backend delays x four arms x four rounds = 64 successful
runs. Round zero is warmup, leaving three measured rounds per case/arm. Outputs,
Join lookup/miss counts, identical seed SST manifests, equal physical bytes and
equal outer fetch counts were verified. All arms have 699 outer reads and
45,248,719 physical bytes in Semi; 2222 outer reads and 144,900,547 bytes in Inner.
No build overlapped a benchmark. Later baseline wall times varied markedly
(e.g. zero-delay Semi about 0.4s then 0.79s), and some comparisons are noisy;
all samples are retained. This run does NOT establish latency/speedup or prove
absence of latency regressions. GET/byte/window evidence is the primary result.

| Join / backend delay | Baseline GET | No prefix prefetch | No Locality prefetch | No plug |
| --- | ---: | ---: | ---: | ---: |
| Semi / 0 | 211 | 213 | 699 | 229 |
| Semi / 2ms | 199 | 205 | 699 | 211 |
| Inner / 0 | 1750 | 1736 | 2222 | 1744 |
| Inner / 2ms | 1723 | 1727 | 2222 | 1735 |

These are three-run medians. The no-plug arm is NOT a no-scheduler arm.
It still permits normal queued requests to coalesce. Current plugging has
some marginal GET benefit in Semi, whereas exposing Locality read-ahead
candidates produces the much larger reduction. Disabling prefix prefetch has
little observable GET effect in these short-prefix shapes; this does not
justify globally disabling it for high-fanout joins.

The isolated window histograms are stable across all measured rounds and delays:

| Path | Windows | Selected block slots | One-block windows | Mean selected blocks |
| --- | ---: | ---: | ---: | ---: |
| Semi Join prefixes | 16384 | 16520 | 16248 (99.17%) | 1.0083 |
| Inner Join prefixes | 65554 | 67196 | 63912 (97.50%) | 1.0250 |
| Locality feeding Semi | 51 | 575 | 3 | 11.2745 |
| Locality feeding Inner | 49 | 568 | 0 | 11.5918 |

These are per-SST selection windows, BEFORE cache arbitration, not physical
GETs or an assertion that every selected block misses. Prefix windows in
these fixtures have at most two slots. Cache hits/shared producers further
reduce actual external candidates. The histograms explain why improving a
registration boundary cannot improve most individual prefix windows.

### Decision

Do not implement the more elaborate registration-scope candidate as the next
production change. Retain bounded Locality read-ahead and simple best-effort
queue merging. Avoid waiting for the next serial probe, and do not add a
universal cross-key batch, a timer, a completion counter or Foyer-specific role
API. The whole feature is not pointless: removing Locality read-ahead raises
GETs significantly at identical bytes. The narrower negative conclusion is
that extra batching effort on these short serial Join prefixes has little
opportunity. The existing simple plug itself is not proven useless.

For short unique-key probes, a separate future review of iterator setup/SST
candidate work (or a correctly guarded point-lookup path) is more relevant than
longer IO waits. A direct `get_row` replacement is NOT validated: matching-PK
layout, watermark filtering, degree handling, cache updates and insert/delete
semantics must be preserved. No such replacement is made here. The actual
SF10 regression still needs same-window physical attribution; these synthetic
SSTs do not reproduce its layered history or full 53-actor graph.

All three temporary ablation source edits are restored exactly. Evidence is
archived as `operator-ablation-*` (patch, run script, full results, window
summary and compressed logs). No commit or push is performed.

Restored-code validation: the stream test binary rebuilt successfully; three
Locality tests, 26 HashJoin tests and both restored chain smoke tests passed
(31 checks). All ablated source/fixture files byte-match their pre-experiment
snapshots, and `git diff --check` passed. The synchronous registration-scope
design remains unimplemented; only review/experiment records are retained.


## Scope cleanup: Range baseline and independent scheduler value (2026-11-17)

The operator ablation above compared Blockwise with Disabled, NOT with the
existing Range implementation. It proves the value of lookahead in those
fixtures; it does not prove that splitting a known contiguous range into
Foyer requests and merging them again outperforms Range prefetch. The SF10
clean-main / previous-all-on / simplified-all-on cohorts changed several
features and advertised different slot counts, so they do not isolate that
comparison either.

`SstableStore::prefetch_blocks` chooses a bounded contiguous span with cache
checks/heuristics, issues one object read, decodes the selected blocks and
retains holders. Blockwise registers separate per-block Foyer responses and
leaves physical merging to the scheduler. Its cache arbitration/sharing can
be useful for mixed hit/shared-fetch workloads; it is not a demonstrated
universal sequential-scan improvement. Both paths retain lookahead through
holders/responses. The misleading PrefetchOptions comment contrasting Range
retention with supposedly cache-only Blockwise retention is corrected.

Scheduler duties should be evaluated separately: admitted/physical concurrency
and runtime placement can be useful without range coalescing. The current
implementation does not provide complete global IO limits: read_with_plug
routes oversized/unbounded reads directly, and can_schedule uses max_read_bytes
both as a scheduling eligibility bound and a merge bound. Waiting callers and
staged plug reads are outside the G admission budget. Do not describe this as
a full global memory bound, bandwidth/fairness scheduler, or isolated IO path
for all object reads. These are scope limitations, not newly fixed behavior.

A decisive future comparison should keep the branch, input SSTs, cache and
runtime settings controlled and distinguish:
- Range + direct reads;
- Range + admission/runtime dispatch with physical merging and inflight reuse
  disabled (the current scheduler can screen this with max_batch_requests=1
  and reuse_inflight_reads=false, though it still pays the queue machinery);
- Range + the current scheduler;
- Blockwise + the current scheduler.

Check oversized/direct-route coverage, bytes, GETs, end-to-end and point-read
tails, CPU and retained memory. A win from dispatch alone would justify a
smaller dispatch implementation, not automatically the current merge machinery.
No new four-arm Range benchmark is claimed here and no default policy is changed.

Behavior-preserving cleanup: remove the temporary pointer-ID stage/flush/late
DEBUG events added for the plug investigation; restore the simple Option
propagation in stage(). The diagnostic patch is archived as
`read-optimization-evidence/plug-diagnostic-tracing.patch` for reproducing old
window-correlated traces. Existing production tracing, ignored experiments,
regression tests and their evidence are retained. No Foyer, Range algorithm,
decode, scheduling policy, commit or push change is made in this cleanup.

Cleanup validation: storage/object-store test binaries rebuilt successfully. Scheduler 24, forward iterator 13, SST store 11 and routing 1 passed (49 checks); git diff --check passed.


### 2026-11-17: Range versus Blockwise, dispatch, and full scheduler

This comparison separates Locality prefetch strategy from scheduler behavior.
The starting HEAD was `c69c12bb910bfab52d9053b7a19931572e3b546c`, with the existing
ignored operator/storage fixtures and working-tree changes retained.
No Foyer, Range algorithm, decode, production scheduling, or default configuration
changes were retained from this experiment.

Arms: A = Locality Range + direct reads; B = Locality Range + the existing scheduler
with `max_batch_requests=1` and `reuse_inflight_reads=false`; C = Locality Range +
full scheduler; D = Locality Blockwise + full scheduler. Ordinary Join prefix
Blockwise and native read-batch/meta reuse were identical across all four arms.
B is a dispatch screening configuration, not an implemented minimal wrapper.

The real LocalityProvider / local permit channel / HashJoin fixture used 4+4 actors,
16 vnodes, 256-row chunks, 2048 exchange permits, a 256 MiB memory-only Foyer cache,
8 initialization/cache workers, 8 streaming workers, and 2 IO workers. Scheduler
settings were G128/P32, N16, 1 MiB, and zero gap. Semi used 16,384 keys with four
input rows and one right row per key; Inner used 65,536 keys with one input row and
three right rows per key. Deterministic SST manifests matched across arms. Output
and Join lookup/miss assertions passed. This is not a complete Q18/SF10 deployment.

There were 64 chain runs: four arms, two Join variants, two backend delays (0 and
2000 us), and one warmup plus three measured rounds in rotating order. All requests
in B/C/D used the scheduled route; no oversized/unbounded bypass was observed.
Final scheduler and physical-read gauges were zero. GET medians:

| Backend delay | Join | A Range/direct | B Range/dispatch | C Range/full | D Blockwise/full |
| --- | --- | ---: | ---: | ---: | ---: |
| 0 us | Semi | 188 | 188 | 188 | 215 |
| 2000 us | Semi | 188 | 188 | 188 | 199 |
| 0 us | Inner | 1718 | 1718 | 1718 | 1726 |
| 2000 us | Inner | 1718 | 1718 | 1718 | 1734 |

C eliminated just one additional GET in one of its 12 measured chain runs. Range
read roughly 0.05–0.43% more bytes than D. Host timing drift was substantial; for
example A Semi/0 varied from 369 to 934 ms and A Inner/0 from 1516 to 4558 ms. Keep
all samples; these timings do not establish a throughput winner.

The main storage-path comparison collected 3840 measured samples (12 scenarios,
two delays, four arms, 40 samples each), plus 640 mechanism-control samples.
Every process/scenario also ran five warmups. Mixed point reads started after the
first real iterator-initialization poll rather than a Blockwise-specific counter.

- All-cold 16 blocks: Range consistently used one GET. D had a median of one but
  maxima of four (0 us) and two (2000 us). In the extra controls, Blockwise direct
  and dispatch each used 16 GETs; full scheduling reduced this to a median of one.
- Alternating warm/cold blocks: all arms used eight GETs and 32,222 bytes. At 2000 us,
  Range took about 37–38 ms while D took 8.45 ms. Extra Blockwise direct/dispatch/full
  controls took 8.16/8.54/8.22 ms: this is concurrent-prefetch benefit, not merging.
- A point read already owning the middle block: Range used two GETs/68,734 bytes;
  D typically used three GETs/64,682 bytes. Sharing the middle block leaves two
  separated cold spans under zero-gap merging. Fewer bytes need not mean fewer GETs.
- A point read started with the scan: Range/full sometimes reduced two GETs to one.
  Added event counters attributed the observed savings to pre-dispatch overlapping
  merges (`merged_requests`), not active reuse (`inflight_reused` stayed zero).
  Do not cite this experiment as evidence of inflight-reuse benefit.
- Standalone point p50 at 0 us was about 33/58/63/58 us for A/B/C/D. At 2000 us it
  was 3.71/3.78/3.86/3.84 ms. No point-latency advantage was demonstrated. These are
  local debug-build samples, not a production tail-latency guarantee.
- Stopping at the first row of block 16 in a 32-block logical scan read 64,682 bytes
  in every arm; the next window was not prefetched speculatively.

Whole-process CPU and raw maximum RSS include SST generation/setup. End cache usage
was identical for Semi and near capacity for Inner; the Range tracker ended at zero.
These are not isolated IO CPU measurements or peak lookahead-retention measurements.

Interpretation: use existing Range as the continuous Locality baseline. Blockwise
can still be useful for per-block sharing and concurrent sparse misses, independently
of physical merging. The scheduler can merge existing compatible requests, but this
workload does not justify more plug waiting or establish an independent benefit for
admission/runtime isolation under backend overload. Do not replace serial Join probe
execution with an assumed concurrent workload. No automatic strategy selector was
implemented and no default was changed.

Evidence uses the `read-optimization-evidence/range-dispatch-*` prefix: `results.json`
and `summary.json` for the chain, `micro-results.json`/`micro-summary.json` for storage,
`control-results.json`/`control-summary.json` for attribution, and `logs.json.gz` for
raw output. Scripts, binary identities, source hashes, a baseline chain fixture, and
ordered experiment patches are archived. Temporary fixture switches were restored
from their pre-run copies after testing. Validation results are recorded separately.

Cleanup validation: rebuilt storage and stream test binaries. Iterator 13, SST store 11,
routing 1, Locality 3, HashJoin 26, and two small chain smoke tests passed (56 total).
`git diff --check` passed. Fixture backups matched byte-for-byte after restoration;
production SST store/iterator/scheduler and Cargo.lock source hashes were unchanged.


### 2026-11-17: cold-run prefetch prototype (memory and disk cache)

Retained an opt-in `PrefetchMode::ColdRanges` implementation in
`hummock/sstable_store/cold_range.rs`, integrated with the SST iterator. Existing
Locality/default routing, Range, Blockwise, Foyer dependencies, and scheduler logic
are unchanged. Temporary operator-routing overrides are archived, not retained.

The iterator plans one bounded window (existing block/byte limits and scan/table
bounds). Actual memory hits are pinned. For hybrid caches, a positive `contains`
hint starts a cache-only lookup and terminates the preceding cold run; a negative
hint extends the cold run. Each closed run starts an object range read immediately,
without waiting for any preceding run or iterator consumption. Disk lookups do not
form a window-wide await barrier. A stale positive hint falls back to a demand get.
Direct runs intentionally do not provide Foyer per-block single-flight. Results are
consumed in order, decoded on the caller, and inserted into the cache. Private range
tasks are aborted on drop/seek; Foyer retains ownership of shared cache producers.
The next window starts only when the current window is exhausted. No timer, new
plug state, Foyer interface, or adaptive policy was introduced.

The outer range task runs on the caller. `object.read()` still routes scheduled
physical reads to the scheduler's dedicated IO runtime when enabled. Direct controls
are explicitly scheduler-OFF. Scheduled controls use the same G128/P32, N16/1 MiB,
zero-gap, 2-worker IO runtime for Range and ColdRanges. Every scheduled-control
request used the scheduled route; no oversized/unbounded bypass occurred in these
fixtures. Existing oversized bypass behavior is not changed by the prototype.

New tests assert two physical reads simultaneously in flight for
`cold,cold,cold,hot,cold,cold,cold`, before any block is consumed, for both memory and
disk hits. They also verify cancellation/reservation release, actual Source::Disk
hits, stale-index fallback after deletion, ordered keys/bytes, and malformed/truncated
prefetch fallback. The first test version accidentally used the default 3-block
window for a 7-block scenario; the fixture now explicitly configures 16 blocks/1 MiB.
The concurrency assertion was not weakened. Cache-only lookups that join an external
fetch count Source::Outer as a miss, matching existing get/Range accounting.

Measured samples: 2880 main micro samples (Range/direct, Blockwise/scheduler,
ColdRanges/direct), 1920 scheduler-ON micro samples, 480 main disk samples and
320 scheduler-ON disk samples; 5600 total, excluding per-scenario warmups. Disk warm
blocks were written only to disk, with memory absence checked. Backend delays were
0/2000 us, injected after routing, not batching timers. Main micro cases have
40 samples per arm/delay/scenario; disk cases have 20.

At 2000 us, with scheduler and dedicated IO runtime enabled:

| Scenario | Range | ColdRanges | Physical reads |
| --- | ---: | ---: | --- |
| 16 all-cold blocks, scan p50 | 5.01 ms | 4.95 ms | both 1 GET / 64,682 bytes |
| Alternating memory hits/misses, scan p50 | 37.93 ms | 8.53 ms | both 8 GETs / 32,222 bytes |
| Alternating disk hits/misses, scan p50 | 38.23 ms | 8.77 ms | both 8 GETs / 32,222 bytes |
| Single cold block, scan p50 | 3.96 ms | 3.91 ms | both 1 GET |
| Unrelated point mixed with scan, point p50 | 3.59 ms | 3.57 ms | both 2 GETs total |
| Memory-hot head, first block p50 | 241 us | 349 us | both 1 cold GET later |
| Disk-hot head, first block p50 | 526 us | 672 us | both 1 cold GET later |

The hot-head latency increase is an observable tradeoff of planning the window before
returning its head; do not advertise universally unchanged latency. Direct shared
point controls use two GETs for both range strategies; scheduler-ON overlapping reads
can reduce the median to one, but per-block sharing is not guaranteed. Stopping at
the first row of block 16 in a 32-block logical scan reads only the first window
(64,682 bytes), with no next-window speculation.

The real Locality/permit-channel/HashJoin comparison used the same fixtures/settings
as the preceding experiment: 48 main runs and 32 scheduler-ON runs (20 warmup,
60 measured). SST manifests, outputs and Join lookup/miss checks matched; all final
queue/reservation/inflight gauges drained. GETs are near Range, not always identical:
0-us Semi GET medians are Range 188 vs ColdRanges 189; 2000-us scheduled Inner medians
are Range 1717 vs ColdRanges 1718. Strict hot-block boundaries may add a request that
Range's heuristic overread avoided. There was substantial host wall-time drift;
these results do not establish end-to-end throughput improvement or a no-regression
guarantee. This is not a full SF10/Q18 deployment. Whole-process CPU/RSS includes setup.

Evidence is archived with the `read-optimization-evidence/cold-range-prefetch-*`
prefix. `implementation.patch` is the retained code change; `experiment.patch`
contains temporary operator/test hooks. `results.json` and `scheduled-results.json`
contain all chain runs, with separate micro/disk result files. Drivers, summaries,
binary hashes, source identities, logs and a human-readable report are included.
The final Source::Outer accounting fix does not change physical requests or routing;
performance samples preceded that statistics-only adjustment.

The prototype is retained for review, not enabled by default. Adopting it should
explicitly replace an existing target path rather than make three strategies the
permanent default design. The acceptance decision must include redundant overlap
reads, extra hot-head work, memory retention, and occasionally more range requests.

Final validation after the Source::Outer accounting fix: storage/stream test builds
passed, followed by 59 checks (iterator 13, SST store 11, cold ranges 2, routing 1,
Locality 3, HashJoin 26, two chain smoke tests, dedicated IO runtime isolation 1).
The existing object-store binary was reused for its unchanged runtime-isolation test.
The old Range function and restored temporary fixtures matched their starting copies;
Foyer/Cargo.lock were unchanged. `git diff --check` passed.

### ColdRanges configuration and fragmentation observability (2026-11-17)

ColdRanges now has a startup configuration entry, without changing the default
selection. Normal deployment needs only the unified mode:

```toml
[storage.blockwise_prefetch]
mode = "cold_ranges" # also: "range", "blockwise", "disabled"
# Optional: reproduce the preceding microbench window instead of the 3-block/256-KiB defaults.
max_blocks = 16
max_bytes = 1048576
```

The section keeps its existing name for configuration compatibility. Optional
`prefix_mode`, `changelog_mode`, `range_scan_mode`, `snapshot_backfill_mode`, and
`locality_backfill_mode` override `mode` for controlled comparisons. For example,
`mode = "range"` plus `locality_backfill_mode = "cold_ranges"` isolates the Locality
strategy. Empty/omitted per-path modes inherit `mode`; an empty/omitted unified
mode preserves the old boolean switches and defaults. Explicit mode selection
does not require also setting a legacy boolean. Ordinary point gets are unchanged.
These settings require a compute-process restart.

Blockwise and ColdRanges share `max_blocks`/`max_bytes`; Range retains its existing
`storage.max_prefetch_block_number` and memory-budget behavior. At this stage,
ColdRanges required cache-fill policy and an eligible multi-block window;
NotFill/Disable support was added in the cache-policy follow-up below. No Foyer or old Range algorithm changes were made. The optional ColdRanges
state is now boxed so disabled iterators retain only one pointer, and its await is
included in existing `SeekAwait::Prefetch` profiling.

The outer private task still runs on the caller runtime. Its `object.read` goes
through the existing object-store routing. To enable scheduled physical reads on
`rw-read-io`, also configure:

```toml
[storage.object_store]
enable_hummock_read_coalescing = true
[storage.object_store.read_scheduler]
use_dedicated_io_runtime = true
```

This runtime guarantee applies to reads accepted by the scheduler; oversized or
unbounded reads retain the existing direct route. Decoding remains on consumption
in the caller runtime. No timer or new scheduling policy was added.

Five new metrics use the `state_store_cold_range_prefetch_` prefix:

| Suffix | Interpretation |
| --- | --- |
| `window_runs` | Histogram of logical cold reads per planned window; zero includes all-hot windows. |
| `read_bytes` | Histogram of each submitted logical cold range's compressed byte length. |
| `gap_bytes` | Histogram of compressed bytes in each internal cache-hit/index-hint gap between two cold runs. Its count is the number of within-window split boundaries caused by such gaps. |
| `gap_blocks` | Histogram of the number of consecutive cache-hit/index-hint blocks per internal gap. |
| `hint_misses` | Counter of positive disk index hints that later produced no cache entry and needed demand fallback. |

Leading/trailing hot blocks and window boundaries do not count as gaps.
Consecutive memory/disk-hinted blocks form one gap, not one gap per block.
For `cold cold cold hot cold cold cold`, planning records two reads and one gap;
for `cold cold hot hot cold cold`, it records two reads and one two-block gap.
Planned/submitted reads remain counted if subsequently cancelled or failed.
Gap index hints are not claimed to be verified disk hits.

Example PromQL (apply the same job/instance filters to all series):

```promql
# Small internal split boundaries per second, here <= 16 KiB.
sum(rate(state_store_cold_range_prefetch_gap_bytes_bucket{le="16384"}[5m]))

# Fraction of internal gaps that are <= 16 KiB; undefined when no gaps occur.
sum(rate(state_store_cold_range_prefetch_gap_bytes_bucket{le="16384"}[5m]))
/
sum(rate(state_store_cold_range_prefetch_gap_bytes_count[5m]))

# Mean submitted cold range size.
sum(rate(state_store_cold_range_prefetch_read_bytes_sum[5m]))
/
sum(rate(state_store_cold_range_prefetch_read_bytes_count[5m]))
```

These metrics reveal small gaps and fragmentation in the iterator plan, not an
exact count of avoidable physical GETs. Scheduler coalescing can join the segments
again; independent windows, limits, cancellation, and cache-hint fallbacks also
affect physical reads. Correlate them with existing object-store logical/physical
counts, `object_store_read_scheduler_group_size`, and scheduler overread bytes.
Metrics are observability only and are never read to make scheduling decisions.

This round verifies configuration, read routing, cache gaps and correctness;
it does not rerun the preceding performance experiment or claim a new latency win.

Validation: common configuration tests passed (15), followed by storage/stream
regressions (59: iterator 13, SST store 11, cold-range 4, legacy routing 1,
explicit routing 1, Locality 3, HashJoin 26). The explicit routing test exercises
32 combinations of unified/per-path selection, four strategies, and four scan
entry points; it verifies the two-block upper bound as well as strategy selection.
Changelog mode resolution is covered by configuration tests and compilation;
this round did not run the standalone changelog integration test. Cold-range
checks cover memory and disk hits, stale disk index fallback, eager submission
of both sides, cancellation, consecutive hot gaps, and window boundaries.
Range/ColdRanges both pass the existing sequential-read/seek correctness test.

The storage and stream test targets compile successfully. Default configuration
and config docs were regenerated by the exact Cargo test used by the RiseDev
recipe: the local wrapper could not run because cargo-make/cargo-binstall are not
installed. The generated-file test then passed without UPDATE_EXPECT. Metrics
inventory generation/check and `git diff --check` passed. The old Range function
matches the pre-prototype copy byte-for-byte, and Cargo.lock/object-store sources
remain unchanged. Existing macOS linker unwind-table and proc-macro future
compatibility warnings are unrelated to these changes.

A mergeable comparison configuration is provided in
[`src/config/cold-range-prefetch.toml`](../../../../src/config/cold-range-prefetch.toml).
It sets the Blockwise/ColdRanges window to 16 blocks/1 MiB, Range's existing block
limit to 16, and the scheduler's accepted range limit to 1 MiB. The last setting
matters: leaving the scheduler at its 256-KiB default would route larger cold
runs directly instead of through the dedicated runtime. `max_gap_bytes = 0`
keeps cache-gap splitting visible without enabling scheduler gap overread.
For the three-arm comparison, change only `mode` between `range`, `blockwise`,
and `cold_ranges`, leave per-path mode overrides unset, restart compute processes,
and repeat the same workload from the same cache state. Keep disk-cache settings
and all other read-optimization settings fixed. This configuration does not itself
clear caches or create a cold workload.

### ColdRanges prefetch memory admission fix (2026-11-17)

Review found that ColdRanges checked the shared counter before a separate
unconditional increment, allowing concurrent windows to exceed the configured
prefetch budget. It also charged only max(compressed, decoded) per block, although
raw range buffers coexist with decoded blocks. The decoded restart-point vector
is a separate allocation not represented by the block's raw byte length.

The fix leaves window selection (`max_blocks`, `max_bytes`), range segmentation,
IO routing, decoding placement, and cache policy unchanged. Before any cache
lookup or private read task is created, ColdRanges now attempts one atomic,
non-waiting reservation against `prefetch_buffer_capacity_mb`. Admission requires
`current + reservation <= capacity` with checked arithmetic. Zero capacity,
insufficient remaining capacity, and arithmetic overflow cannot admit a window.
Failure returns to the existing demand path, without waiting for another iterator
to release its window.

Reservation covers every selected block, including hits and disk hints: compressed
bytes plus a conservative decoded block weight, block keys, both window queue slot
arrays, and the window/tracker structure. The decoded estimate includes the Block
object, payload, hitmap, and a restart-point bound derived from the raw block's
u32-offset encoding. This intentionally overestimates actual restart counts;
metadata does not expose their count before IO. Both queues are preallocated to
the selected window length. The window's byte-selection budget still uses its
existing max(compressed, decoded) definition, so this fix does not silently shorten
otherwise admissible windows.

Reservations remain attached to the whole window and its private IO tasks.
Returning data to Foyer does not release them early. Cancellation requests abort
private reads; their Arc-held reservation is released when the task actually exits,
including abort-before-first-poll. Existing Foyer-owned shared producers are not
cancelled by this mechanism.

This is a bound on estimated ColdRanges reservations, not process RSS. Foyer and
object-store/client internal allocations, allocator rounding, decoder scratch
space, and potentially larger shared scheduler backing buffers are not an exact
part of this estimate. Normal demand reads and the block cache have their own
memory lifecycle. Old Range remains unchanged and still uses legacy soft
admission: its usage is observed by ColdRanges, but concurrent legacy Range
reservations can independently exceed the shared counter's capacity. Do not claim
a strict cross-strategy budget when mixing both implementations.

A separate configuration wiring bug was fixed: `extract_storage_memory_config`,
used by compactor/replay, read `shared_buffer_capacity_mb` as the prefetch budget
and ignored the actual prefetch option. It now reads `prefetch_buffer_capacity_mb`.
CN's separate memory configuration path already read the correct field. Defaults
with neither override set are unchanged.

New metric: `state_store_cold_range_prefetch_budget_denied` counts windows refused
by admission. Under the same global budget, the more conservative charge can
reduce concurrent prefetch windows and increase demand fallbacks; monitor this
counter rather than attributing those requests to cache-gap segmentation.

Validation: storage test target compiled; 47 targeted tests passed (block stream 4,
block decoding 7, cold ranges 8, SST store 11, iterator 13, routing 2, options 2).
All 15 common configuration tests also passed, including generated config/docs.
New cases cover exact/insufficient/zero capacity, integer overflow, concurrent
reservations held across threads, eight actual scans competing for two windows,
zero-budget iterator seeks through demand reads, cancellation before first poll,
and compressed/uncompressed block estimates with different restart intervals.
With exactly enough reservation, 16 fully cold blocks still produce one logical
range and one physical GET. Existing hot-gap concurrency, disk-hit/stale-hint,
corrupt/truncated-read fallback, seek, and cancellation tests passed.

No Foyer, object-store scheduler, old Range implementation, or decode scheduling
changes were made. `git diff --check` and metrics inventory validation passed.
This round does not claim an end-to-end latency or RSS benchmark result.

### ColdRanges cache-policy completion (2026-11-17)

ColdRanges now accepts Fill, NotFill, and Disable using the same bounded window,
atomic memory admission, eager private IO submission, and caller-side decode.
Fill retains the existing cache insertion and priority behavior. NotFill reuses
memory/disk hits but holds remotely fetched blocks as owned values without
inserting them into Foyer. Stale disk hints fall back through the ordinary NotFill
get path and do not refill the cache. Disable bypasses all memory/index/cache-get
lookups and reads the bounded window as one contiguous range, retaining owned
blocks. The existing disable_block_cache failpoint still bypasses prefetch and
uses ordinary demand reads, preserving its prior behavior.

Storage test compilation passed with no new warnings. All 49 targeted tests passed:
block stream 4, block decoding 7, cold ranges 10, SST store 11, iterator 13,
routing 2, and options 2. Cases cover memory/disk hot gaps for all three policies,
owned-vs-cache entry identity, Fill priorities, absence of new cache entries for
NotFill/Disable, no disk-hit promotion under Disable, stale-hint NotFill fallback,
all-hot reads after object deletion, and pre-consumption IO submission/cancellation.
For a seven-block window with block 3 hot, Fill/NotFill read two cold segments
(six blocks), while Disable reads one segment (all seven). For 16 fully cold
blocks, all three policies still produce one physical GET when budget permits.
Zero/insufficient budget, full scan and repeated seek, scan upper bounds, truncated
reads and corrupt speculative neighbors are also covered for the new policies.
No scheduler, Foyer, old Range algorithm, or decode runtime changes were made.
No performance or end-to-end Q18 claims are made by these correctness tests.

Follow-up code review against the user's Q18 observations confirmed three remaining
issues, not fixed by cache-policy completion:

- Singleton cold runs created by splitting a larger window still use direct range
  reads. Only an originally singleton window takes the ordinary demand path. Under
  Fill this bypasses Foyer fetch ownership; multi-block direct reads likewise do not
  participate in per-block single-flight. Scheduler in-flight reuse requires full
  coverage and cannot attach a later larger range to a smaller active read.
- ColdRanges seek boxes its refill/consume future before ruling out a singleton
  scan bound. Short prefix initialization can pay an unnecessary allocation.
- Consuming a cold run decodes and (under Fill) inserts the entire run before
  returning its first block. This is the retained old-Range-style behavior; its
  Q18 latency contribution has not been isolated.

The user's table-38 figures (42,462 cold segments representing 42,463 blocks) make
post-split singleton handling a priority. Segment counts are logical submissions,
not proof that every segment caused a separate or duplicate physical GET. Any fix
must preserve eager submission of later runs and respect NotFill/Disable rather
than unconditionally routing them through Foyer get_or_fetch.

### ColdRanges singleton sharing and demand decode (2026-11-18)

This supersedes the singleton and eager-decode behavior described in the previous
review. No Foyer dependency, scheduler implementation, old Range implementation,
or runtime assignment changes were made.

After cache-gap splitting, singleton **Fill** segments now register an ordinary
`get_block_response` while constructing the window. Its Fill branch registers
Foyer ownership without awaiting IO, so all cold segments still start before any
row consumption. Consuming a singleton waits for that shared response; dropping
an iterator does not abort a producer needed by a point-read waiter. NotFill and
Disable retain their non-filling direct reads. Multi-block direct reads still do
not claim per-block ownership: an existing A request cannot cover a later direct
A+B range. This change fixes singleton sharing, not general overlap deduplication.

Before boxing the ColdRanges future, the iterator releases an exhausted window
and checks whether at least two blocks fit the scan bound, block limit and byte
limit. A nonempty existing window still supplies its final block even when only
one block remains before the bound.

Multi-block reads now retain one raw `Bytes` buffer plus a block cursor. Each
`next` decodes/copies only its current block on the caller runtime. Fill inserts
that consumed block with the existing priority rule; NotFill/Disable return an
owned block. Unconsumed blocks are not decoded or inserted by the direct-read
path. Singleton shared producers still decode/fill normally in Foyer. All direct
IO is still submitted eagerly; this is incremental block delivery after a whole
range read, not streaming an unfinished HTTP response. Corruption in a later
block is reported when that block is consumed, without refetching a valid head.

The raw run can live longer, and later blocks become visible in Foyer later.
Consequently this does not improve sharing with intervening point reads of an
unconsumed block, and may widen that existing direct-range sharing gap. Keep the
conservative whole-window reservation until the window is dropped, including
while retaining its raw bytes. Private pending reads retain their reservation
through cancellation. Foyer-owned shared producers have ordinary cache-fetch
lifetime rather than iterator-owned cancellation. No claim of a global RSS bound
or additional total-decode throughput is made.

Metrics now separate `state_store_cold_range_prefetch_single_blocks` (shared
singleton registrations, not physical GETs) from `cold_range_prefetch_read_bytes`
(direct range submissions). Window runs and internal-gap metrics still include
singleton segments. Singletons use ordinary block total/miss statistics rather
than direct-prefetch block counts.

Validation: the storage test target compiled; all 53 targeted tests passed (block
stream 4, block decoding 7, cold ranges 14, SST store 11, iterator 13, routing 2,
options 2). The final buffered-neighbor error extension was compiled and rerun
successfully through the actual iterator. Added tests cover a split singleton
joining an existing gated Foyer producer, eager cold/hot/cold submission, point
waiter survival after dropping the window, lazy decode/fill for all three cache
policies, delayed corruption reporting and reservation release. Existing tests
cover 16 fully cold blocks in one GET, disk hits/stale hints, full scans, seeks,
scan bounds, budget rejection and cancellation. Metrics inventory and diff
whitespace validation passed.

An ignored `cold_range_delivery_probe` compares the previous eager decode with
incremental decode using the same local debug build and in-memory backend:
16 contiguous blocks of about 32 KiB, Fill, no disk cache or scheduler, five
warmups and 30 measured repetitions per case. Both versions assert one physical
GET per window. The probe waits until IO completes to isolate delivery CPU while
also recording elapsed time including IO. The delay is a Tokio test timer, not a
network model; timer quantization limits end-to-end conclusions.

| Delay | Consumed blocks | First block before / after (us) | IO-ready to first before / after (us) | Total before / after (us) |
| --- | --- | --- | --- | --- |
| 0 ms | 1 | 528 / 56 | 500 / 31 | 530 / 56 |
| 0 ms | 16 | 506 / 60 | 479 / 32 | 509 / 533 |
| 2 ms | 1 | 2993 / 2998 | 450 / 35 | 2996 / 2998 |
| 2 ms | 16 | 2992 / 2531 | 445 / 43 | 2995 / 2984 |

These are medians from a small diagnostic, not a Q18 or production benchmark.
First-block delivery CPU decreases substantially; complete-scan CPU is not
removed (the zero-delay full scan was about 5% slower in this sample). With IO
included, first-block improvement is not uniform. The defensible gain is reduced
work before first delivery and avoidance of decoding/filling unused neighbors.

Evidence: `read-optimization-evidence/cold-range-lazy-validation.json`,
`cold-range-lazy-source-identities.json`, and `cold-range-lazy-logs.json.gz`.

### Observing overlap without adding sharing (2026-11-18)

Scheduler single-flight remains enabled by `reuse_inflight_reads`: a new read
fully covered by an active same-object GET can join it, subject to follower
capacity. It neither stitches a partially overlapping request out of multiple
GETs nor retains completed results for later readers. This round adds diagnostics
only, with no new sharing, timer, admission rule, Foyer change, or old Range
algorithm change.

Two observations distinguish the remaining cases:

| Observation | Count | Bytes |
| --- | --- | --- |
| New physical group dispatch partially overlaps an older same-object flight that cannot cover it | `object_store_read_scheduler_events{event="partial_overlap_dispatch"}` | `object_store_read_scheduler_partial_overlap_bytes` |
| External block fallback finds its bytes already returned but not yet decoded/published in a live ColdRanges window | `state_store_cold_range_buffered_overlap_reads{cache_policy="fill\|not_fill\|disable"}` | `state_store_cold_range_buffered_overlap_bytes` with the same label |

The scheduler computes intersections using existing active-flight metadata, after
trying complete-coverage reuse, and increments only when the physical group read
is polled. A dispatch is counted once; intersections with several old flights are
unioned so the same byte is not counted several times within that dispatch.
Different object paths and adjacent non-overlapping ranges are excluded. An old
range that fully covers the new one is excluded from this partial-overlap metric,
even if reuse is unavailable because of follower capacity. Only the sharing-enabled
group path supplies active flights: the metric is not a complete overlap census
when the scheduler/reuse option is disabled, and does not instrument standalone
fallback retries. The snapshot is taken at flight registration, not by tracing
network transfers. Counts include attempted reads that later fail. Repeated
subsequent dispatches can count the same file bytes again.

For SST reads, same object path means the same SST object. Scheduler metrics are
generic and can also include vector/other object reads on that CN. They deliberately
have no SST/path/table label. For a mixed workload, do not attribute the aggregate
exclusively to SSTs without additional tracing.

The second observation uses only metadata `(sst_id, remaining byte interval)`.
A successful, correctly sized ColdRanges read registers it inside the IO task,
so even a completed task whose result the iterator has not received is visible.
Consumption advances the interval after decode and optional Fill insertion;
dropping the buffered result/window unregisters it. No Bytes or futures are held
by the index, no response is served from it, and no read waits for it. Metadata
has a conservative per-run allowance in the existing prefetch budget. Lifecycle
updates use one short metadata mutex; consumption updates its offset atomically.

Only polled ordinary-block external fallbacks check that index. This includes
short-prefix/demand reads and singleton Fill producers, not only SQL point gets.
Memory/disk hits and Foyer followers do not independently increment it. Several
windows retaining the same block still yield one count per external fallback.
Fill, NotFill and Disable are separated because the latter two can intentionally
re-read without filling cache. This is an observation of a reuse opportunity,
not proof of a physical duplicate GET: scheduler reuse, subsequent cancellation,
and failed reads can change physical IO. It also does not count a later direct
ColdRanges request against another completed window. Both metrics are concurrent
snapshots, and a read can appear in both categories; do not add them together as
a total of duplicated GETs or bytes.

For a run shorter than one hour, select the relevant CN/job/instance series and
use the matching run interval (examples aggregate all selected instances):

```promql
sum(increase(object_store_read_scheduler_events{event="partial_overlap_dispatch"}[1h]))
sum(increase(object_store_read_scheduler_partial_overlap_bytes[1h]))
sum by (cache_policy) (increase(state_store_cold_range_buffered_overlap_reads[1h]))
sum by (cache_policy) (increase(state_store_cold_range_buffered_overlap_bytes[1h]))
```

No new enable switch is needed. Counters with no observations may have no exported
series; missing series can be displayed as zero. Compare these counts/bytes with
physical group reads and external block fetches, respectively, over the same run.
A low first count argues against general partial-flight stitching; a high second
Fill count motivates evaluating shared completed-range bytes. Neither counter on
its own establishes an end-to-end speedup from changing ownership.

Validation: storage and object-store test targets compiled; 73 related tests
passed (scheduler 26, ColdRanges and observation index 17, SST store 11, iterator
13, block stream 4, routing 2). New tests cover full-coverage reuse exclusion,
partial byte-union accounting, different objects, flight completion, observation
before iterator consumption, Foyer follower/cache-hit exclusion, all cache
policies, consumed-block exclusion and dropping an unconsumed completed task.
Metrics inventory and `git diff --check` passed. No workload frequency or latency
result is claimed before running the instrumented build on the user's workload.
Evidence: `read-optimization-evidence/cold-range-overlap-metrics-validation.json`,
`cold-range-overlap-metrics-source-identities.json`, and
`cold-range-overlap-metrics-logs.json.gz`.

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

### SF10 Q18 ColdRanges prefetch budget 4 versus 16 GiB, 2026-11-18

One guarded `risedev d full` run reused native release source
`0bb0df4acf388a958e9592787ec36ceebcb376ed` and executable SHA-256
`81fc8b116fcd1522f87eef68195d914ade84bfabfc2c0f29bc92eef56c0d78ed`.
The only TOML change from the preceding six-worker arm raised
`storage.prefetch_buffer_capacity_mb` from 4096 to 16384.
A startup-log check verified the override. The source SST fingerprint,
fresh cold v1346 APFS seed, SQL, plan, 53 actors, six LocalityProviders,
12 CN slots, fixed4, eight streaming workers, six dedicated I/O workers,
G128/P32/B16, N16/1 MiB, zero gap, Foyer cache settings and trace-off
state all matched. MultiGet, metadata reuse and in-flight reuse stayed
enabled. Foyer file caches were off. The backend was local MinIO.
The same binary required no additional compilation.

| Metric | Fixed release, 4 GiB, one run | Same release, 16 GiB, one run |
| --- | ---: | ---: |
| CREATE MV wall, s | 677.11 | 693.08 |
| Source checkpoint, s | 96.25 | 81.04 |
| Actual source complete from 1 s scrapes, s | 91.00–92.01 | 80.00–81.01 |
| Source GETs | 221,199 | 44,532 |
| After-source GETs | 83,208 | 78,409 |
| Total GETs / read GiB | 304,407 / 36.779 | 122,941 / 35.391 |
| Cumulative physical GET service, s | 717.847 | 269.720 |
| Physical GET mean / p99 / p99.9, ms | 2.358 / 16 / 32 | 2.194 / 16 / 32 |
| ObjectStore caller mean / p99 / p99.9, ms | 8.124 / 252.51 / 378.77 | 11.240 / 252.51 / 378.77 |
| After 5 s admission / dispatch p99, ms | 40.96 / 40.96 | 0.01 / 5.12 |
| Rejected prefetch window reservations | 199,920 | 0 |
| Peak sampled prefetch accounting, MiB | 4,096.00 | 11,364.69 |
| Peak CN RSS, MiB | 4,371 | 5,736 |
| Whole ordinary block outer fetches | 246,399 | 52,451 |
| Buffered-overlap Fill outer fetches | 6,328 | 513 |
| Partial-overlap physical dispatches | 111 | 300 |

The larger budget removed all observed full-window admission rejections.
Source GETs fell 79.9% and whole-query GETs fell 59.6%. After the first
five seconds, no sampled G or P limit was reached in the 16 GiB run.
Source physical GET service time fell from 597.34 to 152.07 aggregate
seconds across parallel reads. Source checkpoint latency improved by
15.21 s. The 16 GiB setting admits the conservative charge; observed peak
accounting was 11.10 GiB and peak CN RSS was 5.60 GiB.

The caller mean uses different request populations: the 4 GiB fallback
issued many more small logical reads. Its lower aggregate mean must be
interpreted alongside request count, size and admission/dispatch tails.
Percentiles are histogram bucket upper bounds. The timing results are
one run per budget.

The full-window rejection fallback still exists in the source. This
configuration isolates the no-rejection workload for further experiments.
The result establishes that the configured budget was sufficient for
this local input. Production budget sizing and reduced-window fallback
remain separate work.

#### Elapsed-time attribution

Higher prefetch capacity improved source scanning. Downstream processing
took longer in this particular run:

| Operator interval | 4 GiB wall / CPU, s | 16 GiB wall / CPU, s | Physical GET service, 4 → 16 GiB, s |
| --- | ---: | ---: | ---: |
| First LocalityProvider → provider 24 | 92.01 / 634 | 104.99 / 685 | 36.45 → 34.91 |
| Provider 24 → 22 | 256.00 / 1,063 | 268.06 / 1,131 | 46.17 → 44.02 |
| Provider 22 → 20 | 143.01 / 671 | 143.00 / 665 | 24.45 → 23.55 |
| Provider 20 → graph merge | 91.83 / 318 | 94.92 / 333 | 17.07 → 15.18 |

The first two downstream intervals added about 25 s with more CN CPU
work and less cumulative backend service. The first LocalityProvider
started 12.02 s earlier; the final graph merge completed 16.10 s later.
Table 38 iterator initialization grew from 806.68 to 846.96 cumulative
seconds, while candidate checks fell from 239.15M to 224.21M. Table 32
initialization decreased from 325.66 to 305.03 s. Intermediate state
is generated during each run. These observations locate the elapsed
difference in downstream CPU work; they do not isolate why that CPU
cost differed.

The old pre-fix six-worker measurement had 35,938 source GETs, 118,369
total GETs, and an 86.03 s source checkpoint. The 16 GiB fixed build
substantially restores IO aggregation, with a remaining source GET
difference relative to that historical implementation. The primary
same-binary comparison here is the fixed 4 versus 16 GiB pair.

All 136,472,104 snapshot rows were observed and all 100 output-oracle
rows matched. No measured read errors or retries occurred. Owned
services, listeners, full-profile containers and the disposable
MinIO/Postgres clone were removed. The lowest sampled free disk was
81.49 GiB. The original cold seed was retained.

The local archive is
`target/locality-tpch-q18/sf10-q18-cold-ranges-0bb0df4acf-io6-prefetch16g.t0813fud/`.
`prefetch16g-vs-4g.json` contains both phase reports, settings, binary
identity and new overlap observations. `prefetch16g-vs-4g.md` gives
the complete comparison table. Raw startup, congestion, source and
whole-query metrics and verified cleanup remain beside them.
