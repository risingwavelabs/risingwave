# Streaming read optimization controls and diagnostics

These startup controls are experimental and default OFF. They require a worker restart;
changing a SQL session parameter does not change them. Use the same TOML on every CN.
No object format, visibility or compaction policy changes are required.

## Configuration

```toml
[storage.blockwise_prefetch]
enabled = true
max_blocks = 3
max_bytes = 262144

[storage.read_batch]
enabled = true
concurrency = 10
meta_reuse = true
meta_max_entries = 16
meta_max_bytes = 1048576

[storage.object_store]
enable_hummock_read_coalescing = true

[storage.object_store.read_scheduler]
max_batch_requests = 16
max_admitted_requests = 32
max_read_bytes = 262144

[batch]
point_get_concurrency = 1
```

All numeric controls above must be positive. Unknown fields in the three new
`blockwise_prefetch`, `read_batch` and `read_scheduler` sections are rejected,
so misspelled experiment settings cannot silently use defaults. The batch row scan control is independent
of streaming MultiGet; retain 1 for streaming-only experiments. A value of 1 for
`max_blocks` disables speculative neighbors, while still permitting demand reads.

| Control | Owner and effect | Evidence for tuning |
| --- | --- | --- |
| `blockwise_prefetch.enabled` | Bounded forward prefix scans requested by HashJoin row/degree reads. Point gets remain demand-only. | Window distribution and consumed/discarded handles. |
| `max_blocks` | Per-iterator window including the demand block. Later windows are issued as consumption advances. | `consume_pending` plus large useful windows can justify increasing N. Single-block ranges cannot benefit. |
| `max_bytes` | Window sum of max(compressed, decoded) block bytes. An oversized demand block reads alone. This is not a CN-wide memory cap. | `limit="bytes"`, window bytes, RSS and discarded handles. |
| `read_batch.enabled` | Enables native batches in adapted streaming operators: HashAgg, distinct, dedup, materialize conflict reads and backfill state reads. | Batch keys, latency, table counters. Not every execution of these operators needs a storage read. |
| `concurrency` | Maximum independent keys active within one MultiGet. A completion admits the next key immediately; there is no wait to fill a batch. | Batch size/concurrency, latency, CPU, backend inflight and saturation. |
| `meta_reuse` | Independent metadata reuse within native MultiGet; no cross-key data-block cache. | `meta_batch_ready/loading/load` and `meta_total`. |
| `meta_max_entries/bytes` | Bounds ready regular metadata retention. One oversized SST may additionally remain cached; loading cells are bounded by active keys. | Budget rejects, oversized metadata, RSS. Increasing a budget cannot help when keys visit unrelated SSTs. |
| `max_batch_requests` | Maximum already-ready requests planned together; also bounded by admission. | Dequeue batch size, group size, split reasons. It does not wait for more requests. |
| `max_admitted_requests` | Retained logical requests, queued plus in flight, per scheduler. Saturation bypasses scheduling. | Pending gauge and `route="scheduler_bypass"`; not a global IO concurrency cap. |
| `max_read_bytes` | Maximum eligible input and merged contiguous range. Same object, adjacent/overlapping ranges only. | `route="oversized"`, `split_bytes`, actual operation bytes and latency. |

The old `RW_EXPERIMENTAL_BLOCKWISE_PREFETCH` and
`RW_EXPERIMENTAL_SST_META_BATCH_REUSE` environment variables are no longer read.
Disable all three streaming controls for an in-image control; this does not undo other
source changes such as iterator construction/future sizing. A main comparison needs a
separate main image built with the same architecture, features and profile.

## Metric meanings

Metrics below are available without TRACE. Storage per-table counters use the usual
Info metric level. Aggregate all relevant CN instances; retain instance labels when
investigating skew. Take deltas over a common interval and drain in-flight operations
before reconciling counts. Counts from different layers are not interchangeable.

| Metric | Meaning / exclusions |
| --- | --- |
| `object_store_read_requests{media_type,route}` | Logical reads reaching the object wrapper. Routes: direct (disabled), scheduled, scheduler_bypass (no admission), oversized, unbounded, empty_range. Foyer hits/followers need not reach here. |
| `object_store_read_request_bytes{media_type,route}` | Bounded logical range sizes before coalescing. Overlaps count repeatedly. Unbounded reads have no size sample. |
| `object_store_read_request_duration_seconds{media_type,route}` | Logical latency including scheduler wait and retry, also recorded for cancelled futures. |
| `object_store_read_outcomes{media_type,stage,outcome}` | Completion counts: logical, physical (one direct read plus its retry loop), attempt (each backend read invocation); ok/error/cancelled. SDK-internal HTTP retries are not separate attempts here. |
| `object_store_read_in_flight{media_type,stage}` | Live reads at each of those layers, including waiting futures; RAII decrements on cancellation. |
| `object_store_operation_bytes{type="read"}` | Successful physical read result sizes. `_count` is successful physical operations; `_sum` is successful returned bytes. Neither counts failed/partial network transfers. |
| `object_store_operation_latency{type="read"}` | Physical read duration including its retry loop. Compare with logical latency and queue latency. |
| `object_store_request_retry_count{type="read"}`, `object_store_failure_count{type="read"}` | Wrapper retries and terminal failures. Use with attempt outcomes; do not count group-size savings as net savings during failures. |
| `object_store_read_scheduler_batch_size` | Live requests present together at dequeue. Small batches indicate too little simultaneous eligible work, not necessarily too small a configured cap. |
| `object_store_read_scheduler_group_size` | Logical members per dispatched group. Sum minus count is *planned* consolidation, not exact net GET savings under cancellation/fallback. |
| `object_store_read_scheduler_queue_duration_seconds` | Admission-to-dispatch latency, before physical retry. |
| `object_store_read_scheduler_pending` | Admitted queued/in-flight logical requests. Should return to zero when quiescent. |
| `object_store_read_scheduler_events{event}` | split_object/split_gap/split_bytes planning boundaries; cancelled_queued (at dequeue), cancelled_before_dispatch (after planning); group_ok/error/cancelled; range_fallback_requests; merged_requests (members minus one for successful groups). Boundaries describe a single ready batch, not all potential future neighbors. |
| `object_store_read_scheduler_gap_bytes` | Positive same-object gaps encountered during planning; these split groups. Cancellation after planning can leave an interior hole in a retained group. |
| `state_store_get_batch_keys{table_id}` | Input keys per MultiGet, including duplicates/missing keys. `_count` records started API batches. |
| `state_store_get_batch_concurrency{table_id}` | Configured key concurrency clipped to batch length; not measured peak utilization. |
| `state_store_get_batch_duration{table_id}` | One end-to-end sample per completed/failed/cancelled API batch. Never compare directly with per-key get latency without accounting for batch size. |
| `state_store_get_batch_outcomes{table_id,outcome}`, `state_store_get_batch_in_flight{table_id}` | Batch ok/error/cancelled and active API calls. |
| `state_store_sst_store_block_request_counts{table_id,type}` | Existing data/meta total/miss; added meta_batch_ready (retained hit), meta_batch_loading (existing loading cell), meta_batch_load (actual batch loader attempt), meta_batch_budget_reject, meta_batch_oversized. Loading-cell reuse is not necessarily time spent blocked. |
| `state_store_meta_batch_wait_duration_seconds` | Time resolving an existing loading cell; includes retry when its original producer was cancelled. |
| `state_store_blockwise_prefetch_window_blocks{limit}` | Window size including demand, clipped by range/blocks/bytes. A window of one produces no lookahead. |
| `state_store_blockwise_prefetch_window_bytes` | Selected window budget weight, including demand. Not transferred bytes. |
| `state_store_blockwise_prefetch_events{event}` | policy_skipped; registered_ready/pending; registration_error; consumed (handle taken); consume_ready/pending; consume_ok/error; discarded. Consumed is an attempted use; cancellation can occur before success. |
| `state_store_blockwise_prefetch_bytes{event}` | Compressed lengths of registered, consumed or discarded neighbor handles; not physical network bytes. |
| `state_store_blockwise_prefetch_wait_duration_seconds` | Wait when consuming a registered neighbor, including cancellation. High pending fraction/wait can mean the lookahead is too short or the backend is slow. |
| `state_store_block_cache_read_events{event}` | Fill requests classified memory_hit/pending at registration; outer_fetch counts actual producer invocations (including other cache policies). Pending also includes disk-cache reads and shared fetch followers. It is not an S3-miss counter. |

Before dispatch, cancelled members are removed and the outer span is trimmed. If an interior
member cancels, the existing group is retained rather than split into additional GETs; its
bytes may still be read. If every member cancels, no read is issued. Cancellation after
dispatch cannot undo bytes already transferred.

Discarding a prefetch handle does not establish wasted IO: Foyer can retain the result
or share its producer with another iterator. The metrics do not subtract pending minus
outer_fetch to claim an exact follower count. For that attribution use Foyer's own
cache metrics plus a short `state_store_read_batch=trace` capture correlating block,
object range, logical request IDs and scheduler group dispatch. Do not enable TRACE
in the timed performance comparison, or use object/key IDs as Prometheus labels.

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
# Scheduler saturation bypass fraction, among eligible submissions.
sum(rate(object_store_read_requests{route="scheduler_bypass"}[5m]))
/ sum(rate(object_store_read_requests{route=~"scheduled|scheduler_bypass"}[5m]))
# Batch latency by state table.
histogram_quantile(0.99, sum by (le, table_id)
  (rate(state_store_get_batch_duration_bucket[5m])))
# Mean batch key count.
sum by (table_id) (rate(state_store_get_batch_keys_sum[5m]))
/ sum by (table_id) (rate(state_store_get_batch_keys_count[5m]))
# Meta reuse distribution, including actual loader attempts.
sum by (table_id, type) (rate(state_store_sst_store_block_request_counts{
  type=~"meta_batch_ready|meta_batch_loading|meta_batch_load"}[5m]))
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
