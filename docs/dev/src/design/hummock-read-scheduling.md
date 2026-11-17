# Hummock blockwise prefetch and read scheduling

Blockwise prefetch lets a forward SST iterator request nearby blocks before
consuming them. Foyer owns the cache entries and shares fetches for the same
block. The object-store scheduler merges ready ranges to reduce physical GETs.
This design preserves the existing SST format, visibility rules, and cache
ownership. The [configuration and experiment record](./read-optimization-testing.md)
lists the startup controls, metric names, and measured runs.

## Read path

| Layer | Input and decision | Output and ownership |
| --- | --- | --- |
| SST iterator | Select a window within the scan end, `max_blocks`, and `max_bytes`. Count each block by the larger of its compressed and decoded size. | Register neighbors first, then demand with the same window token. Retain neighbor responses until consumption or discard. Dropping the registration handle does not flush the plug. |
| Foyer | Resolve each block from cache or share the fetch with other readers. | Only an outer fetch can submit a range. The store attaches the window token to external fetches. Foyer still owns cache lookup, single-flight and publication. |
| Object-store scheduler | Flush a short plug started by the first actual outer read, then try to join each group into a pending group of the same object. Take one credit for each new group. | Wait for physical capacity, dispatch the group with the oldest admitted request, and return the requested slices. |

A 16-block window can yield fewer outer reads because Foyer handles hits
and shared fetches. With a zero gap limit, a cache hit between two cold
ranges leaves a hole that the scheduler cannot merge across. Foyer
producers can reach the scheduler at different times, so even adjacent
blocks can use separate GETs if the first group has already dispatched.
The first actual outer read starts the window's plug. Registration can finish
before any producer reaches the scheduler; dropping the plug handle does not flush.
The Hummock path does not register expected ranges from cache responses. A pending Foyer response
can be a shared fetch or a disk hit, neither of which promises a new outer
read for this window. Foyer retains every registration's fallback and owns
its selection; RW does not need to observe a leader/waiter role.

Late outer reads use ordinary admission. They can still merge with pending
groups until a physical task selects the group, or reuse a covering active
GET when enabled. This avoids waiting for nonexistent candidates, but does
not guarantee that cold neighbors arriving at different times share a GET.
Groups that need a new credit wait independently.

When the disk cache is enabled, `contains` remains only a submission hint:
a possible cache hit uses the ordinary fetch path, and a stale hint can still
produce an external read through the same atomic Foyer registration.

The plug has three states represented by one optional vector: idle (empty),
collecting (nonempty), and closed (None). The first read schedules one flush;
that task yields once, then closes the plug. With dedicated IO enabled, this
runs on that runtime; otherwise it is spawned on the current producer runtime. New arrivals
never restart or extend the flush. Reaching the smaller of the window/member
limits, or the logical-byte limit, closes it immediately. A one-member window
therefore needs no flush task. Cache-only windows schedule no task at all.

Closing takes the collected vector exactly once under the staging mutex and
submits outside the lock. A cancelled flush task closes the plug and drops its
response senders, including cancellation before its first poll. The plug cannot
reopen. A retained plug handle or token cannot hold it open. G and P are only
taken after collection. Ordinary point reads do not create a plug, but a Foyer
follower can still depend on a prefetch producer whose actual read is plugged.

There is no expected-candidate list, timer, or pressure heuristic. A yield is a
scheduling opportunity, not a time bound or a guarantee that another runtime
has advanced. An incomplete window containing one cold read pays the extra
handoff/yield; local measurements quantify this cost in the testing record.
Late arrivals keep using pending merging and covered in-flight reuse.
The old `prefetch_batch_wait_us` setting and its wait/release metrics were removed.
Historical candidate-based and Lead-only measurements remain in the testing record.

The Foyer dependency is restored to the existing recovery-optimization fork
at `02d6b5e`. No IO-scheduler-specific Foyer API or patch is required.

## Executor entry points

The four Blockwise switches act independently. The prefix-scan switch
maps a caller's Blockwise request to ordinary reads when disabled.
Ordinary Snapshot Backfill source scans and LocalityProvider snapshot
scans each fall back to Range when their own switch is disabled. Change-log scans construct their own SST iterator options.
The SST iterator registers lookahead only with `CachePolicy::Fill` and
when the scan bounds and window budget allow at least one neighbor.
[Hummock option selection](https://github.com/risingwavelabs/risingwave/blob/4549066c84066448793e134ea404945e91d40c07/src/storage/src/hummock/store/version.rs#L1121)

| Executor or read path | Where it enters | Startup control |
| --- | --- | --- |
| HashJoin | A join-cache miss scans state rows by join-key prefix. The degree-state lookup uses the same prefetch option when the join needs degrees. [Call sites](https://github.com/risingwavelabs/risingwave/blob/4549066c84066448793e134ea404945e91d40c07/src/stream/src/executor/join/hash_join.rs#L266-L337) | `storage.blockwise_prefetch.enabled` |
| TemporalJoin | Lookup-side cache fills and memo-table scans for deletes or update-deletes request prefix prefetch. [Cache fill](https://github.com/risingwavelabs/risingwave/blob/4549066c84066448793e134ea404945e91d40c07/src/stream/src/executor/temporal_join.rs#L115-L145) · [Memo scan](https://github.com/risingwavelabs/risingwave/blob/4549066c84066448793e134ea404945e91d40c07/src/stream/src/executor/temporal_join.rs#L553-L572) | `storage.blockwise_prefetch.enabled` |
| AsOfJoin | Cache population scans the join-key prefix. The uncached range-stream path requests prefix prefetch with the inequality bounds. [Cache fill](https://github.com/risingwavelabs/risingwave/blob/4549066c84066448793e134ea404945e91d40c07/src/stream/src/executor/join/asof_join.rs#L364-L390) · [Uncached scan](https://github.com/risingwavelabs/risingwave/blob/4549066c84066448793e134ea404945e91d40c07/src/stream/src/executor/join/asof_join.rs#L689-L698) | `storage.blockwise_prefetch.enabled` |
| OverWindow | Partition cache expansion scans the full partition or a bounded forward subrange. Emit-on-window-close state recovery scans the partition prefix. [Full partition](https://github.com/risingwavelabs/risingwave/blob/4549066c84066448793e134ea404945e91d40c07/src/stream/src/executor/over_window/over_partition.rs#L792-L808) · [Bounded subrange](https://github.com/risingwavelabs/risingwave/blob/4549066c84066448793e134ea404945e91d40c07/src/stream/src/executor/over_window/over_partition.rs#L1007-L1021) · [State recovery](https://github.com/risingwavelabs/risingwave/blob/4549066c84066448793e134ea404945e91d40c07/src/stream/src/executor/over_window/eowc.rs#L387-L399) | `storage.blockwise_prefetch.enabled` |
| LocalityProvider | The snapshot stream scans its state table one vnode at a time and resumes from the saved position. Sort-buffer replay uses Range prefetch. [Snapshot scan](https://github.com/risingwavelabs/risingwave/blob/4549066c84066448793e134ea404945e91d40c07/src/stream/src/executor/locality_provider.rs#L338-L381) · [Sort-buffer replay](https://github.com/risingwavelabs/risingwave/blob/4549066c84066448793e134ea404945e91d40c07/src/stream/src/executor/locality_provider.rs#L592-L613) | `storage.blockwise_prefetch.locality_backfill_enabled` for the snapshot stream |
| Snapshot backfill | The source snapshot uses Range by default and can opt into Blockwise independently. Catch-up through `BatchTable` calls Hummock `iter_log`, which selects Blockwise for old and new value SST iterators and passes down the key-range end. [Backfill](https://github.com/risingwavelabs/risingwave/blob/4549066c84066448793e134ea404945e91d40c07/src/stream/src/executor/backfill/snapshot_backfill/executor.rs#L501-L567) · [Change-log iterator](https://github.com/risingwavelabs/risingwave/blob/4549066c84066448793e134ea404945e91d40c07/src/storage/src/hummock/store/version.rs#L1295-L1337) | `storage.blockwise_prefetch.snapshot_backfill_enabled` for the source snapshot and `storage.blockwise_prefetch.changelog_enabled` for catch-up |
| HashAgg and Materialize | HashAgg restores intermediate state for multiple missing group keys through `StateTable::get_rows`. Materialize uses the same API for missing cache entries on its checked-write path. The point API runs bounded concurrent gets and can reuse SST metadata handles within that call. [HashAgg](https://github.com/risingwavelabs/risingwave/blob/4549066c84066448793e134ea404945e91d40c07/src/stream/src/executor/aggregate/hash_agg.rs#L294-L309) · [Materialize](https://github.com/risingwavelabs/risingwave/blob/4549066c84066448793e134ea404945e91d40c07/src/stream/src/executor/mview/cache.rs#L360-L403) | `storage.read_batch.enabled`, with optional `meta_reuse` |
| Batch RowSeqScan | Full-key probes use individual point reads with bounded concurrency. The remaining SQL `LIMIT` can further lower the number of outstanding point reads. [Point-read loop](https://github.com/risingwavelabs/risingwave/blob/4549066c84066448793e134ea404945e91d40c07/src/batch/executors/src/executor/row_seq_scan.rs#L203-L228) | `batch.point_get_concurrency` |

These paths share the object scheduler once a read reaches a supported
Hummock object store. It also accepts ordinary bounded reads, including
Range-prefetch reads and point-read misses. The store enables coalescing
through `storage.object_store.enable_hummock_read_coalescing` for native S3,
in-memory object storage, and OpenDAL S3 or MinIO.
[Store wiring](https://github.com/risingwavelabs/risingwave/blob/4549066c84066448793e134ea404945e91d40c07/src/storage/src/store_impl.rs#L990-L1011) ·
[Backend selection](https://github.com/risingwavelabs/risingwave/blob/4549066c84066448793e134ea404945e91d40c07/src/object_store/src/object/mod.rs#L297-L312)

The Blockwise, coalescing, and point MultiGet switches default to off.
With coalescing enabled, the scheduler defaults to 64 admitted groups,
16 members per group, 32 physical reads, and a 256 KiB maximum range.
Blockwise defaults to three blocks and 256 KiB per iterator window.
`use_dedicated_io_runtime` optionally moves the dispatcher, scheduled
physical reads, range-error fallback, and deferred admission to a
process-shared runtime. It is off by default;
`dedicated_io_runtime_workers` defaults to two when enabled. Foyer
producers keep their existing runtime ownership.

## Implementation boundaries

The scheduler has three implementation units under
`src/object_store/src/object/`:

- `read_scheduler/batch.rs` collects external reads for one prefetch window.
  The first actual read starts a one-yield flush; count/byte limits flush early.
  Registration lifetime does not delay or truncate the plug. Collection takes
  neither a group credit nor a physical permit; late reads use ordinary admission.
- `read_scheduler.rs` owns admission and the pending queue. Compatible
  requests can join a pending group at full G, including requests already
  waiting for a credit. Absorbing a waiter also cancels its semaphore acquire.
- `read_scheduler/execution.rs` owns dispatch reservations, backend reads,
  in-flight reuse, cancellation, and result delivery. Reserving P does not
  remove a group from the pending queue: the spawned task selects it when
  it first runs. After selection, merging stops; execution can still trim
  cancelled end members before fixing the GET range. In-flight followers
  can only reuse ranges that GET already covers.

The regression tests remain in the `read_scheduler::tests` module, with
source in `read_scheduler/tests.rs`. Preserve the handoff and cancellation
tests when changing these boundaries: moving selection earlier or waking
waiters to retry instead of absorbing them can split an otherwise mergeable GET.

## Admission and resource lifetime

| Limit | What it bounds |
| --- | --- |
| `max_admitted_groups` (G) | Groups from queue admission through completion. |
| `max_batch_requests` | Logical members of one group. |
| `max_read_bytes` | Eligible input range and merged range span. |
| `max_gap_bytes` | Maximum gap between neighboring ranges. The default zero merges contiguous and overlapping ranges. |
| `max_inflight_reads` (P) | Concurrent scheduled backend reads, including retries and individual range-error fallbacks. |
| `reuse_inflight_reads` | Let a read share a scheduled GET that already covers its range. |

A compatible read can join a pending group even when G is full. A
range that bridges compatible pending groups can absorb one group and
return its credit immediately. A read that needs a new group waits in
its caller and tries to merge again when it acquires a credit. The
scheduler starts a physical read task as soon as P is available. The
task takes the oldest group when it first runs, so the group remains
mergeable during the existing task handoff. A task releases its P
permit if cancellation or merging leaves no group to take. Oversized
and unbounded requests take the direct path.

An in-flight follower consumes neither another G nor another P. One
physical GET accepts up to `max_batch_requests` additional followers.
Fully covered readers already waiting for a group credit can join when
the GET is published. The physical read remains alive while any of its original requests or
followers is waiting. The scheduler removes the flight on completion
or cancellation and returns each follower's requested bytes.

A group holds its credit through response delivery, cancellation, or
range fallback. If the merged read has a range error, it releases its
physical slot before retrying individual ranges under the same P limit.
The worker can reclaim a queued group or cancel a physical read when
all its waiters leave. If interior members cancel, the remaining read
can cross their bytes, which are counted as overread.

Iterator limits cover one window. Foyer owns its producers, including
ones waiting for scheduler admission. G bounds admitted groups, not all
logical requests: staged batch members and callers waiting for G are
outside that budget. Each admitted group has a member limit, and in-flight
reuse permits an additional bounded set of followers as described above.
There is no separate process-wide preparation budget. Historical request-count
admission limits are therefore not interchangeable with G. Use the waiting
gauge alongside group and physical depths to diagnose pressure outside the queue.

## Historical Q18 preparation experiments

The old Q32 trace captured 1,112 complete Locality windows with 16 selected
blocks. Of the 934 windows with 16 external reads, 867 used one GET and 67
used multiple GETs. In each of those 67, the worker had not yet dequeued
every member when it selected the first group. The trace does not attribute
every late member to Q pressure.

The whole-window implementation closes the Q-pressure case. Its deterministic
test holds ordinary Q slots with an existing GET, verifies an adjacent window
issues one GET, and checks point-read progress and cancellation. The fallback
test holds its first physical read open and checks that only the tail retains
preparation capacity while waiting.

Two untraced whole-window Q64 runs with 16-block windows saved 28,171 and
28,182 scheduled requests by merging after the source scan checkpoint.
The second run observed six positive gaps in that phase, three of which
were at most 64 KiB. Across the full runs, Q64 issued 53,914 and 47,508
GETs. The historical Range run issued 51,333 GETs. The Q64 runs shared
the same starting seed but sent different numbers of reads to the scheduler.
Their elapsed times do not isolate the effect of the prefetch mode.

The later Q64 run recorded seven packets admitted with preparation permits
and zero admission or packet-limit splits. These are measurements of the
frozen whole-window binary from before the fallback accounting cleanup.

The checkpoint Q64 trace matched 770 complete, fully cold 16-block
LocalityProvider windows by request identity. Each produced 16 Foyer outer
reads, one 16-request packet, and one successful GET. The only observed
admission split followed a preparation timeout in a three-block window.
The [request identity trace](./read-optimization-evidence/q18-window-identity.md)
records the cohort and the split. Gap observations describe the worker's
ready set and need a combined-span check to estimate eligible merges and
extra bytes.
The [Q18 repeat](./read-optimization-testing.md#q18-q64-io-distribution-repeat)
records the I/O distributions and their measurement boundaries.
The [checkpoint artifacts](./read-optimization-evidence/q18-checkpoint.md) retain the
aggregate data, query, and worker configuration.

## Current policy and simplification boundaries

The retained production policy uses fixed blockwise windows, registers neighbors
before demand, and retains the first-actual-IO plug. The no-plug, rolling-refill
and demand-first experiments were not adopted. Their retained fixture and backend
latency injection are test-feature code, not production policy switches.

`ReadPlug` has no drop-driven sealing behavior. Its `ReadPlugToken` is a shared window
handle captured by fallback futures. A window can close before registration has
finished on another runtime. Such late reads fall back to ordinary admission.
The group count limit is distinct from physical concurrency: G bounds admitted
groups through completion, while P bounds reserved/scheduled backend reads.
Staged plug members and G waiters are outside G, so G is not a total outstanding
logical-request or memory cap.

The disk-cache `contains` check is still a routing hint: a positive hint omits
the plug token, but Foyer remains authoritative. A stale positive miss therefore
uses ordinary admission. Since only actual outer reads enter the plug, this
check is not required to prevent waiting for hits. Removing it would change
batch participation and should not be described as a behavior-preserving cleanup.

The unused `sealed_batch_empty` event has been removed. The single/multiple flush
events retain their existing Prometheus labels (`sealed_batch_single` and
`sealed_batch_multiple`) for query compatibility. `ReadPlug` and `ReadPlugToken`
name the window handle and the token captured by each external-read future.
The cancellation guards, whole-group G admission, mergeable G waiters, pending
merge until physical selection, and range-error fallback preserve substantive
lifecycle/resource behavior. Removing them requires more than a naming cleanup.
