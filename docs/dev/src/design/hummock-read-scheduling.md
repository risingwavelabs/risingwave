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
| SST iterator | Select a window within the scan end, `max_blocks`, and `max_bytes`. Count each block by the larger of its compressed and decoded size. | Register lookahead with Foyer and retain the neighbor handles until consumption or discard. Close the submission batch after registering the demand block. |
| Foyer | Resolve each block from cache or share the fetch with other readers. | Only an outer fetch can submit a range. The store attaches the window token to external fetches. Foyer still owns cache lookup, single-flight and publication. |
| Object-store scheduler | Release ready ranges at registration close. Optionally wait briefly for compatible ranges in the same window, then try to join each group into a pending group of the same object. Take one credit for each new group. | Wait for physical capacity, dispatch the group with the oldest admitted request, and return the requested slices. |

A 16-block window can yield fewer outer reads because Foyer handles hits
and shared fetches. With a zero gap limit, a cache hit between two cold
ranges leaves a hole that the scheduler cannot merge across. Foyer
producers can reach the scheduler at different times, so even adjacent
blocks can use separate GETs if the first group has already dispatched.
By default the batch releases the reads available at registration close.
With `prefetch_batch_wait_us` enabled, the scheduler partitions candidate
ranges by its existing gap, byte and member limits. A group releases when
all its candidates arrive, the deadline from its first external read
expires, or admission pressure is observed at seal or arrival. A held
group takes neither G nor P. A request that arrives after release takes
the ordinary path. Groups that need a new credit wait independently.
Each batch is bounded by its iterator window. A demand reader sharing
a prefetched fetch through Foyer also shares that fetch's wait.
When the disk cache is enabled, the iterator's block registrations use
Foyer's `contains` as a submission hint. A block that may already be
cached goes through the ordinary fetch path. A stale hint can still
produce an external read through the same Foyer registration. Confirmed
memory hits are also excluded from the candidates. Disk hits and shared
fetches can leave candidates unresolved until the deadline.

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
physical reads and their fallback to a process-shared runtime. It is off
by default; `dedicated_io_runtime_workers` defaults to two when enabled.
Foyer, deferred admission and prefetch batch timers keep their existing
runtime ownership.

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
ones waiting for scheduler admission. The admitted group limit bounds
scheduler-owned requests. Use the waiting gauge alongside group and
physical depths to diagnose pressure outside the queue.

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
