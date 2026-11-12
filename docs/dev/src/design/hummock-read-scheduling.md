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
| SST iterator | Select a window within the iterator's scan end, `max_blocks`, and `max_bytes`. Count the demand block and use the larger of its compressed and decoded sizes for each block's budget weight. | Register the neighbors and then the demand block in one `ReadBatch`. Seal registration before awaiting the demand block. The iterator keeps the neighbor handles until consumption or discard. A one-block window uses the ordinary demand path. |
| Foyer | Resolve each block as a cache hit, shared fetch, or fetch that needs an object read. | Only an actual external fetch can stage an object range. Cache hits and followers resolve their registration without adding a range. An independent producer may outlive one iterator's handle. |
| Window preparation | Reserve capacity before registration and start the deadline when the first cold request stages. | Once the sealed window resolves or the deadline expires, publish its surviving requests. A hot demand block can return while the driver continues preparing its neighbors. Late producers use ordinary admission. |
| Scheduler admission | Charge each retained logical descriptor to ordinary admission or to its existing preparation permit. | Publish a complete window as one packet when it fits and ordinary slots are available. When ordinary slots are short, a complete mergeable window can transfer preparation permits into the packet. |
| Scheduler dispatch | Collect ready packets, wait for physical capacity, and select the group containing the oldest ready request. | Merge adjacent or overlapping ranges of the same object under `max_read_bytes`. Execute one physical GET per group and return each requested slice. |

The window size is a count of selected blocks, the packet size is a count of
admitted object reads, and the physical GET size is a span in stored bytes.
An iterator can select 16 blocks and Foyer can satisfy some of them without
creating any new I/O. If two cold runs remain on either side of a cache hit,
the scheduler issues separate GETs because the intervening bytes form a gap.
The worker consumes a packet intact, but it can regroup its members with
other ready reads. An older adjacent range can consume some of the byte budget
and move the final GET boundary inside that packet.

## Executor entry points

The three Blockwise switches act independently. The prefix-scan switch
maps a caller's Blockwise request to ordinary reads when disabled.
LocalityProvider snapshot scans fall back to Range when their switch is
disabled. Change-log scans construct their own SST iterator options.
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
| Snapshot backfill | The source snapshot uses Range prefetch. Catch-up through `BatchTable` calls Hummock `iter_log`, which selects Blockwise for old and new value SST iterators and passes down the key-range end. [Backfill](https://github.com/risingwavelabs/risingwave/blob/4549066c84066448793e134ea404945e91d40c07/src/stream/src/executor/backfill/snapshot_backfill/executor.rs#L501-L567) · [Change-log iterator](https://github.com/risingwavelabs/risingwave/blob/4549066c84066448793e134ea404945e91d40c07/src/storage/src/hummock/store/version.rs#L1295-L1337) | `storage.blockwise_prefetch.changelog_enabled` for the catch-up phase |
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
With coalescing enabled, the scheduler defaults to Q32/B16/P32,
256 KiB maximum range, and a 1 ms explicit-window preparation timeout.
Blockwise defaults to three blocks and 256 KiB per iterator window.
The Q18 measurements used an explicit N16/Q64 override.

## Admission and resource lifetime

| Limit | What it bounds |
| --- | --- |
| `max_admitted_requests` (Q) | Ordinary retained logical reads. A separate preparation pool of the same size covers windows before admission and eligible packets that transfer preparation permits. Together the pools bound retained scheduler descriptors by 2Q. |
| `max_batch_requests` (B) | The logical receive budget per dequeue pass, capped by Q. A packet that does not fit the remaining budget is deferred intact. Ready requests from multiple passes can enter the same GET. |
| `max_inflight_reads` (P) | Concurrent scheduled physical reads. A group holds one slot across its retry loop. |

A complete window of two or more surviving cold reads first tries to take
all its ordinary Q permits together, up to `min(B, Q)` reads. On failure it
can transfer its preparation permits if the ranges form one contiguous or
overlapping span in one object under the byte cap. The fallback takes the
available prefix under ordinary Q, publishes it, and waits for the tail.
If the preparation reservation was rejected, the reads use ordinary
admission. If the deadline fired, the staged reads use prefix admission and
late members submit individually. A timeout of zero disables explicit
window preparation.

The prefix relinquishes preparation permits as soon as it obtains Q and is
ready to publish. The unpublished tail keeps its reservation until admission
or cancellation. The driver releases unused reservations as soon as
preparation closes. Preparation permits
transferred to an admitted packet live with its requests through completion
or cancellation.
[Window admission](https://github.com/risingwavelabs/risingwave/blob/4549066c84066448793e134ea404945e91d40c07/src/object_store/src/object/read_scheduler.rs#L274-L447)

Ordinary reads dispatch whenever their ranges are ready and physical capacity
is available. The worker collects an available batch without waiting to fill
it, then chooses the group containing the oldest ready request. Positive gaps
end a group. Oversized and unbounded ranges use the direct path. Cancellation
removes queued members and can cancel a physical read after all its waiters leave. Range-error
fallback releases the parent physical slot before issuing individual
children under the same physical limit.
[Dispatch and range grouping](https://github.com/risingwavelabs/risingwave/blob/4549066c84066448793e134ea404945e91d40c07/src/object_store/src/object/read_scheduler.rs#L451-L689)

## Q18 stage assessment

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

The measurements establish aggregate merge behavior. To distinguish
cache effects from scheduler splits for a particular window, correlate
selected blocks, external reads, admitted packets, and physical groups by
request identity. Gap observations describe the worker's ready set and still
need a combined-span check to estimate eligible merges and extra bytes.
The [Q18 repeat](./read-optimization-testing.md#q18-q64-io-distribution-repeat)
records the I/O distributions and their measurement boundaries.
The [checkpoint artifacts](./read-optimization-evidence/q18-checkpoint.md) retain the
aggregate data, query, and worker configuration.
