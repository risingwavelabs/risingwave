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

Source snapshot backfill uses Range prefetch. LocalityProvider can select
Blockwise independently. HashJoin prefix scans have their own Blockwise
control, and change-log scans have a separate switch. Point MultiGet is a
separate state-store API. Those callers share the object scheduler whenever
their reads reach it.

## Admission and resource lifetime

| Limit | What it bounds |
| --- | --- |
| `max_admitted_requests` (Q) | Ordinary retained logical reads. A separate preparation pool of the same size covers windows before admission and eligible packets that transfer preparation permits. Together the pools bound retained scheduler descriptors by 2Q. |
| `max_batch_requests` (B) | The logical receive budget per dequeue pass, capped by Q. A packet that does not fit the remaining budget is deferred intact. Ready requests from multiple passes can enter the same GET. |
| `max_inflight_reads` (P) | Concurrent scheduled physical reads. A group holds one slot across its retry loop. |

A complete window can transfer preparation permits only if its cold requests
form a contiguous or overlapping span in one object, fit the byte limit, and
fit a packet. Otherwise, ordinary admission publishes the prefix that fits
and waits for capacity for the rest. The prefix relinquishes preparation
permits as soon as it obtains Q and is ready to publish. The unpublished tail
keeps its reservation until admission or cancellation. The driver releases
unused reservations as soon as preparation closes.

Ordinary reads dispatch whenever their ranges are ready and physical capacity
is available. Only an explicit window has a preparation deadline. Oversized
and unbounded ranges use the direct path. Cancellation removes queued members
and can cancel a physical read after all its waiters leave. Range-error
fallback releases the parent physical slot before issuing individual
children under the same physical limit.

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
