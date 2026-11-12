# Q18 cold-window request trace

One traced TPC-H SF1 Q18 run used the same Hummock version 392 cold seed,
[SQL](./q18.sql), and [Q64 config](./q18-q64.toml) as the checkpoint. The
guarded `risedev d full` run used MinIO, one 2 GiB CN and four-way streaming
parallelism. It completed and matched the independent 100-row result.
The build used checkpoint `658df66c87` and added DEBUG logging of request
IDs on successful window packet publication. Scheduler policy was identical.

## Completed 16-block windows

The [analyzer](./analyze-window-identity.py) joins staged Foyer outer reads
to admitted packets and successful physical groups by request ID. A match
requires the outer read and completion trace for every member, contiguous
ranges on one SST within the 1 MiB cap, and a successful physical group
without range fallback.

| Windows | Foyer outer reads staged per window | Admitted packets | Successful GETs touching the window |
| ---: | ---: | --- | ---: |
| 770 | 16 | One packet of 16 | 1 |

All 770 were LocalityProvider windows. No completed, fully cold 16-block
window was unmatched or split. Of the other 16-block reservations seen by
the scheduler, 189 finished preparation with 14 or 15 surviving outer reads,
and 41 hit the preparation deadline. These counts cover reservations that
started a preparation driver. The fully ready cohort requires 16 staged
outer reads and completed preparation.

The 189 completed reservations with fewer staged reads still entered the
scheduler as one packet per window and used one successful GET per window.
Across these windows, 201 selected block slots had no staged outer read.
The trace does not assign each missing slot a Foyer outcome or exclude a
later ordinary read for that slot.

All 41 timed-out reservations submitted the reads already staged in one
packet each. Of those staged reads, 37 windows used one GET, three used two,
and one used three. These counts exclude late reads that can enter the
scheduler individually after the deadline.

## The observed admission split

The only admission split in the full-run metrics was a three-block
LocalityProvider window on SST 2474. At the 1 ms deadline, two requests had
staged. They entered through prefix admission as two single-request packets.
The head also used a separate read. The resulting GETs were:

| Block | Outer-read ID | Admitted packet | Physical range |
| --- | ---: | --- | --- |
| 1 (head) | 71260 | Ordinary | `65438..130846` |
| 2 | 71245 | `[71245]` | `130846..196305` |
| 3 | 71254 | `[71254]` | `196305..458113` |

The last GET also covered requests from outside this window. In particular,
another LocalityProvider window on the same SST finished preparation with
two cold members, `[71259, 71265]`. It used the existing preparation permits
to enter intact and both members shared that GET.

| CN counters | Through source checkpoint | After source checkpoint | Full |
| --- | ---: | ---: | ---: |
| Successful physical GETs | 44,154 | 8,916 | 53,070 |
| Admission splits | 0 | 1 | 1 |
| Windows admitted with preparation permits | 0 | 1 | 1 |
| Window preparation timeouts | 11 | 57 | 68 |

The source checkpoint is the first five-second scrape at 13,652,430 source
rows. It contains some overlapping downstream work. The 464.659-second
traced run is diagnostic and its wall time is unsuitable for comparison
with untraced runs. [Structured results, trace identities and example](./q18-window-identity.json).

## Source scan read path

The source snapshot scan requests Range prefetch. The LocalityProvider
snapshot requests Blockwise under the Q64 config. Both paths ran in this
query and both could send eligible reads to the scheduler.

Joining physical GETs to all their logical members by request ID matched
44,006 successful scheduled GETs through the source checkpoint. Of the
41,230 GETs at most 64 KiB, 41,124 consisted exclusively of per-block reads
under `StreamScan`. The phase cutoff uses the before-scrape file time plus
60.503 seconds and matches the phase scheduler success counter.
The source tables also reported 1,757 Range prefetch operations covering
12,715 blocks by that checkpoint.

The trace identifies where the small requests were created. It does not
record whether an individual source per-block read followed clipping to
the last block of an SST iterator or the Range prefetch memory-budget
fallback. Both paths use the same block-read trace. The SST metadata
comparison below resolves the dominant cause.

### Same-SST gaps among small StreamScan reads

All 41,124 StreamScan GETs at most 64 KiB had one logical request each.
The [replay script](./analyze-source-gaps.py) rebuilt the worker's ready
set from dequeue and selection events and checked its size at each
selection. A pair qualifies when both original GETs belong to the same
SST and all their members are already dequeued when the first GET is
selected. The combined range must fit the existing 1 MiB limit.

| Maximum gap | Eligible pairs, cumulative | GETs saved | Extra bytes |
| ---: | ---: | ---: | ---: |
| 32 KiB | 0 | 0 | 0 |
| 64 KiB | 2 | 2 | 130,636 |
| 128 KiB | 8 | 8 | 915,467 |
| 256 KiB | 51 | 51 | 11,907,137 |
| 512 KiB | 122 | 98 | 31,270,782 |

The estimate replaces whole original GETs without waiting, follows the
recorded selection order, and counts each original GET once per threshold.
Extra bytes are the gaps covered by the resulting merged ranges. The
64 KiB cases cross gaps of 65,273 and 65,363 bytes. The estimate holds
request timing fixed and covers only pairs of small StreamScan GETs.
[Replay data and request IDs](./q18-source-small-get-gaps.json).

### Adjacent blocks and request arrival

The same 41,124 single-request GETs covered 119 SSTs. Requests were joined
by SST ID and consecutive block index, and byte offsets had to touch.
Repeated reads of a block count as distinct requests. The table assigns
each GET once, with the strongest matching evidence first.

| Evidence of an adjacent small GET in the same SST | GETs | Share |
| --- | ---: | ---: |
| Same StreamScan actor and executor, with the second submission after the first physical GET completed | 40,471 | 98.41% |
| An adjacent GET was observed, with no pair satisfying the condition above | 567 | 1.38% |
| No adjacent small GET in the phase | 86 | 0.21% |

Consecutive small GET selections switched SST in 39,676 of 41,123
transitions. At selection, 40,317 of 41,124 GETs had no other small GET
from the same SST in the worker's ready set. The 807 with a same-SST
peer had no adjacent ready peer. Across the full phase, all 71,683
observed adjacent request pairs reached the worker in different
selection intervals. In 71,601 pairs the second logical submission
followed completion of the first physical GET. The trace identifies the
actor and executor but has no iterator ID.

For 38,863 GETs with a later adjacent GET, the median time from the
first selection to the earliest later adjacent submission was 562 ms.
Only 128 had that next submission within 10 ms of selection, including
50 whose next request was submitted before selection but dequeued later.

### Range prefetch buffer and scan boundaries

The cold seed retained the SSTs for all 41,124 small StreamScan GETs.
An offline footer decode matched all 119 SSTs and each traced block's
offset and length. The source scans used full PK ranges per vnode. If
the next block's smallest key has the same table and vnode as the
current block's smallest key, the scan bound still includes that next
block. For the 37,063 matching GETs, the iterator was eligible for Range
prefetch. This includes 18,919 GETs with at least 16 blocks remaining
in the same table and vnode.

| Classification by the scan bound | GETs | Share |
| --- | ---: | ---: |
| Next block stays within the scan bound | 37,063 | 90.13% |
| End of SST or table, or next vnode belongs to another actor | 2,353 | 5.72% |
| Crosses a vnode boundary with insufficient request context to decide | 1,708 | 4.15% |

The boundary count includes 294 SST or table tails and 2,059 vnode
tails identified using actor ownership observed in the same trace. The
Range path awaits an ordinary block get when the shared prefetch buffer
is over capacity. This run's 2 GiB CN derives a 172 MiB capacity with
the default memory configuration. Successful Range reads use a different
trace path, and no Range prefetch error was logged. The iterator advances
only after the demand block returns and its rows are consumed. Together
the call path and the reconstructed bounds identify the over-capacity
fallback for the non-tail reads. The archived scrapes do not record
per-request buffer occupancy.

### Source StreamScan with Blockwise prefetch

The trial used the same cold seed, Q18 SQL, traced dev profile and
[Q64 configuration](./q18-q64.toml) as the source Range baseline above.
The loaded TOML and both EXPLAIN outputs matched byte for byte.
The experimental build changed the source snapshot scan in
`make_snapshot_stream` to use `PrefetchMode::Blockwise` while preserving
the other fields of `PrefetchOptions::prefetch_for_large_range_scan()`.
The 100-row oracle passed. The change was reverted after freezing the
trial binary.

| Scope and metric | Source Range | Source Blockwise | Change |
| --- | ---: | ---: | ---: |
| Source phase StreamScan GETs at most 64 KiB | 41,124 | 4,308 | −89.5% |
| Source phase all CN GETs | 44,154 | 11,603 | −73.7% |
| Source phase CN read bytes | 3,714,206,916 | 3,414,299,022 | −8.1% |
| Source phase elapsed seconds | 60.503 | 45.278 | −25.2% |
| Source phase sampled peak CN RSS, GiB | 2.40 | 3.64 | +52.0% |
| Full Q18 CN GETs | 53,070 | 21,012 | −60.4% |
| Full Q18 CN read bytes | 6,180,254,395 | 5,903,347,632 | −4.5% |
| Full Q18 elapsed seconds | 464.659 | 393.037 | −15.4% |
| Full Q18 sampled peak CN RSS, GiB | 2.48 | 4.09 | +65.1% |

The source checkpoint is the first five-second scrape at 13,652,430
source rows. The small GET row counts successful scheduled GETs whose
members were all StreamScan block reads. The candidate emitted 6,504
StreamScan Blockwise windows through the checkpoint. Trace group totals
reconciled with the scheduler counters in both runs. This is one traced
run per mode. [Structured results and build identities](./q18-source-blockwise.json).
