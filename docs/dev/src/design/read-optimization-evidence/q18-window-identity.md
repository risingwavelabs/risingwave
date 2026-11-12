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
