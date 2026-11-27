# Hummock read scheduling

## Object-store scheduler

`read_scheduler.rs` owns admission and pending groups;
`read_scheduler/execution.rs` owns physical dispatch, cancellation, optional
in-flight reuse, range-error fallback, and response delivery. Every ordinary `store.read()`
uses the scheduler when Hummock read coalescing is enabled on a supported
backend (native S3, in-memory, or OpenDAL S3/MinIO).

| Control | Meaning and unchanged default |
| --- | --- |
| `max_admitted_groups` (G) | Groups from admission through completion; 64 |
| `max_batch_requests` | Logical requests per group; 16 |
| `max_inflight_reads` (P) | Concurrent scheduled backend reads; 32 |
| `max_read_bytes` | Maximum byte span for coalescing; 256 KiB |
| `max_gap_bytes` | Maximum gap between neighboring ranges; 0 |
| `reuse_inflight_reads` | Share already covering active GETs; false |
| `use_dedicated_io_runtime` | Run dispatch, physical reads and range-error fallback on the shared IO runtime; false |
| `dedicated_io_runtime_workers` | Workers when the dedicated runtime is enabled; 2 |

The scheduler cap is independent of the scan window cap. A larger input range
keeps its original bounds and uses one G credit and one P slot. Open-ended reads
such as SST metadata fetches (`meta_offset..`) and whole-object reads use the same
queue. These standalone reads are dispatched individually. The `route` label is
`scheduled` for mergeable ranges, `scheduled_oversized` for larger ranges, and
`scheduled_other` for other bounds. The separate `streaming_read()` API returns a
reader and retains its existing dispatch path.

A compatible request may join a pending group even at full G. Waiting requests
can be absorbed into a compatible group without waiting to retry admission.
A bridging request can merge pending groups and return a credit. Physical tasks
select the oldest group when first polled, leaving it mergeable during task
handoff. Once selected, it cannot absorb new non-covered requests. There is no
extra delay to wait for potential neighbors.

Covered in-flight followers use no extra G or P; each physical read accepts up
to `max_batch_requests` additional followers. A read survives while an original
request or follower remains. Fully cancelled groups and reads release resources.
A merged-range error releases P before individual retries acquire P under the
same limit; the group credit lasts through delivery or cancellation.

G does not bound callers waiting for admission or total request memory. Queue
and waiting gauges must be interpreted separately. Positive gap limits trade
extra bytes for fewer GETs; merged span and member limits still apply. The
compactor's streaming block reader is separate from scan prefetch and is retained.
