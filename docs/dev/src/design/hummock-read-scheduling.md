# Hummock read scheduling

## Read path

Forward SST iterators use one prefetch implementation: ColdRanges. The old eager
Range implementation, per-block Blockwise registration, and window plug have been
removed. Ordinary point reads still use the existing Foyer get-or-fetch path.
No scheduler-specific Foyer patch is required; the dependency remains at `02d6b5e`.

```text
Operator / state-table scan
  -> HummockVersionReader: resolve caller policy and scan bounds
  -> SstableIterator: select a bounded ColdRanges window
     -> memory hit: retain the cached block
     -> disk-cache index hint: cache-only lookup, ordinary get on stale hint
     -> single cold Fill block: register ordinary Foyer get-or-fetch
     -> multi-block cold run: submit one bounded object read
        -> object scheduler, when enabled and eligible
           -> pending merge / optional covered in-flight reuse
           -> physical ObjectStore read
     -> consume blocks in order: decode and optionally fill Foyer on demand
```

Every run in the admitted window starts before consuming its first block. For
`cold cold hot cold cold`, both cold runs can be in flight concurrently. The
iterator does not wait for one run to finish before submitting the next. It keeps
the encoded result and decodes one block at a time on the consuming runtime.
There is no window timer, expected-candidate count, plug, or pressure heuristic.

A short prefix or last-block read with no usable neighbor bypasses the boxed
prefetch future. A range error on a multi-block read falls back to the demanded
block, which can still be valid if a neighbor lies past EOF. Object-wide errors
and failures decoding the demanded block propagate to the caller. Explicit seek,
rewind, exhaustion, error, and drop release the old window and cancel its private
read tasks. Foyer owns shared producers independently.

## Caller policies and configuration

The strategy is `cold_ranges` or `disabled`. Callers retain independent controls;
path-specific values override the unified `mode`.

| Path | Default | Override in `storage.scan_prefetch` |
| --- | --- | --- |
| Ordinary range scan, including Locality sort-buffer replay | ColdRanges | `range_scan_mode` |
| Snapshot Backfill source | ColdRanges | `snapshot_backfill_mode` |
| LocalityProvider snapshot | ColdRanges | `locality_backfill_mode` |
| Join / TemporalJoin / AsOfJoin / OverWindow prefix scan | Disabled | `prefix_mode` |
| Change-log scan | Disabled | `changelog_mode` |
| Point get | No prefetch | Not controlled by scan mode |

Existing range-scan callers now resolve to ColdRanges. The internal `RangeScan`
and `PrefixScan` names identify callers, not separate IO implementations. Scans
remain bounded by table ID and the requested end key before selecting neighbors.
A serial probe whose result fits in one block offers no within-iterator merging
opportunity. ColdRanges does not batch dependent probes from different keys.

For the explicit 16-block, 1 MiB test configuration:

```toml
[storage.scan_prefetch]
mode = "cold_ranges"
max_blocks = 16
max_bytes = 1048576

[storage.object_store]
enable_hummock_read_coalescing = true

[storage.object_store.read_scheduler]
max_read_bytes = 1048576
max_gap_bytes = 65536
use_dedicated_io_runtime = true
dedicated_io_runtime_workers = 2
```

The 64 KiB gap above is an explicit test choice, not a changed scheduler default.
Omit `mode` to keep each path's default, or set individual path modes for a targeted
comparison. Changes require worker restart. See also
[`cold-range-prefetch.toml`](../../../../src/config/cold-range-prefetch.toml).

`max_blocks` defaults to the existing `storage.max_prefetch_block_number` (16).
An explicit `scan_prefetch.max_blocks` overrides it; 1 disables lookahead.
`max_bytes` defaults to 1 MiB and bounds the sum of each selected block's larger
compressed/uncompressed size. Oversized individual blocks use ordinary get.

The production configuration uses only `storage.scan_prefetch`, with
`cold_ranges` and `disabled` modes. Experimental section names, `range`/`blockwise`
mode values and legacy boolean switches have no compatibility mapping. Use the
experiment branch to reproduce old configurations.

## Cache and memory ownership

`Fill` reuses cached blocks and inserts each directly fetched block only when
consumed. `NotFill` can reuse cache hits but owns new blocks without inserting
them. `Disable` skips cache inspection and reads the bounded window directly.
The disk index `contains` result is only a hint. Eviction or a collision falls
back to the ordinary cache/fetch path; it never creates a missing scan block.

The shared prefetch budget uses atomic reservations before starting IO. A window
charges retained encoded buffers, pinned cache results, concurrently completing
lookup/singleton results, one direct-decode peak, and bookkeeping. It does not
charge every block in a direct run as simultaneously decoded. When the desired
window does not fit, planning tries shorter prefixes before falling back to a
single demanded block. No IO or cache fetch starts for a rejected prefix.

The budget is shared by scan windows regardless of the caller's large-query
classification. Private tasks retain their reservation until cancellation is
observed. Returned blocks belong to the iterator consumer or Foyer; this budget
is not a process RSS limit or a bound on all ordinary shared cache producers.
Decoded blocks copy their own bytes so returning one block cannot retain the
whole raw range after the window ends.

A multi-block direct read does not register ownership for each block in Foyer.
A later point read or another range can therefore duplicate its bytes. The
scheduler's full-coverage in-flight reuse only helps while a covering physical
read is still active and that option is enabled. It cannot share completed
iterator-private buffers or join partially overlapping active ranges. This
limitation is deliberate; no new sharing layer is introduced here.

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

ColdRanges starts its wrapper tasks on the caller runtime. A read that needs a
new group waits for G in the submitting future. Its owner must keep polling it
or drop it to cancel. Foyer fetch tasks and ColdRanges read tasks continue polling
when an iterator pauses. Physical IO, dispatch and
range-error fallback use the configured scheduler runtime. Decode/fill stays on
the consuming runtime. Foyer keeps its configured runtime ownership.

## Observability and validation

ColdRanges keeps window-run, range-size, cache-gap, singleton, stale-hint,
budget-shrink and budget-denial metrics. Scheduler queue/admission/dispatch,
physical reads, coalescing, overread and reuse metrics remain. Blockwise/plug-only
metrics are removed. See the generated [metric inventory](../../../metrics/inventory.tsv).

Set `storage.scan_prefetch.observe_buffered_overlap = true` to track unconsumed buffers.
The default skips registering ranges and looking them up on demand reads.

The overlap observations are diagnostic, not ownership: ordinary block requests
can report collision with completed unconsumed buffers, and scheduled reads can
report in-flight partial overlaps. They do not count all repeated multi-block
reads of completed private buffers. Zero observations are not proof of zero
redundant bytes; in-flight observations also depend on reuse being enabled.

Regression tests cover policy/default selection, scan bounds, point-get isolation,
cache policies, disk-cache hints, singleton sharing, concurrent cold runs, lazy
decode, cancellation, error fallback, memory admission/shrinking, G/P limits,
pending merges, in-flight coverage and dedicated-runtime progress.
