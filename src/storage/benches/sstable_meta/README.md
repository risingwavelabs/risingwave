# SST metadata CPU and allocation benchmark

This registered RW microbenchmark uses actual `SstableBuilder`, `SstableMeta`,
`Sstable` and Foyer `Code::decode`, with no copied wire structs or filter algorithm.
It compares main `a5a57ad` with serde_bytes fix `5cf5dfd`; both have block-key reuse.
See the completed [main A/B results](RESULTS.md).

## TODO

- [x] Use complete real Hummock SST fixtures; verify native metadata, positive filter lookups,
  actual SST serde/filter round-trip, and identical meta-cache bytes in both implementations.
- [x] Separate normal release timing binaries from allocation-instrumented binaries.
- [x] Measure per-entry wall time and actual calling-thread CPU time, excluding output drop.
- [x] Count only the measurement thread's allocation requests; check thread isolation,
  realloc accounting, and complete deallocation inside the measurement scope.
- [x] Run fixed 64-iteration batches, seven per variant, alternating AB/BA order.
- [x] Retain source/binary/fixture fingerprints, raw CSV and per-round text evidence.
- [x] Review the result and update the PR benchmark table with the main A/B measurements.

## Run

```sh
python3 src/storage/benches/sstable_meta/run.py --build
# Wait for a quiet host: no Cargo or Docker builds/other benchmark processes.
python3 src/storage/benches/sstable_meta/run.py --measure --check-only
python3 src/storage/benches/sstable_meta/run.py --measure
```

Build and measure are intentionally separate. The runner only replaces the SST
module after verifying it is exactly the reviewed PR version; it restores it in
`finally`. The same benchmark source and lockfile are used for both libraries.
The benchmark is registered in `src/storage/Cargo.toml`, so it can also be run with
`cargo bench -p risingwave_storage --bench bench_sstable_meta --features test`.
The runner records every child with `wait4`, including process CPU, max RSS,
faults and context switches. Process CPU includes setup and is **not** used to
derive per-entry CPU. Per-entry CPU uses `CLOCK_THREAD_CPUTIME_ID` directly.

Fixtures are generated outside measurement with 64 KiB blocks, 16-byte values,
blocked XOR16 filters, and 1.1M/1.3M 64-byte keys or 160K 4096-byte keys. Existing
complete SSTs can be placed in the fixture directory to reuse identical bytes.
No cache warming or fixture construction is included in the measured operation.
Each measurement includes XXHash64, actual `Box<Sstable>` decoding/filter reconstruction
(matching `SstableStore`'s Foyer value type), and cache-key decoding. The returned
SST's final drop is outside the timer.

## Allocation scope and limitations

The timing binary is compiled **without** counter/TLS updates, verified through
its symbols. Compile the same bench with `RW_META_BENCH_ALLOC=1` for a separate
resource-only pass. Its thread-local allocator counter captures Rust `GlobalAlloc`
calls only between decode start and output drop. Inputs, fixture generation,
printing, and allocations from other threads are excluded.

`alloc_calls` includes alloc/alloc_zeroed/realloc; `realloc_calls` is its subset.
`requested_bytes` sums the requested size of every successful allocation or
reallocation. `peak_live_bytes` is the maximum live requested bytes within one
decode; realloc resizes the tracked allocation rather than assuming its allocator
internals temporarily retain both buffers. `returned_live_bytes` is live bytes
after decode and before drop. Every entry must return to zero after drop.

These measure **requested Rust bytes**, not malloc arena size, allocator-internal
transient copies, allocations bypassing Rust, or process RSS. Both variants use
`System` on the same host; production's allocator and ARM EC2 CPU may differ.
Allocation-pass elapsed time is deliberately not reported as decode latency.
This is warm-input CPU work, excluding page cache, EBS, scheduling and cache lookup.
Generated workloads do not establish the customer's precise data distribution.

Raw evidence is kept in `target/sstable-meta-main-ab`: all four binaries, compiler
and source snapshots, `build.json`, fixture hashes, per-batch CSV and text records,
`measure.json`, and a summary table. Do not compare only terminal output.
