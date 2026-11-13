# Linux read scheduler replay

Run on Linux with an isolated MinIO server and a new empty bucket. The test writes
18 deterministic, uncompressed Hummock SSTs (about 580 MiB). It then runs
Blockwise scans and timed point reads through the real Foyer cache and object
scheduler. Use a disposable bucket. The fixture leaves its objects in the bucket
for local inspection. Use alphanumeric MinIO credentials for URL parsing.

```sh
export RW_REPLAY_MINIO_ADDR=127.0.0.1:9000
export RW_REPLAY_BUCKET=read-scheduler-replay
export RW_REPLAY_TRACE="$(mktemp)"
# Export MINIO_ROOT_USER and MINIO_ROOT_PASSWORD for the local server.
# Create the empty RW_REPLAY_BUCKET before running this command.
CARGO_PROFILE_RELEASE_SPLIT_DEBUGINFO=unpacked cargo test --locked --release \
  -p risingwave_storage --lib --features read-scheduler-replay \
  linux_read_scheduler_replay -- --ignored --nocapture
```

The ignored test checks every scanned key/value, point block keys, and row
counts. It reports the actual Tokio worker count, visible CPU count,
point p99, scan time, backend bytes and completed GET count. G is 128,
P is 32, and the scan uses N16/B16 with a 1 MiB limit and 100 µs post-seal
window. Depth samples report how often G128 has waiters and how often
32 scheduled physical reads are active. Saturation is an observation.

All TSV timestamps are relative to one monotonic clock. Sort numerically by
the first column after the header. Queue events record live groups in
column `a` and starting reservations in `b`. Permit events bracket
acquire polls, aborts and releases and record available P in `a`.
`backend_begin` and `attempt_begin` carry the physical permit ID.
Wake begin/forwarded events share an ID, as do dispatcher poll/exit
events. For repoll delay, use time after the prior exit and after the
wake returns and intersect with free P and live groups in excess of
starting reservations. A permit transition bracket can be ambiguous.
The recorder changes timing. Use it for stage attribution and retain
the printed worker count and CPU environment when comparing runs.
