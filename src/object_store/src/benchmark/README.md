# Object-store read diagnostics

Add `--trace-output reads.jsonl` to `risectl bench object-store` to record each
compactor/interference stream await. The destination must not exist, its parent
must exist, and it must differ from `--output`. Tracing is disabled by default.

```sh
risectl bench object-store --store s3://benchmark-bucket \
  --profile compactor --objects 3 --concurrency 1 --object-size 4MiB \
  --pause-ms 10000 --output report.json --trace-output reads.jsonl
```

The JSONL format is version 1 (recorded in the report's `read_trace` section):

- `read_start` associates a unique `request_id` with phase, object key, reader ID,
  mode and requested byte range. Repeated requests by the same reader have
  different request IDs.
- `open`, `next`, `pause`, `readiness` and `read_end` record spans. Each contains
  `started_unix_us`, monotonic `elapsed_us`, absolute object `offset`, `bytes`,
  and `status`. A `next` offset is the position before consuming that item.
- Every completed `next` await is recorded, including empty items, EOF, errors
  and cancellation. A terminal `read_end` distinguishes complete, failed and
  interrupted logical reads. Interrupted awaits do not imply an SDK failure.
- Intentional pauses and readiness waits are separate spans. Histogram
  `first_poll`/`resume_poll` samples include only nonempty successful reads;
  interference histograms exclude readiness, but the trace retains it.

Rows are emitted on span completion, so concurrent readers' timestamps need not
be in sorted order. Join by `request_id` to reconstruct a read. For example:

```sh
jq -c 'select(.event == "next" and .elapsed_us >= 1000000)' reads.jsonl
```

A bounded queue sends records to a background file writer, flushed about once a
second and on completion. The writer never reads the object response. Await
latency is sampled before enqueueing its record, but tracing still has CPU and
scheduling overhead. Check `read_trace.complete`, `records_written`, and
`records_dropped` before treating the file as complete; queue overflow or file
write failure makes the CLI exit unsuccessfully after emitting the benchmark
report. Early setup errors may leave an empty trace file; existing files are
never truncated. Abrupt process termination can lose buffered records.

No object payloads, credentials, or SDK error strings are recorded. Error details
remain in the redacted benchmark report. These are client awaits, not individual
synchronous `poll_next` calls, TCP packets, or physical GET/retry events. Basic
profile operations are not included. Capture OpenDAL retry logs separately when
investigating whether a slow await contains an SDK retry.

## Consumption pacing

Continuous mode always drains without intentional pauses. Intermittent and
chunked modes share these options (including their interference reads):

| CLI option / JSON field | Default | Meaning |
| --- | --- | --- |
| `--pause-every-chunks` / `pause_every_chunks` | `1` | Pause after every N nonempty chunks, while data remains. Must be positive. |
| `--max-pauses` / `max_pauses` | unlimited / `null` | Maximum pause opportunities per logical read, then drain continuously. `0` disables pauses. |
| `--pause-ms` / `pause_ms` | `10000` | Base duration of each eligible pause, in milliseconds. |

Empty stream items do not advance the chunk count. The entire returned `Bytes`
is consumed and dropped before pausing. No pause follows the final data item or
EOF. Chunk sizes depend on the transport; in chunked mode each complete short GET
counts as one chunk. The pause limit resets for each logical read, not each short
GET or SDK retry. A zero-duration opportunity still counts toward that limit.

Existing `jitter_ms`, `long_pause_every`, and `long_pause_ms` still apply: duration
is base + deterministic per-reader jitter + any long-pause addition. Long pauses
retain their original **chunk-index** schedule, but only eligible pause boundaries
can sleep. For example, every 2 chunks with a limit of 1 pauses after chunk 2 only.
Old parameter files retain the original per-chunk, unlimited, independent pacing.

Each reader follows its own pause schedule. Fixed-work phases keep their existing
worker scheduling, and interference phases retain their initial readiness and
fixed observation window. No synchronization occurs at pause points.

Examples of pacing options (append to a normal benchmark invocation):

```sh
# Existing behavior: independent pause after every nonfinal chunk.
--pause-ms 45000

# Independent one-time pause, followed by continuous draining.
--pause-ms 45000 --max-pauses 1

# Repeated bursts of 16 chunks, at most 3 pauses per logical read.
--pause-every-chunks 16 --pause-ms 1000 --max-pauses 3
```

For the paused-read investigation, a complete invocation can use:

```sh
risectl bench object-store --store s3://benchmark-bucket \
  --profile compactor --objects 1002 --concurrency 1000 --object-size 4MiB \
  --pause-ms 45000 --max-pauses 1 \
  --output report.json --trace-output reads.jsonl
```

The full compactor profile needs distinct objects for the two default fast readers,
hence 1002 objects. Workers independently take subsequent objects after completing
a read. Configure
`storage.object_store.retry.streaming_read_attempt_timeout_ms = 30000` and
`streaming_read_retry_attempts = 10` in the RW config passed via `--config-path`.
The per-next SDK/RW timeout is separate from `--max-runtime-secs`; intentional
sleeps do not consume a per-next timeout. Set the overall budget for all phases.
A published Docker port and a direct endpoint can behave differently; record the
topology and effective backend/version. These options express the reproduction
workload; observing TCP pressure or a long await still depends on the environment.
