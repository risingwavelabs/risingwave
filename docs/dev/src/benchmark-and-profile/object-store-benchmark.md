# Object store benchmark

`risectl bench object-store` measures S3-compatible storage with RisingWave's
object-store clients. It needs no Meta connection or running RW cluster. It can
run in an existing component container, a standalone container, or on a host
with access to the storage endpoint. It does not discover Kubernetes Secrets or
copy configuration from other Pods.

## Configuration and a complete run

Use the deployment's credential provider and existing environment, including
`AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_SESSION_TOKEN` when applicable,
`AWS_REGION`, `RW_S3_ENDPOINT`, and `RW_IS_FORCE_PATH_STYLE=true` for path-style
MinIO. `--store s3://bucket` overrides `RW_HUMMOCK_URL`; the latter can include
the usual `hummock+` prefix. Legacy `minio://` URLs are also accepted.

```sh
risectl bench object-store \
  --store s3://benchmark-bucket \
  --config-path /etc/risingwave.toml \
  --profile all \
  --output /tmp/object-store-result.json
```

The bucket must already exist. Each run creates a unique
`risectl-bench/<uuid>/` prefix, and only deletes the explicitly known test keys.
The full profile needs object PUT/GET/LIST/DELETE and multipart permissions;
it does not require bucket creation or deletion. It tests connectivity through
actual selected operations. Authentication, authorization, and network errors
are reported in the failing phase, rather than inferred from a single HEAD.

`--config-path` reads `[storage.object_store]` from the normal RW TOML schema.
Without it, defaults are used and this is recorded. Endpoint, credentials,
addressing, proxies, and trust files must be available in the executing process.
The benchmark does not silently read settings from Meta. CLI tuning overrides
the optional JSON `--parameters` file; omitted fields use documented defaults
from `BenchmarkConfig`.

`--backend opendal|aws` explicitly overrides the configured backend. A PEM
`--ca-cert` bundle is added to this diagnostic client's native trust roots;
otherwise `SSL_CERT_FILE`, then `AWS_CA_BUNDLE`, supplies the bundle. TLS
verification remains enabled. Failed OpenDAL writes enable abort-on-error for
this run, and this override is recorded. Other retry and timeout settings are
inherited. An OpenDAL concurrency limit below the requested mixed experiment's
fast-plus-competitor count is rejected to avoid a readiness deadlock.

## Workloads

`basic` covers the five workload types used by Console's WARP profile, using
RW clients rather than invoking WARP:

| Phase | Work |
| --- | --- |
| `multipart_put` | Concurrent streaming writes, timed through `finish()` |
| `range_get` | Ranged GETs at deterministic offsets |
| `block_get` | Small random ranged GETs modelling cache-miss block fetches |
| `list` | Concurrent complete scans of the test metadata prefix |
| `batch_delete` | Disjoint batches of existing test metadata objects |

Defaults are 16 objects of 64 MiB, 8 workers, 1 MiB ranged reads, 64 KiB block
reads, 16 read iterations per object, 1000 metadata objects of 4 KiB, and delete
batches of 100. The CLI requires the large object to exceed the configured
multipart part size (and 5 MiB). Test multipart behavior against the chosen
backend; a streaming API alone does not prove the wire-level part layout.

One deterministic random payload is generated before PUT timing and shared
between workers. Payload memory is one object, in addition to per-worker and
SDK buffers. Dataset preparation is separate from read/list/delete timing.
Each existing metadata key is submitted to measured DELETE exactly once.
Preparation and final cleanup appear separately from the measured workloads.

`compactor` uses the same large-object range plan for three fixed-work modes:

* `continuous`: consume a large streaming response as quickly as possible.
* `intermittent`: consume B bytes, pause, then continue the same logical reader.
* `chunked`: download a short range completely, consume its in-memory bytes at
  the same B/pause cadence, then fetch the next short range.

B defaults to 256 KiB, the pause to 10 ms, and short GETs to 4 MiB. B is **not**
an HTTP range or SDK chunk size. No background body-draining task is introduced.
`--jitter-ms`, `--long-pause-every`, and `--long-pause-ms` add deterministic
per-reader jitter and occasional long pauses. There is no sleep after the last
consumption unit. Objects must cover `fast_readers + concurrency`, so the mixed
cohorts use distinct objects.

The interference experiment holds the fast cohort fixed and runs three cases:

1. Fast readers only.
2. The same fast readers plus continuously consuming large-range competitors.
3. The same fast readers plus intermittently consuming large-range competitors.

Competitors receive their first byte before the common window starts; fast
readers then run for the entire window, not just until a finite dataset ends.
The default window is 10 seconds and the fast cohort has 2 readers. Statistics
are separate for each cohort. Administrative interruption at the window end is
not an I/O failure or a completed-request latency sample. Drain time is reported
separately and excluded from window throughput.

## Repeats and retained data

`--keep-data` retains the large objects. Measured batch DELETE still removes its
own metadata dataset. Reuse the returned prefix for a reordered compactor run:

```json
{
  "profile": "compactor",
  "objects": 16,
  "object_size": 67108864,
  "read_order": ["chunked", "intermittent", "continuous"]
}
```

```sh
risectl bench object-store --store s3://benchmark-bucket \
  --profile compactor --keep-data --output /tmp/first.json

# Copy the exact benchmark.prefix from first.json. Match objects and object_size.
risectl bench object-store --store s3://benchmark-bucket \
  --parameters /tmp/reordered.json \
  --reuse-prefix risectl-bench/RUN_UUID/ --output /tmp/reordered-result.json
```

Reused objects are size-validated, not overwritten, and retained afterwards.
No test scans production SST paths. Retained objects can be removed using the
storage administrator's normal tooling against that exact test prefix.

Cache state is uncontrolled. Newly uploaded data and a first run are not proof
of cold caches. Record experiment order and repeat conditions. A single command
does not sweep the full range/concurrency/pause parameter matrix.

## Result semantics

Progress and RW logs go to stderr; stdout contains one final JSON document,
optionally also saved with `--output`. It records schema/profile version, build,
target, effective object-store configuration, workload configuration, prefix,
per-phase status and cleanup status. Credentials and PEM content are omitted.

Phase rates count logical operations, not raw HTTP requests. Multipart parts,
LIST pagination, metadata HEADs, and internal retries can issue extra requests.
LIST reports complete scans/sec and enumerated objects/sec; it may include RW's
extra metadata requests. Do not interpret this as pure S3 LIST request rate.
Observed outer retry events are separate from final failures and exclude
unobserved SDK/OpenDAL retries. These results require new baselines and cannot
be compared numerically to an old WARP baseline without calibration.

`peak_inflight_logical_operations` includes client initialization/queuing and
does not claim a count of active server-side GETs. `planned_dataset_bytes` is
the intended dataset size, not proof that setup completed. Skipped workloads
are explicit when prerequisites fail or cancellation prevents their execution.

Latency distributions pool worker histograms (three significant digits,
microsecond resolution), include sample counts, and do not average worker p99s.
Operation latency includes failures and intentional pauses. First-data latency
includes initialization and receipt of a byte; reader creation alone may be
lazy. Consumption latency measures acquiring a consumption unit; resume latency
measures acquiring its first byte after a pause. Both include client scheduling
and buffers and are not pure server latency. Streaming byte counts retain
observed progress on errors; failed buffered GET/PUT transfers can have
unobserved partial network progress.

Large-range tests and `max_stream_chunk_bytes` help inspect client buffering.
The report deliberately leaves `backpressure_verified=false`: verifying that
pressure reached the server requires correlated server/network observations.
A healthy local result does not establish a customer's historical root cause.

`--max-runtime-secs` defaults to 1800. Ctrl-C and the deadline stop new work and
interrupt reads/pauses cooperatively. In-flight writes finish or reach their
configured SDK timeout before cleanup. Cleanup ignores the cancellation token.
The JSON preserves partial results; the command exits nonzero for failed or
cancelled work or failed cleanup. Process kill and incomplete multipart uploads
may require server-side lifecycle cleanup; deleting object keys alone cannot
prove that all multipart parts were aborted. There is no automatic storage
performance pass/fail threshold.

## Validation

```sh
cargo test -p risingwave_object_store --lib benchmark::tests
cargo test -p risingwave_ctl --test object_store_benchmark

# Use disposable local MinIO and an existing test bucket; configure endpoint/auth.
RW_BENCH_TEST_STORE=s3://benchmark-bucket \
  cargo test -p risingwave_object_store --lib benchmark::tests::real_s3_profiles \
  -- --ignored --nocapture
```

Set `RW_BENCH_TEST_BACKEND=aws` to repeat with the native S3 backend. The ignored
test checks complete profiles, equal fixed-work bytes, paginated metadata
listing, and cleanup. Use MinIO request trace or a controlled S3 test server to
check actual multipart and ranged requests. Service-side backpressure and
interference require separate sufficiently large, long-running experiments.
