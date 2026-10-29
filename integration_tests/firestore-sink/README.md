# Firestore sink validation

The normal connector tests run an in-process gRPC server using the real Firestore
SDK. No Google project, credentials, Python packages, or external service is needed:

```sh
cargo test -p risingwave_connector --lib --no-default-features sink::firestore
cargo test -p risingwave_connector --lib --no-default-features tests::test_with_options_yaml_up_to_date
```

The tests cover native non-finite floating-point values, integer boundaries,
preservation of nested NULLs, rejection of unsigned JSONB integers above i64::MAX,
unsupported nesting, configuration validation, bounded request concurrency,
draining concurrent requests on error, per-document ordering, and acknowledgement
before log truncation. Explicit batch failure and missing status entries are
errors; a failed first chunk cannot truncate the log or send the following chunk.

## Official emulator

Requires Java 17 or later, curl, ripgrep, and the normal Rust build prerequisites.
The runner downloads a pinned official emulator, starts it on a local port, runs
the ignored connector test, and stops only the process it started:

```sh
bash integration_tests/firestore-sink/run-emulator-test.sh
```

Set `FIRESTORE_EMULATOR_JAR` to reuse an existing emulator jar and
`FIRESTORE_EMULATOR_PORT` to change port 18080. Alternatively, start an emulator
yourself and run:

```sh
FIRESTORE_EMULATOR_HOST=127.0.0.1:8080 cargo test -p risingwave_connector --lib --no-default-features \
  sink::firestore::tests::emulator_crud_replay_and_native_values -- --ignored --nocapture
```

The connector honors the standard `FIRESTORE_EMULATOR_HOST=host:port` variable.
For this endpoint it uses the emulator owner token, without requesting ADC or
forwarding configured cloud credentials. Keep the variable unset for cloud use.

The test invokes the actual sink writer and reads raw Firestore documents. It
checks insert/update/delete, repeated delivery after recreating the writer,
document-ID changes, batch splitting, native NaN/Infinity, integer boundaries and
nested NULLs. Each run uses a unique collection in `demo-rw-firestore`.

`firestore.max_future_send_nums` bounds concurrent batch RPCs within a chunk.
Chunks finish in order; enabling concurrent chunks without document versioning
would allow older changes to overwrite newer ones. Requests within one chunk are
independent because the planner keeps only its final mutation for each document.

These are connector integration tests, not a full SQL/recovery test. Before a
production release, additionally run a real RisingWave deployment against the
emulator with a continuously updated table and a compute restart, reconcile all
keys, and perform a small cloud smoke test for IAM and actual service limits.
Firestore BatchWrite remains non-atomic and does not fence requests with unknown
outcomes across recovery; these changes do not provide exactly-once delivery.

References: [Firestore emulator](https://firebase.google.com/docs/emulator-suite/connect_firestore),
[Firestore limits](https://firebase.google.com/docs/firestore/quotas).
