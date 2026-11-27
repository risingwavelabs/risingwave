# Firestore sink validation

Creating a Firestore sink requires a valid RisingWave license that enables the
`FirestoreSink` feature. This applies to all decimal modes and is separate from
the Google Firestore Enterprise edition requirement for Decimal128.

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
errors by default; a failed first chunk cannot truncate the log or send the following chunk.
With `firestore.skip_invalid_rows=true`, only invalid data is skipped. Permission,
quota, unavailable-service, and incomplete-ACK errors still prevent log truncation.

Encoding and validation tests also run in deterministic simulation:

```sh
CARGO_TARGET_DIR=target/sim cargo test -p risingwave_connector --lib --no-default-features --locked \
  --config "target.'cfg(all())'.rustflags = ['--cfg=madsim']" sink::firestore
```

The real gRPC mock and official emulator tests are compiled only without `madsim`.
The Firestore SDK uses real HTTP/2 sockets, and the simulation's replacement for
`tonic` does not provide the gRPC server API used by the mock.

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
  sink::firestore::tests::grpc::emulator_ -- --ignored --nocapture
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

## Native types and subcollections

`TIMESTAMPTZ` is written as a native Firestore timestamp. `TIMESTAMP` is interpreted
as UTC, independent of the session or process time zone. Submicrosecond precision
is truncated; years outside 0001–9999 are invalid rows. NULL remains native NULL,
including in lists and structs.

`DECIMAL` defaults to native Decimal128, including NaN and infinities. It preserves
all significant digits of a RisingWave decimal without an intermediate double,
also inside lists and structs. Firestore Enterprise edition supports Decimal128;
Standard edition does not. Use `firestore.decimal_mode` to select the encoding:

| Value | Behavior |
| --- | --- |
| `decimal128` (default) | Preserve decimal precision; requires Enterprise edition. |
| `double` | Opt into 64-bit floating point, which can round decimal values. Works with Standard edition and the pinned emulator. |

For example, Standard edition users can set
`firestore.decimal_mode = 'double'` in the sink's WITH options. The value
`9007199254740993` remains exact with Decimal128 but becomes `9007199254740992`
with double. Cast the column to `VARCHAR` in the sink query if exact text storage
is preferred on Standard edition.

When the sink schema contains decimals and the mode is `decimal128`, sink
validation and writer startup query the target database's `databaseEdition` using
Firestore Admin `GetDatabase`. A known Standard database is rejected with an
actionable configuration error, before writing rows, even with
`firestore.skip_invalid_rows=true`. The sink never automatically switches to
double. Metadata requires `datastore.databases.getMetadata`; Datastore User
includes it, but a custom write-only role might not. Permission-denied or
unimplemented metadata requests (including emulators without the Admin API), and
unspecified or unknown edition values, produce a warning and keep Decimal128.
Other failures, including authentication, missing database, transport errors and
the 10-second metadata timeout, fail startup. Explicit `double` and schemas
without decimals do not need the metadata request.

The Decimal128 wire encoding follows the official
[Firestore BSON type mapping](https://cloud.google.com/firestore/mongodb-compatibility/docs/supported-data-types-drivers):
a reserved `__decimal128__` map containing the exact decimal text. This denotes a
native scalar, rather than an ordinary string field. Unit and gRPC tests cover
this representation and edition detection; the pinned Standard-style emulator
tests exercise the explicit double mode. A live Enterprise smoke test is still
needed to verify native Decimal128 query behavior against the service.

Parent document segments in `firestore.collection` may reference sink columns:

```sql
CREATE SINK child_sink FROM child_view WITH (
    connector = 'firestore',
    type = 'upsert',
    primary_key = 'parent_id,child_id',
    firestore.project_id = 'my-project',
    firestore.collection = 'parents/{parent_id}/children',
    firestore.document_id_column = 'child_id',
    firestore.skip_invalid_rows = 'true'
);
```

Each row writes to `parents/<parent_id>/children/<child_id>`. Placeholders must
occupy a whole parent document segment and reference a column in the sink schema.
Multiple levels are supported, for example
`tenants/{tenant_id}/parents/{parent_id}/children`. Values must be non-NULL, valid
single Firestore IDs; `/` cannot inject another path segment. Static nested paths
continue to work. Include the parent columns in the sink primary key so that
children with the same ID under different parents remain independent. A parent
change uses the old row to delete the old path and the new row to insert the new
path in upsert mode. Append-only mode retains the old document.

## Invalid row policy

The sink rejects `MAP`, `VECTOR`, `VARIANT`, and directly nested SQL array column
types at creation, including inside structs and lists. Errors identify the field
path. `JSONB` contents are validated per row.

`firestore.skip_invalid_rows` defaults to `false`, preserving failure/replay.
Opting in permanently skips invalid mutations and logs a warning. It covers
invalid/empty/NULL document or parent IDs, unsupported row values, excessive
nesting, out-of-range timestamps, oversized documents, and per-write
`INVALID_ARGUMENT`/`OUT_OF_RANGE` responses. If an entire RPC is rejected for
invalid data, the sink isolates the writes and preserves healthy documents.
Requests are limited by both write count and bytes to stay below the 10 MiB RPC
limit.

Authentication, permission, quota, transport, and transient errors are never
skipped. Neither are missing or inconsistent batch statuses. Healthy writes must
still be acknowledged before log truncation. Skipped updates can leave an older
valid document in Firestore; this mode favors continued progress over a complete
mirror and has no dead-letter queue. Once skipped data is corrected, a new source
mutation delivers it normally. Deleting a row with an unusable ID is also skipped.

The emulator suite additionally reads back native timestamps and decimal numbers,
writes the same child ID under two parents, moves a child, mixes healthy rows with
NULL/empty/invalid IDs and a document over 1 MiB, and checks continued delivery.
It also sends a deliberately oversized write directly to exercise server rejection
and isolation alongside a healthy write. End-to-end SQL and crash-recovery testing
against a real RisingWave deployment is still needed.
