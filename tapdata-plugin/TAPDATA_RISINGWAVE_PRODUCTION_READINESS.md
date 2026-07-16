# TapData RisingWave Connector Historical Production Qualification

Status date: 2026-07-15 (historical record)
Branch: `wenym1/tapdata-plugin`
Baseline commit: `987970037a`
Historical release artifact: `tapdata-plugin/target/risingwave-connector-1.0.0.jar`

This document is the durable chronological execution record for an earlier production
qualification. It records commands, observations, failures, fixes, and remaining risks. It does
not certify the current branch or an unreleased artifact. The current development artifact is
`target/risingwave-connector-1.0-SNAPSHOT.jar`; before release, rerun Cloud and CI qualification
against the exact immutable artifact. Secrets and record payloads containing sensitive data must
not be recorded here.

Later local hardening, not fully reflected in the historical entries below, includes named
RisingWave Secret references, partial-update completion, qualified-schema WebSocket routing,
8 MiB WebSocket batch splitting, repeated reconnect fault injection, reduced JDBC `FLUSH` usage,
and JSONB binary normalization. See `README.md` and the localized connector docs for current
behavior and limitations.

## Qualification Scope

| Area | Status | Production gate |
|---|---|---|
| Build and automated baseline | Passed | P0 |
| WebSocket recovery and retry behavior | Passed, including deterministic lost-ACK replay | P0 |
| Data type compatibility matrix | Core scalar matrix passed after two fixes | P0 |
| WebSocket versus JDBC benchmark | Passed locally; batch-size caveat documented in results | P0 |
| TapData negative-task behavior | Passed for keyless and connection pre-check cases | P0 |
| TLS and RisingWave Cloud | Passed against a current dev Cloud cluster over JDBC TLS and WSS | P0 |
| Final code review and release packaging | Passed locally and on Cloud with the exact final artifact | P0 |

## Test Environment

- macOS arm64 development host
- TapData container: `tapdata-clean`, UI/API at `127.0.0.1:3031`
- RisingWave SQL endpoint: `127.0.0.1:4566`
- RisingWave WebSocket ingest endpoint: `127.0.0.1:4560`
- PostgreSQL client container: `tapdata-pg-source`
- TapData source: `mocksource` (`Mock Source`)
- TapData target: `rwconnection` (`RisingWave`, `streaming_jsonb`)
- TapData task: `Task 1` (`initial_sync+cdc`)

## Baseline Before Qualification

Observed before starting the qualification pass:

- `Task 1` was running from `mocksource.mock_source` to `rwconnection.mock_source`.
- Mock Source emitted only inserts: 100 initial rows, then one row per 1000 ms.
- Target table was a webhook-backed single-column `data JSONB` table without a primary key.
- RisingWave row count reached 655 and was increasing.
- The registered connector exposed `streaming`, `streaming_jsonb`, and `jdbc` write modes.

## Execution Log

### 2026-07-14 - Baseline

Status: Passed after one packaging fix.

Planned checks:

1. Validate spec JSON.
2. Run unit tests.
3. Run live RisingWave integration tests.
4. Build the shaded connector JAR.
5. Confirm the currently running TapData task and target count.

Results:

- `python3 -m json.tool src/main/resources/spec_risingwave.json`: passed.
- `mvn test`: passed, 16 tests, 0 failures, 0 errors.
- `mvn -Drisingwave.it=true -Dtest=RisingWaveConnectionTestIT test`: passed against the
  local RisingWave instance, 15 tests, 0 failures, 0 errors.
- `mvn -DskipTests package`: passed and produced the shaded connector JAR.
- `git diff --check`: passed.

Finding `BUILD-001`:

- Severity: P0 before release packaging.
- Repeated `mvn package` executions could pass the previously shaded main artifact back into the
  Shade plugin when the JAR plugin considered the base artifact up to date. The warning expanded
  from normal duplicate license/manifest resources to hundreds of duplicate dependency classes.
- A clean build did not reproduce the duplicate-class warnings, confirming that the stale main
  artifact was the input.
- Fix: configured `maven-jar-plugin` 3.5.0 with `forceCreation=true`, as recommended by the local
  plugin help for builds where another plugin post-processes the JAR.
- Verification: ran `mvn package -DskipTests` twice consecutively. Both executions recreated the
  unshaded JAR and reported only the expected duplicate manifest/license resources; no dependency
  classes from an earlier shaded connector artifact were reprocessed.
- Resolution: fixed and verified.

## Open Findings

- `BUILD-001`: repeated-package re-shading risk. Fixed and verified.
- `JSONB-001`: keyless JSONB retry can duplicate a persisted insert when its ACK is lost. Expected
  at-least-once limitation; release documentation and UX must state it clearly.
- `TYPE-001`: high-precision NUMERIC values were rounded through JSON-number decoding. Fixed by
  sending exact decimal strings to typed NUMERIC targets.
- `TYPE-002`: byte arrays were Base64-encoded but the default webhook decoder expects PostgreSQL
  bytea text. Fixed by emitting `\\x` hexadecimal bytea values.
- `LIFECYCLE-001`: TapData's normal Stop operation sometimes remained in `stopping` for more than
  two minutes and required the TapData force-stop operation. This was not isolated to the target
  connector. Subsequent final-connector local and Cloud tasks both stopped normally, so this is a
  P1 operational observation rather than a connector release blocker.

### 2026-07-16 - Source Update Images and WebSocket Payload Boundary

Status: Passed for PostgreSQL, MySQL `FULL`, and default MongoDB; SQL Server remains untested.

Source matrix results:

- PostgreSQL real CDC: insert, update of selected SQL columns, primary-key change, and delete
  previously passed against keyed WebSocket streaming. Unchanged target columns were preserved.
- MongoDB default `enableFillingModifiedData=true`: a real TapData replication task passed
  top-level update, nested update, top-level `$unset`, nested `$unset`, and delete. The connector
  fix treats an empty MongoDB `before` image as unavailable identity rather than a primary-key
  change; otherwise it would send `delete {}` before the valid upsert and RisingWave would reject
  the batch for missing `_id`.
- MySQL 8.4 with `binlog_row_image=FULL`: updating only `name` updated the target while preserving
  `quantity` and `note`.
- MySQL 8.4 with `binlog_row_image=MINIMAL`: TapData's Debezium source failed before the event
  reached the target with `Data row is smaller than a column index`. The target retained its last
  valid row. Production MySQL sources therefore require `binlog_row_image=FULL`.
- SQL Server was not runnable because the installed TapData instance and local connector checkout
  did not contain a SQL Server connector. This is a remaining external qualification item, not a
  known target-connector failure.

Native WebSocket payload results:

- Raw single-frame payloads with wire sizes 8,388,681 bytes, 15,728,713 bytes, and 16,776,265
  bytes received durable ACKs from the local RisingWave frontend.
- A 16,777,289-byte frame and a 17,825,865-byte frame closed with WebSocket code 1006 and no close
  frame. This confirms the effective 16 MiB single-frame boundary in the tested frontend stack.
- The connector intentionally uses an 8 MiB safety limit. It splits a multi-record logical batch
  into ordered frames and waits for each ACK. One source record whose serialized DML operation is
  over 8 MiB fails locally with an explicit error because it cannot be split into independent DML
  records.

Verification for the final local code in this pass:

- `mvn test`: 36 passed.
- `mvn -Drisingwave.it=true -Dtest=RisingWaveConnectionTestIT test`: 32 passed with Java 17 and
  `-DsocksNonProxyHosts='localhost|127.*'` to bypass the host's configured SOCKS proxy.
- `mvn package -DskipTests`: passed.
- TapData source-matrix tasks and temporary source/target databases were removed after the run.

Release observations:

- Re-registering changed Java code under the same connector metadata hash can leave TapData Agent
  using a cached old JAR. Release artifacts must use an immutable, non-SNAPSHOT version; upgrade
  testing must verify the loaded artifact checksum after Agent restart.
- The current `1.0-SNAPSHOT` connector and `2.0.8-SNAPSHOT` PDK dependencies still require an
  explicit release-version and CI decision before publication.

### 2026-07-14 - TapData Runtime Restart

Status: Passed.

Procedure:

1. Queried `count(*)` and `count(distinct data->>'id')` from `public.mock_source`.
2. Restarted the `tapdata-clean` container without modifying its writable layer.
3. Waited for the TapData HTTP endpoint to return successfully.
4. Polled the RisingWave target until the row count increased.
5. Queried the TapData task status and rechecked row identity uniqueness.

Results:

- Before restart: 1089 rows and 1089 distinct UUID values.
- TapData HTTP recovered successfully.
- Count when HTTP became ready: 1090.
- Count increased to 1091 without manual task intervention.
- Final verification: 1108 rows and 1108 distinct UUID values.
- `Task 1` returned to `running`.
- No duplicate source UUID was observed across this restart test.

Conclusion:

- TapData process restart recovery passed for the current JSONB append-only workload.
- This test does not prove ACK-loss behavior because the exact server persistence boundary was not
  controlled.

### 2026-07-14 - RisingWave Frontend Disconnect and Recovery

Status: Recovered with one duplicate in JSONB append-only mode.

Procedure:

1. Recorded the target count and distinct source UUID count.
2. Terminated only the RisingWave frontend process, leaving meta and compute state intact.
3. Kept the frontend unavailable for 12 seconds so the existing WebSocket connection closed.
4. Started a new frontend process with the same configuration.
5. Waited for SQL and WebSocket ports, then polled the target for resumed growth.
6. Checked the TapData task status and duplicate source UUIDs.

Results:

- Before disconnect: 1141 rows and 1141 distinct UUID values.
- TapData recorded WebSocket abnormal close code 1006.
- RisingWave frontend restarted successfully and retained the target table/data.
- The task recovered automatically after the connector/task retry path completed.
- After recovery: 1325 rows and 1324 distinct UUID values.
- Exactly one source UUID appeared twice.
- `Task 1` returned to `running` and continued ingesting.

Finding `JSONB-001`:

- Severity: expected semantic limitation; P0 documentation/product decision before release.
- A WebSocket disconnect can cause an acknowledged-or-persisted insert to be retried when the ACK
  is not observed. A keyless append-only target has no identity with which to deduplicate the
  replay, so the retry can append a duplicate JSON document.
- This matches the documented at-least-once limitation of `streaming_jsonb`; it is not acceptable
  if the product claims exactly-once behavior for this mode.
- Required release action: keep the mode explicitly labeled append-only, retain the update/delete
  rejection, and make retry duplicates visible in user-facing documentation. A future deduplicated
  JSONB mode would need a deterministic event identity and a target primary key.

### 2026-07-14 - Keyed Streaming Frontend Disconnect and Recovery

Status: Passed.

Procedure:

1. Created an isolated RisingWave connection named `rw_streaming_keyed_qual` using the normal
   `streaming` write mode and schema `keyed_qual`.
2. Created and started `keyed_recovery_qual`, an `initial_sync+cdc` task from
   `mocksource.mock_source` to `keyed_qual.mock_source`.
3. Verified that the target table retained source columns and used `id` as its primary key.
4. Stopped the independent JSONB task to isolate the recovery result.
5. Terminated only the RisingWave frontend, held it down for 12 seconds, and restarted it with the
   same configuration.
6. Waited for the TapData retry path and compared target row count with distinct primary-key count.

Results:

- Initial keyed target verification: 295 rows and 295 distinct `id` values.
- Immediately before the controlled disconnect: 323 rows and 323 distinct `id` values.
- The target continued from 329 rows after the frontend returned, then paused during the retry
  window and resumed without manual intervention.
- Final recovery verification: 343 rows and 343 distinct `id` values.
- No duplicate primary key was present after WebSocket replay/recovery.

Conclusion:

- Normal keyed `streaming` recovered automatically and preserved one target row per source key in
  this disconnect test.
- Together with `JSONB-001`, this demonstrates the semantic boundary: a primary-key target can
  make replay idempotent through upsert, while keyless append-only JSONB cannot deduplicate a lost
  ACK replay.
- This is strong recovery evidence but not yet a deterministic ACK-loss test; an injected fault at
  the persist-before-ACK boundary was added and is recorded below.

### 2026-07-14 - Deterministic Persisted-but-Lost-ACK Replay

Status: Passed with mode-specific expected semantics.

Method:

- Added `scripts/ws_ack_drop_proxy.py`, which forwards a real ingest WebSocket to RisingWave, waits
  until RisingWave returns the batch ACK, drops that ACK, and closes the client connection.
- Added opt-in `RisingWaveAckLossIT`. It sends through the fault proxy, verifies the first write is
  already query-visible, reconnects directly, and replays the same insert.
- The test covers both a primary-key webhook table and a keyless single-JSONB webhook table.

Results:

- The proxy deterministically dropped ACK 1 for each table after RisingWave generated it.
- Keyed table: persisted first attempt plus replay resulted in exactly 1 row.
- Keyless JSONB table: persisted first attempt plus replay resulted in exactly 2 rows.
- Test result: 1 passed, 0 failures/errors.

Conclusion:

- The connector's retry boundary is now proven rather than inferred: normal keyed streaming is
  idempotent under an ACK loss, while JSONB append-only is at-least-once and can duplicate.
- The existing mode labels and retry-duplicate documentation match observed behavior.

### 2026-07-14 - Keyed Task Stop, Start, and Reset Lifecycle

Status: Passed, with one slow-stop observation to repeat.

Stop/start results:

- Initial state: `running`, 540 rows and 540 distinct primary keys.
- A normal Stop request reached `stop`; the target remained exactly 540/540 for an 8-second
  observation window.
- Start transitioned through `wait_run` to `running` and target growth resumed at 553/553.
- No duplicate primary key was introduced.

Reset results:

- A stopped task had 801 rows and 801 distinct primary keys before Reset.
- Reset transitioned from `renewing` to `wait_start` without dropping or corrupting the existing
  keyed target.
- Start transitioned from `wait_run` to `running`; the target subsequently grew to 901 rows and
  901 distinct primary keys.
- Reset/start therefore preserved keyed idempotency for this task.

Test-script correction:

- An initial Reset probe incorrectly used PostgreSQL's `to_regclass()` function, which the tested
  RisingWave build does not provide. The resulting `InternalError: function to_regclass(character
  varying) does not exist` was a qualification-script error, not a connector or Reset failure.
- The repeated test used `pg_tables` and direct target queries instead.

Observation `LIFECYCLE-001`:

- One normal Stop completed quickly; a later Stop remained in `stopping` for more than two minutes
  before eventually reaching `stop`. During `stopping`, premature Reset and Start requests did not
  advance the task, and the target remained stable at 801/801.
- The slow Stop repeated on the PostgreSQL-to-RisingWave type task after loading the updated JAR;
  it required TapData's force-stop path after a 60-second normal-stop wait. This is now reproducible
  at the TapData task lifecycle layer, but is not yet attributed to the RisingWave connector because
  TapData owns pipeline shutdown and the source connector also participates.

### 2026-07-14 - WebSocket Scalar Type Matrix

Status: Passed after two serialization fixes.

Coverage:

- `smallint`, `integer`, `bigint` including an integer above JavaScript's exact-number boundary
- `real`, `double precision`, and high-precision `numeric`
- `boolean`, Unicode `varchar`, and `NULL`
- `date`, microsecond `time`, microsecond `timestamp`, and offset-aware `timestamptz`
- `bytea`
- nested `jsonb`

Method:

- Added a live integration test that creates a webhook-backed table through the connector, sends
  one record through the connector's WebSocket write capability, reads it back over JDBC, and
  asserts every value and type.
- Added a JSONB-mode precision assertion to ensure the chosen exact-number representation is
  explicit and repeatable.

Finding `TYPE-001` and fix:

- Before the fix, `1234567890.123456789` arrived as `1234567890.123457` in a typed `NUMERIC`
  target because the webhook JSON-number path rounded through floating point.
- The connector now serializes `BigDecimal`/`BigInteger` as exact decimal strings where required.
  Typed `NUMERIC` parses the string back to the exact numeric value.
- In JSONB append-only mode, exact arbitrary-precision values remain JSON strings because JSONB's
  numeric path also rounded (`1234567890.123456789` became `1234567890.1234567`). This avoids
  silent corruption at the cost of JSON numeric type; English, Chinese, and README documentation
  now state this behavior.

Finding `TYPE-002` and fix:

- Before the fix, Jackson encoded Java `byte[]` as Base64 while the webhook decoder's default bytea
  mode expected PostgreSQL bytea text. A 3-byte input was read back as 8 incorrect bytes.
- Typed `bytea` values are now serialized as PostgreSQL `\\x` hexadecimal strings.
- The live test reads back the original three bytes exactly.

Verification:

- `websocketStreamingWritesSupportedScalarTypes`: passed.
- `jsonbAppendOnlyModeCreatesKeylessTargetAndWritesWholeDocuments`: passed with exact decimal text
  and `jsonb_typeof(...) = 'string'`.
- Full unit suite after the fixes: 16 tests passed, 0 failures/errors.
- Full live RisingWave suite after the fixes: 16 tests passed, 0 failures/errors.
- Shaded package build and `git diff --check`: passed.

End-to-end TapData confirmation:

- Registered the rebuilt JAR with replace-latest and restarted TapData.
- Reset the isolated PostgreSQL `qual_types` to RisingWave WebSocket task and recreated its target.
- The task reached `running` / `CDC` without errors.
- Read-back preserved all tested values, including exact
  `1234567890.123456789` and binary hex `000fff`; the primary key, Unicode, temporal values, nested
  JSON, and null value were also correct.
- Running the same task before redeploying the rebuilt JAR intentionally reproduced the old numeric
  and bytea behavior, confirming that the observed correction came from the connector changes.

### 2026-07-14 - Real TapData Negative Validation

Status: Passed for the tested cases.

Keyless normal-streaming task:

- Created a real PostgreSQL source table `qual_keyless` without a primary key and a TapData
  `initial_sync+cdc` task targeting normal WebSocket `streaming`.
- The task transitioned to `error` during target table application.
- TapData exposed this user-visible error event:
  `WebSocket streaming requires a primary key for table qual_keyless so updates, deletes, and
  retries preserve row identity`.
- The failure occurred before creating an invalid target table.

Connection pre-checks through TapData:

| Case | Connection status | Relevant result |
|---|---|---|
| Unreachable WebSocket endpoint | `invalid` | SQL/version/schema/write passed; `ingest_endpoint` failed with `java.net.ConnectException` |
| Missing schema | `invalid` | schema failed explicitly; write and ingest were skipped with schema-unavailable explanations |
| User without schema CREATE | `invalid` | schema passed; write reported permission denied for `public` CREATE; ingest was skipped |
| PostgreSQL presented as a streaming RisingWave target | `invalid` | version failed with `WebSocket streaming requires RisingWave 3.0.0 or later` |

Conclusion:

- The requested schema, write-permission, version-family, and WebSocket reachability checks are
  visible in TapData and correctly gate dependent checks.
- The live environment cannot impersonate an actual RisingWave 2.x server; the precise 2.9-to-3.0
  boundary remains covered by unit version parsing/check tests rather than a live old-version node.

### 2026-07-14 - WebSocket Versus JDBC Local Benchmark

Status: Passed on the final JDBC-correctness implementation.

Method:

- Added an opt-in `RisingWaveWriteBenchmarkIT` so normal unit/live suites do not inherit benchmark
  runtime or timing assumptions.
- Both modes use the real connector capability, identical two-column keyed tables and records, and
  verify that all rows become query-visible before reporting success.
- The benchmark separately reports connector write/ACK time and post-write visibility lag.

Results:

| Rows | Batch | WebSocket rows/s | JDBC rows/s | WebSocket/JDBC |
|---:|---:|---:|---:|---:|
| 2,000 | 500 | 501.5 | 227.6 | 2.20x |
| 10,000 | 1,000 | 1,000.6 | 233.0 | 4.29x |

Observations:

- WebSocket was 2.20-4.29x faster in these final local runs.
- The corrected JDBC fallback uses manual keyed upsert and explicit `FLUSH` visibility boundaries
  because RisingWave does not support PostgreSQL `ON CONFLICT`. That correctness cost is reflected
  in the final JDBC throughput.
- Performance remains workload-, environment-, and batch-size-dependent; these numbers justify the
  WebSocket default but are not a Cloud capacity or SLA claim.
- An immediate query in the first benchmark attempt saw 1,948 of 2,000 rows despite all ACKs. A
  short visibility poll reached all rows; the corrected benchmark measures visibility separately.
- All successful runs verified the exact final row count.

Reproduction:

```bash
mvn -Drisingwave.it=true -Drisingwave.benchmark=true \
  -Drisingwave.benchmark.rows=10000 -Drisingwave.benchmark.batchSize=1000 \
  -Dtest=RisingWaveWriteBenchmarkIT test
```

### 2026-07-14 - TLS/WSS Capability and SSL UX

Status: Trusted WSS path passed locally and was subsequently revalidated on RisingWave Cloud.

Review finding `TLS-001`:

- The connector spec's `ssl` tag caused TapData to inject a generic UI for custom CA, client
  certificate, and client key uploads.
- The connector did not consume any of those injected fields. Leaving the panel visible would
  falsely claim custom-CA and mutual-TLS support.
- Fix: removed the generic `ssl` tag. The connector still exposes the capabilities it implements:
  JDBC `sslmode` and explicit `wss://` ingest endpoints using the Java runtime trust store.
- English, Chinese, and README documentation now state that custom CA uploads and mTLS client
  certificates are unsupported. RisingWave Cloud's publicly trusted server certificate does not
  require those uploads.

Trusted WSS test:

- Generated a one-day local certificate with IP SAN `127.0.0.1` and imported it into an isolated
  Java trust store.
- Ran the ingest proxy with TLS termination and ACK forwarding to the real local RisingWave WebSocket
  endpoint.
- Added and ran opt-in `RisingWaveTlsIT`; it connected over `wss://`, wrote a real DML batch, received
  the RisingWave ACK, and verified the row through SQL.
- Result: 1 passed, 0 failures/errors.

The current Cloud validation is recorded in the dedicated section below.

### 2026-07-14 - Release Packaging and Registered 1.0.0 Black-box Check

Status: Passed locally.

Release metadata:

- Changed the connector artifact version from `1.0-SNAPSHOT` to `1.0.0`. The TapData PDK/API
  dependencies remain at the TapData runtime-compatible `2.0.8-SNAPSHOT` level.
- Updated README and handoff deployment paths to `risingwave-connector-1.0.0.jar`.
- Clean unit run: 17 tests passed, 0 failures/errors.
- Clean real-RisingWave integration run: 20 tests passed, 0 failures/errors.
- Two consecutive non-clean `mvn -DskipTests package` runs passed without re-shading dependency
  classes. Only expected manifest/license/notice resource overlap warnings remained.
- `git diff --check` passed.

Artifact audit:

- Artifact: `tapdata-plugin/target/risingwave-connector-1.0.0.jar`
- Last qualified release-shaped artifact size: 3,857,700 bytes.
- Last qualified release-shaped artifact SHA-256:
  `4c6e28a04fec69332cf72a266c4d181427220ca36fb2675fcef18a534e34637e`.
- Manifest `Implementation-Version`: `1.0.0`; Java release: 11.
- Embedded `spec_risingwave.json` parsed successfully.
- PostgreSQL JDBC driver was present.
- 1,175 relocated Jackson entries were present under the connector namespace; no unrelocated
  `com.fasterxml.jackson` classes remained.
- No `io.tapdata.pdk` or `io.tapdata.entity` classes were bundled, avoiding runtime API conflicts.

TapData registration and black-box result:

1. Confirmed TapData had zero tasks and zero connections, then removed only the obsolete
   RisingWave definition and its 20 historical GridFS files.
2. Registered the exact audited JAR with `pdk-deploy register -l` and restarted TapData.
3. Verified TapData stored the then-current 3,857,697-byte qualification JAR whose MD5 matched the
   local artifact.
4. TapData intentionally records a directly uploaded non-SNAPSHOT PDK as `latest=false`; it is not
   user-visible until it passes the normal TapData publication/approval step. For local acceptance
   testing only, the definition was marked latest in the embedded development database.
5. Created a temporary local target connection through TapFlow. It became `ready`, reported
   `definitionVersion=1.0.0`, used the expected PDK hash, and completed schema loading.
6. Repeated the exact-artifact pre-check against RisingWave Cloud over JDBC TLS and WSS. That
   connection also became `ready`, reported `definitionVersion=1.0.0`, and completed schema loading.
7. Deleted both temporary connections and dropped the temporary Cloud schema.
8. A later documentation-only change reordered the displayed write modes to streaming, JSONB, and
   JDBC. A clean build and all 17 unit tests passed afterward, producing the current artifact hash
   above. Connector code and runtime behavior did not change, so the Cloud pre-check was not
   repeated for this wording-only rebuild.

Earlier registered-`1.0.0` checkpoint replication task:

1. Created an isolated `release_qual` schema and a second target connection pinned to the new
   `1.0.0` PDK hash.
2. Started `rw_release_1_0_0_task`, a real PostgreSQL `pg_qual_source.qual_types` to RisingWave
   WebSocket streaming replication task.
3. The task reached `running/CDC`, proving the TapData worker loaded and used the registered
   `1.0.0` connector rather than only validating its metadata.
4. RisingWave contained the expected row with exact NUMERIC `1234567890.123456789`, BYTEA hex
   `000fff`, Unicode `TapData 中文 🚀`, and JSON `{"nested": [1, "two"]}`.
5. A normal synchronous Stop completed successfully, the task reached `stop`, and task deletion,
   connection deletion, and schema cleanup all succeeded. This clean run reduces the likelihood
   that `LIFECYCLE-001` is intrinsic to every target-connector shutdown, but does not explain its
   earlier reproducible occurrence.

Post-restart data verification:

- The real PostgreSQL-to-RisingWave scalar task remained running after TapData restart.
- `keyed_qual.qual_types` retained one row with exact NUMERIC
  `1234567890.123456789`, BYTEA hex `000fff`, Unicode text, JSON, and date/time values.
- The separate `keyed_recovery_qual` task's `error` timestamp predates the final build and restart;
  it is historical qualification state, not a new `1.0.0` regression.

### 2026-07-14 - RisingWave Cloud JDBC TLS, WSS, and Replication

Status: Passed.

Environment and transport:

- Used the existing authenticated `rwc` CLI to confirm the `my-project` dev cluster was Running and
  Healthy in `ap-southeast-1`.
- Connected through the Cloud SQL endpoint with PostgreSQL JDBC-compatible TLS semantics equivalent
  to `sslmode=require`.
- The server reported RisingWave `3.0.1`, covering the connector's minimum supported WebSocket
  version boundary.
- The Cloud HTTPS endpoint presented a publicly trusted certificate (`curl` verification result 0).
- The correct ingest base was the cluster hostname over `wss://` on port 443; direct port 4560 is
  not the public Cloud route.

Registered `1.0.0` connection pre-check:

- Created an isolated Cloud schema and a temporary TapData target connection pinned to connector
  definition `1.0.0`.
- The connection reached `ready`.
- PDK version, JDBC connection, RisingWave version, schema, webhook table create/drop, WSS signed
  init, real DML, and RisingWave ACK all passed.
- The optional initial `Load Schema` item reported no tables because the isolated schema was empty;
  this did not invalidate the connection, and the replication task subsequently created its table.

Full and incremental replication:

1. Started a real PostgreSQL-to-Cloud task using normal keyed WebSocket streaming.
2. The task reached `running/CDC` and created the target table.
3. Initial data preserved exact NUMERIC `1234567890.123456789`, BYTEA hex `000fff`, Unicode text,
   and nested JSON.
4. Inserted a CDC row and verified exact NUMERIC `9876543210.123456789`, BYTEA hex `deadbeef`, text,
   and JSON in Cloud.
5. Updated that row's primary key from 2 to 3. Cloud removed key 2 and exposed the updated key 3,
   validating delete-before-upsert identity handling.
6. Deleted key 3 at the source and verified it disappeared from Cloud.
7. A normal synchronous task Stop completed, the task reached `stop`, and task deletion succeeded.

Cleanup and security:

- Deleted the temporary Cloud connection and dropped the isolated Cloud schema.
- Removed the temporary TapFlow virtual environment.
- Cloud and TapData credentials were passed only through interactive process environment variables
  and were not persisted in this record, repository files, or test scripts.

### 2026-07-14 - Final Pre-commit Code Review and Exact-artifact Requalification

Status: Passed; the changes were subsequently split into conventional commits on 2026-07-15.

Review findings and fixes:

- `WS-CONCURRENCY-001`: a terminal WebSocket failure could race a new send after pending futures
  were drained, leaving the new future to wait for the full timeout. Terminal failure, close, send,
  and pending-future completion now share the send lock; closed clients reject new batches
  immediately. A deterministic unit test covers the close boundary.
- `JDBC-DML-001`: the fallback used PostgreSQL `ON CONFLICT`, which RisingWave does not support,
  and update/delete behavior was incorrect for primary-key changes and keyless before images. JDBC
  now performs manual keyed upsert, uses delete-before-upsert for identity changes, filters keyless
  updates by the full before image including `IS NULL`, and establishes `FLUSH` visibility
  boundaries.
- `JDBC-BATCH-001`: two inserts for the same primary key in one TapData batch could both observe the
  row as absent before RisingWave made the first DML visible. The connector now tracks pending keyed
  identities and flushes only when a key repeats, retaining normal batch throughput. A live test
  proves the second event wins and only one row remains.
- `LIFECYCLE-002`: repeated `init`, table drop, and partial initialization could retain JDBC or
  WebSocket resources. Initialization now closes prior resources first, resolves streaming config
  before JDBC allocation, and closes the per-table WebSocket client before dropping a table.
- `TIMEZONE-001`: the previous offset choices were not valid RisingWave timezone names and URL
  concatenation could decode them incorrectly. The UI now accepts IANA names such as `UTC` and
  `Asia/Shanghai`; JDBC passes the value with the connection `options` property. The live IANA
  timezone test passed.

Final verification on the exact source tree:

- `mvn clean test`: 17 passed, 0 failures/errors.
- `mvn -Drisingwave.it=true -Dtest=RisingWaveConnectionTestIT test`: 20 passed against the real
  local RisingWave service, 0 failures/errors.
- Two consecutive `mvn -DskipTests package` runs passed with only expected manifest/license/notice
  overlap warnings.
- `git diff --check` and JSON parsing of both source and embedded `spec_risingwave.json` passed.
- JAR audit found the PostgreSQL driver, 1,175 relocated Jackson entries, zero unrelocated Jackson
  classes, and no bundled TapData PDK/entity API classes.
- The exact final JAR passed local and RisingWave Cloud TapData connection pre-checks as described
  above.

### 2026-07-14 - Local Qualification Environment Cleanup

Status: Completed.

- Requested normal TapData deletion for all six qualification tasks. TapData renamed the records
  with random suffixes and left them indefinitely in `deleting`, reproducing the stale-task UI
  behavior independently of RisingWave writes.
- After confirming the exact qualification task IDs and finding no associated records in other
  collections, removed only those six stuck local Task records from the embedded MongoDB.
- Deleted all four qualification connections through the normal TapData API.
- Final TapData state: 0 tasks and 0 connections.
- Removed the obsolete RisingWave snapshot and earlier `1.0.0` connector definitions plus their
  stale GridFS files. TapData now contains only the final RisingWave connector `1.0.0` artifact with
  the qualified PDK hash.
- Restarted TapData and verified its HTTP endpoint recovered successfully.
- Dropped all qualification schemas/tables from local RisingWave and the PostgreSQL source.
- Removed temporary TapFlow environments and confirmed no ACK proxy or qualification processes
  remained.
- Preserved the local TapData, PostgreSQL, and RisingWave services for the next development phase.
- Preserved the audited `1.0.0` artifact and all source/test/readiness changes. No commit was made
  during the qualification run itself.

### 2026-07-15 - Conventional Commit Preparation

Status: Completed through documentation commit preparation.

- Restored the development artifact version to `1.0-SNAPSHOT`; the immutable `1.0.0` bump is
  reserved for the release step after review and CI.
- Split build configuration, core implementation, live qualification tests, user-facing modes,
  and the local pipeline helper into independently reviewable conventional commits.
- Verified the build commit in a clean detached worktree.
- Verified the core implementation commit with clean unit tests and packaging.
- Verified the live-test commit with 17 unit tests and 20 real local RisingWave integration tests.
- Verified the spec/documentation commit with clean unit tests, packaging, source spec parsing, and
  parsing of the spec embedded in the shaded JAR.

### 2026-07-15 - P1 PDK Alignment and Maintainability Pass

Status: Connector changes passed local unit, live integration, and packaging verification; visual
TapData form inspection remains manual because no browser-control runtime was available.

Implementation:

- Aligned `ConnectionOptions` with the official TapData PostgreSQL connector pattern. Connection
  tests now return a password-free connection string, stable SHA-256 instance identity, schema
  namespace, and the actual RisingWave server version. The identity excludes the password,
  webhook secret, write mode, and schema so changing transport or namespace does not create a
  second physical database identity.
- Replaced repeated form parsing with immutable `RisingWaveConfig`, shared by connection tests and
  task initialization, so defaults, JDBC properties, and WebSocket settings cannot drift.
- Extracted connection qualification, JDBC classloader setup, SQL/type helpers, and value
  conversion into focused classes. `RisingWaveConnector` decreased from 1,608 to 1,026 lines while
  retaining task lifecycle, schema discovery, DDL, and write orchestration.
- Added focused unit coverage for identity stability, password exclusion, JDBC property handling,
  exact JSONB number serialization, nested values, and BYTEA encoding.
- Follow-up review moved configuration parsing inside the connection-test error boundary. Invalid or
  out-of-range ports now produce one clear failed Connection test item instead of escaping from the
  PDK call. The returned `dbVersion` is normalized to the RisingWave semantic version while the
  Version test item retains the complete server banner.
- Removed the hard-coded JSONB payload column from webhook validation SQL and expanded the identity
  contract tests: password, webhook secret, schema, and write mode do not change the instance ID;
  host, port, database, and user do.
- Removed unrelocated Jackson multi-release optimization classes from the shaded JAR. Maven Shade
  relocates the base classes but not classes beneath `META-INF/versions`; the connector supports
  Java 11 and uses the relocated base implementation on all supported runtimes.

Verification:

- `mvn test`: 25 passed, 0 failures/errors.
- `mvn -Drisingwave.it=true -Dtest=RisingWaveConnectionTestIT test`: 21 passed against the real
  local RisingWave service, including the new `ConnectionOptions` identity and version assertion.
- `mvn clean package` followed by a second `mvn -DskipTests package`: both passed; only the expected
  manifest/license/notice resource overlap warnings remained.
- Final snapshot JAR SHA-256:
  `071ee87424474288461c4286bf3957d95da89799e67dcfd5f7dd220a1f8143ee`.
- JAR audit: PostgreSQL driver present, 1,129 relocated Jackson classes, zero original Jackson
  classes, and zero bundled TapData PDK/entity API classes.
- `git diff --check` passed.

Deferred or external:

- TapData Create Connection visual inspection is still required after installing this snapshot;
  the current session could inspect the form schema but could not control a browser.
- `LIFECYCLE-001` remains a TapData runtime observation and was not masked by connector retry code.
- CI integration remains deferred pending Wenym's confirmation.
- No commit, push, PR, version bump, or connector publication was performed in this pass.

## Release Decision

Connector-controlled production qualification passed. The `1.0.0` artifact is ready for a
controlled production pilot and the normal code review, CI, and TapData connector
publication/approval process.

All P0 connector gates passed locally and against RisingWave Cloud, including connection pre-check,
TLS, WSS, full sync, incremental insert, primary-key update, delete, exact scalar serialization,
restart/recovery, ACK-loss behavior, and clean task shutdown. `LIFECYCLE-001` remains a P1 TapData
operational observation because earlier tasks occasionally needed force-stop, but both final
`1.0.0` local and Cloud tasks stopped normally.

The qualification pass itself preceded the conventional commits. No CI submission or release
publication has been performed.
The local `latest=true` activation was acceptance-test setup only and must not be used as the
production publication procedure.

### 2026-07-15 - Local TapData Metadata Reset

Status: Completed; the environment is ready for a fresh Data Replication task.

- Removed the stopped `Task 1`, its TaskRecord, two monitoring log entries, and 16 metadata records
  associated with the old Mock Source/RisingWave connections.
- Removed the `rwrwmock` and `rwrw` connections. The old target was pinned to connector `1.0.0`
  while its metadata used the `1.0-SNAPSHOT` qualified name, which caused
  `metadataInstances is null` before connector write execution.
- Dropped only the 11 `ws_*` tables created by `e2e_test/webhook/websocket_ingest.slt`; the
  `public` schema itself was preserved.
- Removed both stale RisingWave `DatabaseTypes` records and registered the current
  `risingwave-connector-1.0-SNAPSHOT.jar` again.
- TapData stores snapshot versions as `latest=false` even when `pdk-deploy register -l` is used.
  For this local acceptance environment only, marked the sole RisingWave snapshot definition as
  `latest=true` and restarted TapData. A production package must use a normal release version and
  publication flow instead of this database override.
- Final verification: 0 connections, 0 tasks, 0 stale metadata records, no tables in local
  RisingWave `public`, exactly one RisingWave connector definition (`1.0-SNAPSHOT`, `latest=true`),
  and the `tapdata-clean` container healthy.

### 2026-07-15 - Fresh-User Manual Installation Baseline

Status: Ready for the user to install the connector manually.

- Removed the remaining RisingWave `DatabaseTypes` definition, 16 uploaded GridFS files and their
  72 chunks, the copied `/tmp` connector, and old connector JARs cached under TapData `dist` and
  `tap-running` directories.
- Restarted TapData without registering a replacement connector. Final baseline: 0 RisingWave
  plugin definitions, 0 RisingWave GridFS files, 0 connections, 0 tasks, no RisingWave connector
  JAR inside the container, and no tables in local RisingWave `public`.
- Rebuilt the distribution artifact from the current working tree with `mvn clean package`, then
  reran `mvn package`: 25 unit tests passed and both builds completed successfully.
- Manual-install artifact: `tapdata-plugin/target/risingwave-connector-1.0-SNAPSHOT.jar`.
- Final JAR SHA-256: `8480b6aa0c8fd9496ae93789d1ed905d9d0ad67d6a260943d304fd59cac8ca4b`.
- The environment intentionally stops before `pdk-deploy register`, so the next action exercises
  the same explicit plugin-install step expected of a test user.
- The subsequent manual-user install uploaded the JAR and created the expected RisingWave
  `DatabaseTypes` record, but TapData stored `1.0-SNAPSHOT` as `latest=false` despite the deploy
  command using `-l`; the connector was therefore absent from Create Connection after restart.
  Marking the sole local snapshot record `latest=true` made it eligible for UI discovery. This is
  a snapshot-only local test workaround and further confirms that the distributable production
  artifact must use a non-SNAPSHOT release version.
- A fresh connection against an intentionally empty `public` schema passed all six connector-owned
  checks: PDK version, JDBC connection, RisingWave version, schema existence, webhook-table
  create/drop permission, and WebSocket init/DML/ACK. TapData appended a non-required
  `LOAD_SCHEMA` item with `Not found any schema`; the overall result remained `SUCCESS`, and the
  saved connection was `ready` with `everLoadSchema=true`, `loadFieldsStatus=finished`, and
  `tableCount=0`. This is expected for an empty target and must not be worked around by leaving a
  synthetic table behind.
- The first fresh-user Mock Source to RisingWave Data Replication task started successfully and
  created `public.mock_source_test`. The UI displayed `The requested resource does not exist`
  because it requested the absent Community endpoint `/api/task-inspect/<task-id>`; this request
  occurred before the successful task start and was unrelated to connector execution.
- The same `ghcr.io/tapdata/tapdata:latest` environment rejected Agent metric submissions to
  `/api/measurement/points/v2` with `TapOssNonSupportFunctionException`. The Agent applied its
  120-second retry window, so initial batch read appeared stuck for exactly two minutes before it
  completed and incremental sync started.
- Final live verification: task status `running`, zero task monitoring errors, RisingWave row count
  increased from 156 to 161 in five seconds. This proves automatic target-table creation and
  WebSocket incremental replication from an initially empty RisingWave schema. The missing
  task-inspect/measurement APIs are a TapData Community image/UI/Agent compatibility issue, not a
  RisingWave connector failure; production qualification should use a pinned, internally matched
  TapData release rather than the mutable `latest` tag.

### 2026-07-15 - Post-review Concurrency, Discovery, and Secret Hardening

Status: Implemented and verified locally; final package/review gate pending.

- Confirmed and fixed the missing `FLUSH` after `clearTable` so a JDBC clear is query-visible before
  a WebSocket reload starts. A live clear-to-streaming-reload test verifies only the replacement row
  remains.
- Confirmed that PDK write callbacks may reach the connector concurrently while PostgreSQL JDBC
  connections are not thread-safe. JDBC write batches now serialize around the shared connection;
  a concurrent same-key integration test verifies one final target row without duplicate-key races.
- Fixed filtered schema discovery so a requested missing table returns no models instead of falling
  back to every base table. Added the defensive `table_name` PK join condition; this is hygiene, not
  a known PostgreSQL correctness failure because constraint names are schema-scoped.
- Replaced per-table `HttpClient` construction with one shared client. The table WebSocket cache now
  uses leases, retains at most 100 idle/cached clients, evicts only clients without in-flight work,
  and closes overflow clients after their active batch rather than interrupting an ACK wait.
- Removed webhook-secret literals from table validation DDL. Signed tables now use
  `VALIDATE SECRET <name>`; the connector can securely create/rotate/drop a per-table catalog Secret
  or reference a user-managed Secret. Automatic management requires RisingWave Secret Management
  and `CREATE SECRET` permission; unsigned WebSocket mode and JDBC mode do not require it.
- Verified that `SHOW CREATE TABLE` contains the Secret reference but not its value, managed Secret
  rotation works across connector restart, a user-managed Secret is not dropped, and all test-created
  Secrets are cleaned up.
- Current automated results: 28 unit tests passed and 27 live RisingWave integration tests passed.
