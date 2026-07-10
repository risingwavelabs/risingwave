# Tapdata RisingWave Plugin Handoff

Status date: 2026-07-10 (updated)
Original date: 2026-06-17
Workspace used for validation: `/Users/william/conductor/workspaces/risingwave/milan`
Branch: `wenym1/tapdata-plugin`

This document is intended to be self-contained enough to continue the Tapdata RisingWave plugin work in a new workspace.

## Current Status

The Tapdata RisingWave plugin has been updated, smoke-tested, and verified end-to-end against RisingWave Cloud (v3.0.1) via WebSocket ingest.

Validated path:

1. Mock Source or PostgreSQL source in Tapdata.
2. Tapdata pipeline using the RisingWave connector in `streaming` mode.
3. RisingWave target table auto-created as a webhook-backed table (with VALIDATE clause when webhook secret is configured).
4. Tapdata initial sync writes rows through RisingWave websocket ingest.
5. Tapdata CDC writes update, insert, and delete through the same websocket stream.
6. RisingWave target table reaches the expected final state.

## Changes since 2026-06-17 (2026-07-10 update)

### Build system
- **PDK dependency fixed**: Replaced non-public `io.tapdata:tapdata-pdk-all:2.0.7-SNAPSHOT` with the official split dependencies `io.tapdata:tapdata-pdk-api:2.0.8-SNAPSHOT` + `io.tapdata:tapdata-api:2.0.8-SNAPSHOT` (from `https://nexus.tapdata.net/repository/maven-snapshots/`). This matches how official connectors (postgres, doris) in `tapdata/tapdata-connectors` declare dependencies.
- **PDK source**: The PDK API source lives in `tapdata/tapdata-common-lib` repo, `plugin-kit/` directory (the old `tapdata/idaas-pdk` repo is archived).
- **Java 11** target (matches wenym's original).

### UX improvements
- **Icon added**: `src/main/resources/icons/risingwave.png` (real RisingWave wave logo).
- **Documentation added**: `docs/risingwave_en_US.md` and `docs/risingwave_zh_CN.md`.
- **`doc` field fixed**: Changed from inline text to markdown file path (matching official connector convention).
- **SSL mode field**: Added `sslmode` config field (default `prefer`). Cloud needs `require` for TLS SNI tenant routing.
- **Extra parameters + timezone fields** added to spec.
- **`ssl` tag** added to connector tags.

### DDL generation
- **VALIDATE clause**: `createTable` now adds `VALIDATE AS secure_compare(...)` when `webhookSecret` is configured. Previously no signature verification.

### SSL fix
- Changed JDBC `sslmode` from hardcoded `disable` to configurable (default `prefer`). Fixes "failed to get tenant identifier" on RisingWave Cloud.

### Version requirements
- WebSocket streaming mode requires RisingWave **3.0.0+** (PR #25444).
- JDBC mode works with any RisingWave version.

Current touched files:

```text
tapdata-plugin/setup_pipeline.py
tapdata-plugin/src/main/java/io/tapdata/risingwave/RisingWaveConnector.java
tapdata-plugin/src/main/java/io/tapdata/risingwave/streaming/WsIngestClient.java
```

Important: the current workspace also contains unrelated/untracked PoC report files. They are not part of the Tapdata plugin work.

## Protocol Design

### Old Client Payload

The previous plugin sent a top-level JSON array of per-row DML objects:

```json
[
  {"dml_id":1,"op":"upsert","data":{"id":1,"name":"Alice"}},
  {"dml_id":2,"op":"delete","data":{"id":2}}
]
```

### Current Required Payload

RisingWave now expects a batch envelope:

```json
{
  "dml_batch_id": 1,
  "items": [
    {"op":"upsert","data":{"id":1,"name":"Alice"}},
    {"op":"delete","data":{"id":2}}
  ]
}
```

ACKs are by batch:

```json
{"ack":1}
```

Fatal errors still use:

```json
{"fatal":"..."}
```

The client still sends an init frame first:

```json
{"type":"init","timestamp":1760000000000}
```

If `webhookSecret` is configured, the init payload is signed with HMAC SHA-256 and sent in header `x-rw-signature` as:

```text
sha256=<hex hmac>
```

## Plugin Architecture

### Write Modes

`RisingWaveConnector` supports two write modes:

```text
jdbc       standard JDBC insert/update/delete path
streaming  websocket ingest path, using RisingWave webhook-backed target tables
```

`ingest_mode=streaming` enables websocket ingest. If unset, the connector defaults to JDBC mode.

### Streaming Connection Fields

Streaming mode uses these Tapdata connection config fields:

```text
host             RisingWave SQL host, used for JDBC metadata/DDL
port             RisingWave SQL port, usually 4566
database         RisingWave database, usually dev
schema           RisingWave schema, usually public
user             RisingWave SQL user, usually root
password         RisingWave SQL password
ingest_mode      streaming
ingestEndpoint   websocket endpoint, for example ws://host.docker.internal:4560
webhookSecret    optional webhook init signature secret
```

The websocket URI is built as:

```text
{ingestEndpoint}/ingest/{database}/{schema}/{table}
```

Example:

```text
ws://host.docker.internal:4560/ingest/dev/public/tapdata_ws_smoke_v14
```

### Connection Test Design

Connection test still checks JDBC connectivity and `SELECT version()`.

For streaming mode, connection test only checks TCP reachability of `ingestEndpoint`. It intentionally does not open an ingest websocket against an arbitrary existing table.

Reason: existing tables may not be webhook-backed or may require a different secret. Earlier tests failed with errors such as:

```text
`SECURE_COMPARE()` failed
table lookup failed: Table `...` is not with webhook source
```

This was not a useful connection-test signal.

## DDL Design

In streaming mode, Tapdata target table creation must create RisingWave webhook-backed tables:

```sql
CREATE TABLE IF NOT EXISTS "public"."tapdata_ws_smoke_v14" (
  "id" integer,
  "customer_name" text,
  "amount" numeric,
  "status" text,
  "updated_at" timestamp,
  PRIMARY KEY ("id")
) WITH (connector = 'webhook');
```

Key details:

```text
Streaming mode appends: WITH (connector = 'webhook')
Streaming mode omits NOT NULL column options
Primary key is still emitted when Tapdata provides one
JDBC mode keeps normal CREATE TABLE behavior, including NOT NULL
```

Why omit `NOT NULL` in streaming mode:

RisingWave webhook tables currently reject non-NULL column options. The observed error was:

```text
Invalid input syntax: only NULL column option is supported for webhook tables
```

## DML Design

Tapdata `TapRecordEvent` is converted into websocket DML operations:

```text
TapInsertRecordEvent -> {"op":"upsert","data": after}
TapUpdateRecordEvent -> {"op":"upsert","data": after}
TapDeleteRecordEvent -> {"op":"delete","data": before}
```

The websocket protocol uses only `upsert` and `delete`. The plugin normalizes `insert` and `update` into `upsert`.

Each `writeRecordStreaming(...)` call builds one list of operations, then calls:

```java
WsIngestClient.sendBatch(operations)
```

The returned future is awaited before Tapdata offset advancement:

```text
wait for RisingWave ACK -> report success to Tapdata
ACK failure -> remove cached websocket client -> throw
```

## Batch ID Concurrency Design

RisingWave requires `dml_batch_id` to increase monotonically on one websocket stream.

Tapdata may call `writeRecordStreaming(...)` concurrently for the same target table. The plugin shares one `WsIngestClient` per table. Therefore, `dml_batch_id` allocation must happen under the same lock used for `sendText`.

Current design in `WsIngestClient.sendBatch(...)`:

```java
synchronized (sendLock) {
    dmlBatchId = dmlBatchIdGen.getAndIncrement();
    String payloadJson = buildBatchPayloadJson(dmlBatchId, operations);
    pending.put(dmlBatchId, ackFuture);
    webSocket.sendText(payloadJson, true).get(10, TimeUnit.SECONDS);
}
```

This matters. Before this fix, two Tapdata writer threads produced:

```text
send dmlBatchId=2
send dmlBatchId=1
```

RisingWave correctly rejected the stream:

```text
dml_batch_id must increase monotonically: received 1 after 2
```

## JSON Serialization Design

The websocket client serializes Java/Tapdata values into JSON without external JSON libraries.

Supported conversions include:

```text
null -> null
Boolean -> JSON boolean
Number -> JSON number, BigDecimal uses toPlainString()
Double/Float NaN/Infinity -> quoted string
byte[] -> PostgreSQL bytea hex string, for example "\\xface01"
DateTime -> timestamp string unless more specific TapType handling applies
OffsetDateTime/ZonedDateTime/Instant -> ISO offset datetime
LocalDateTime/LocalDate/LocalTime -> string
java.sql.Date/java.sql.Time/java.util.Date -> string/timestamp string
UUID -> string
CharSequence/Character -> JSON string
Map -> JSON object
Collection/array -> JSON array
RawJson -> inserted as raw JSON
```

`RisingWaveConnector.normalizeRecordForStreaming(...)` uses Tapdata field metadata before JSON serialization:

```text
TapDate + DateTime -> yyyy-mm-dd
TapTime + DateTime -> HH:mm:ss
TapDateTime + DateTime with timezone -> ISO offset datetime
TapDateTime + DateTime without timezone -> timestamp string
TapJson/TapMap or data type containing json + string -> RawJson
Map/Collection/array -> recursive normalization
```

This normalization exists because Tapdata values can arrive as Tapdata `DateTime`, maps, arrays, JSON strings, byte arrays, and other Java objects that must match RisingWave webhook JSON decoder expectations.

## Current Implementation Summary

### `WsIngestClient.java`

Current responsibilities:

```text
Build websocket URI
Send signed init frame
Build batch-envelope payload with dml_batch_id and items
Serialize DML data to JSON
Track pending ACK futures by dml_batch_id
Handle ack/error/fatal server messages
Fail all pending futures on websocket close/error/fatal
Serialize dml_batch_id under sendLock to preserve monotonic ordering
```

Important current behavior:

```text
sendBatch(empty) returns an empty future list and sends nothing
sendBatch(non-empty) returns exactly one future for the batch
parseErrorDmlBatchId checks dml_batch_id first and falls back to legacy dml_id
```

### `RisingWaveConnector.java`

Current responsibilities relevant to streaming mode:

```text
Read ingest_mode, ingestEndpoint, database, schema, webhookSecret from connection config
Use JDBC for metadata discovery and DDL even in streaming mode
Connection test checks websocket endpoint reachability only
Create webhook-backed tables in streaming mode
Do not emit NOT NULL in streaming webhook DDL
Normalize Tapdata event values for webhook JSON
Cache one WsIngestClient per table
On ACK failure, remove and close cached client so the next batch reconnects
```

### `setup_pipeline.py`

Current helper script behavior:

```text
Creates/updates PostgreSQL source connection
Creates/updates RisingWave target connection with ingest_mode=streaming
Creates and starts a Tapdata Flow from PG_CONNECTION.TABLE to RW_CONNECTION.TABLE
Supports environment overrides for source/target host, port, database, schema, user, password, table, and job names
```

Useful environment variables:

```text
TAPDATA_SERVER=127.0.0.1:3031
TAPDATA_ACCESS_CODE=3324cfdf-7d3e-4792-bd32-571638d4562f
TAPDATA_JOB_NAME=pg_to_rw_ws_smoke_v14
TAPDATA_PG_CONNECTION=PG_Source_ws_smoke_v14
TAPDATA_RW_CONNECTION=RW_Native_ws_smoke_v14
TAPDATA_TABLE=tapdata_ws_smoke_v14
TAPDATA_PG_DATABASE=postgres
TAPDATA_PG_SCHEMA=public
TAPDATA_PG_USER=william
TAPDATA_RW_DATABASE=dev
TAPDATA_RW_SCHEMA=public
TAPDATA_RW_USER=root
TAPDATA_RW_INGEST_ENDPOINT=ws://host.docker.internal:4560
TAPDATA_RW_WEBHOOK_SECRET=
```

## Validation Environment Used

RisingWave:

```text
Started with ./risedev d
SQL endpoint: localhost:4566
Websocket ingest endpoint from Tapdata container: ws://host.docker.internal:4560
Observed version string: PostgreSQL 13.14.0-RisingWave-2.9.0-alpha (unknown)
```

Tapdata:

```text
Container: tapdata-clean
Host UI/API port: 3031 mapped to container 3030
Access code: 3324cfdf-7d3e-4792-bd32-571638d4562f
Connector log inside container: /tmp/rw_connector.log
TapFlow Python package: tapflow 0.2.81
```

Plugin deploy command used:

```bash
mvn -f tapdata-plugin/pom.xml -DskipTests package
docker cp tapdata-plugin/target/risingwave-connector-1.0-SNAPSHOT.jar tapdata-clean:/tmp/risingwave-connector-new.jar
docker exec tapdata-clean java -jar /tapdata/apps/lib/pdk-deploy.jar register \
  -t http://localhost:3030 \
  -a 3324cfdf-7d3e-4792-bd32-571638d4562f \
  /tmp/risingwave-connector-new.jar
docker restart tapdata-clean
```

Wait for Tapdata after restart:

```bash
curl -fsS http://127.0.0.1:3031/api/users/generatetoken \
  -X POST \
  -H 'Content-Type: application/json' \
  -d '{"accesscode":"3324cfdf-7d3e-4792-bd32-571638d4562f"}'
```

## Validation Commands

### Prepare PostgreSQL Source Table

```bash
psql -h localhost -p 5432 -d postgres -U william -v ON_ERROR_STOP=1 <<'SQL'
SELECT pg_terminate_backend(active_pid)
FROM pg_replication_slots
WHERE active_pid IS NOT NULL;

SELECT pg_drop_replication_slot(slot_name)
FROM pg_replication_slots
WHERE active = false;

DROP TABLE IF EXISTS public.tapdata_ws_smoke_v14;
CREATE TABLE public.tapdata_ws_smoke_v14 (
  id integer PRIMARY KEY,
  customer_name text,
  amount numeric,
  status text,
  updated_at timestamp
);

INSERT INTO public.tapdata_ws_smoke_v14 VALUES
  (1, 'Alice', 10.50, 'new', '2026-04-27 10:00:00'),
  (2, 'Bob', 20.75, 'new', '2026-04-27 10:01:00');

DROP PUBLICATION IF EXISTS tapdata_ws_smoke_v14_pub;
CREATE PUBLICATION tapdata_ws_smoke_v14_pub FOR TABLE public.tapdata_ws_smoke_v14;
SQL
```

### Clean RisingWave Target Table

```bash
./risedev psql -c 'DROP TABLE IF EXISTS public.tapdata_ws_smoke_v14;'
```

### Create Tapdata Pipeline

```bash
TAPDATA_SERVER=127.0.0.1:3031 \
TAPDATA_JOB_NAME=pg_to_rw_ws_smoke_v14 \
TAPDATA_PG_CONNECTION=PG_Source_ws_smoke_v14 \
TAPDATA_RW_CONNECTION=RW_Native_ws_smoke_v14 \
TAPDATA_TABLE=tapdata_ws_smoke_v14 \
TAPDATA_PG_DATABASE=postgres \
TAPDATA_PG_SCHEMA=public \
TAPDATA_PG_USER=william \
TAPDATA_RW_DATABASE=dev \
TAPDATA_RW_SCHEMA=public \
TAPDATA_RW_USER=root \
TAPDATA_RW_INGEST_ENDPOINT=ws://host.docker.internal:4560 \
TAPDATA_RW_WEBHOOK_SECRET='' \
python3 tapdata-plugin/setup_pipeline.py
```

### Check Initial Sync

```bash
./risedev psql -c \
"SELECT id, customer_name, amount::varchar, status, updated_at::varchar
 FROM public.tapdata_ws_smoke_v14
 ORDER BY id;"
```

Expected initial output:

```text
 id | customer_name | amount | status |     updated_at
----+---------------+--------+--------+---------------------
  1 | Alice         | 10.5   | new    | 2026-04-27 10:00:00
  2 | Bob           | 20.75  | new    | 2026-04-27 10:01:00
```

### Apply CDC on PostgreSQL Source

```bash
psql -h localhost -p 5432 -d postgres -U william -v ON_ERROR_STOP=1 <<'SQL'
UPDATE public.tapdata_ws_smoke_v14
SET status='paid', amount=11.25, updated_at='2026-04-27 10:05:00'
WHERE id=1;

INSERT INTO public.tapdata_ws_smoke_v14
VALUES (3, 'Carol', 33.30, 'new', '2026-04-27 10:02:00');

DELETE FROM public.tapdata_ws_smoke_v14 WHERE id=2;

SELECT id, customer_name, amount::varchar, status, updated_at::varchar
FROM public.tapdata_ws_smoke_v14
ORDER BY id;
SQL
```

### Check Final RisingWave State

```bash
./risedev psql -c \
"SELECT id, customer_name, amount::varchar, status, updated_at::varchar
 FROM public.tapdata_ws_smoke_v14
 ORDER BY id;"
```

Expected final output:

```text
 id | customer_name | amount | status |     updated_at
----+---------------+--------+--------+---------------------
  1 | Alice         | 11.25  | paid   | 2026-04-27 10:05:00
  3 | Carol         | 33.3   | new    | 2026-04-27 10:02:00
```

## Observed Successful Connector Log

From `/tmp/rw_connector.log` in container `tapdata-clean`:

```text
createTable() sql=CREATE TABLE IF NOT EXISTS "public"."tapdata_ws_smoke_v14"
("id" integer, "customer_name" text, "amount" numeric, "status" text, "updated_at" timestamp,
PRIMARY KEY ("id")) WITH (connector = 'webhook')

WsIngestClient.sendBatch dmlBatchId=1 json={"dml_batch_id":1,"items":[{"op":"upsert","data":{"id":2,...}}]}
WsIngestClient.sendBatch dmlBatchId=2 json={"dml_batch_id":2,"items":[{"op":"upsert","data":{"id":1,...}}]}
WsIngestClient.recv json={"ack":1}
WsIngestClient.recv json={"ack":2}

WsIngestClient.sendBatch dmlBatchId=3 json={"dml_batch_id":3,"items":[{"op":"upsert","data":{"id":3,...}}]}
WsIngestClient.sendBatch dmlBatchId=4 json={"dml_batch_id":4,"items":[{"op":"upsert","data":{"id":1,...}}]}
WsIngestClient.sendBatch dmlBatchId=5 json={"dml_batch_id":5,"items":[{"op":"delete","data":{"id":2}}]}
WsIngestClient.recv json={"ack":3}
WsIngestClient.recv json={"ack":4}
WsIngestClient.recv json={"ack":5}
```

## Failure History and Fixes

### Failure: Existing Plain Table Is Not Webhook Source

Symptom:

```text
table lookup failed: Table `tapdata_ws_smoke_v9` is not with webhook source
```

Cause:

The plugin originally created a normal RisingWave table, then tried websocket ingest into it.

Fix:

In streaming mode, append:

```sql
WITH (connector = 'webhook')
```

Important operational note:

`CREATE TABLE IF NOT EXISTS ... WITH (connector='webhook')` will not convert an existing plain table into a webhook table. Drop the old target table or use a fresh table name.

### Failure: Webhook Table Rejects NOT NULL

Symptom:

```text
Invalid input syntax: only NULL column option is supported for webhook tables
```

Cause:

Tapdata marked primary key fields as not nullable, and the plugin emitted `NOT NULL`.

Fix:

In streaming mode, do not emit `NOT NULL`. Keep the primary key clause.

### Failure: Out-of-Order Batch IDs

Symptom:

```text
dml_batch_id must increase monotonically: received 1 after 2
```

Cause:

Batch IDs were allocated before entering the websocket send lock. Two Tapdata threads could allocate `1` and `2`, then send `2` before `1`.

Fix:

Allocate `dml_batch_id` inside `synchronized (sendLock)`.

### Failure: PostgreSQL Replication Slots Exhausted

Symptom:

```text
ERROR: all replication slots are in use
Hint: Free one or increase "max_replication_slots".
```

Cause:

Old Tapdata smoke jobs left inactive or active replication slots.

Cleanup:

```sql
SELECT pg_terminate_backend(active_pid)
FROM pg_replication_slots
WHERE active_pid IS NOT NULL;

SELECT pg_drop_replication_slot(slot_name)
FROM pg_replication_slots
WHERE active = false;
```

Use carefully if other real replication jobs are running.

## Build and Test Status

Passed:

```bash
mvn -f tapdata-plugin/pom.xml -DskipTests package
```

Passed earlier in the same workstream:

```bash
./risedev slt e2e_test/webhook/websocket_ingest.slt
```

Not run for this plugin-only change:

```bash
cargo clippy --all-targets --all-features
```

Reason: the changes are in the Java Tapdata plugin and Python helper script, not Rust source.

## Known Limitations and Follow-Ups

1. `WsIngestClient` uses hand-written JSON serialization. It passed the smoke path, but a future cleanup should consider a real JSON library if allowed by plugin packaging constraints.

2. `sendBatch(empty)` sends nothing. The RisingWave protocol supports empty `items` batches with ACK, but Tapdata currently does not need this path. If a future caller needs empty-batch offset barriers, implement explicit empty-envelope send.

3. Error handling is coarse. Fatal errors fail all pending futures and the connector reconnects on the next batch. This is acceptable for the smoke path but should be reviewed for production retry semantics.

4. `findAnyTableName(Connection conn, String schemaName)` remains present but is no longer used by connection test. It can be removed in cleanup if no other call sites need it.

5. `setup_pipeline.py` contains local defaults such as user `william`, access code, and host ports. It is a smoke helper, not production configuration.

6. Existing normal tables cannot be upgraded in-place to webhook tables with `CREATE TABLE IF NOT EXISTS`. Always drop the target table or use a fresh table name when switching to streaming mode.

7. The current validation covered simple scalar columns and update/insert/delete CDC. Earlier type-heavy logs existed from previous attempts, but the final successful Tapdata run was the scalar smoke table.

## Recommended Next Workspace Steps

1. Re-apply or port the changes in the three touched files.

2. Build the plugin:

```bash
mvn -f tapdata-plugin/pom.xml -DskipTests package
```

3. Start RisingWave:

```bash
./risedev d
```

4. Run websocket SLT:

```bash
./risedev slt e2e_test/webhook/websocket_ingest.slt
```

5. Deploy to Tapdata:

```bash
docker cp tapdata-plugin/target/risingwave-connector-1.0-SNAPSHOT.jar tapdata-clean:/tmp/risingwave-connector-new.jar
docker exec tapdata-clean java -jar /tapdata/apps/lib/pdk-deploy.jar register \
  -t http://localhost:3030 \
  -a 3324cfdf-7d3e-4792-bd32-571638d4562f \
  /tmp/risingwave-connector-new.jar
docker restart tapdata-clean
```

6. Repeat the `tapdata_ws_smoke_v14` validation with a fresh suffix, for example `tapdata_ws_smoke_v15`.

7. If validation passes, consider adding an automated test around `WsIngestClient.sendBatch(...)` to prove concurrent callers cannot produce out-of-order `dml_batch_id` sends.

## Final Known Good Smoke Result

Pipeline:

```text
pg_to_rw_ws_smoke_v14
```

Initial RisingWave state:

```text
1 | Alice | 10.5  | new
2 | Bob   | 20.75 | new
```

CDC operations:

```text
UPDATE id=1 amount=11.25 status=paid
INSERT id=3 Carol amount=33.30 status=new
DELETE id=2
```

Final RisingWave state:

```text
1 | Alice | 11.25 | paid
3 | Carol | 33.3  | new
```

Conclusion:

The plugin design is supportable with the current RisingWave websocket ingest protocol. The validated implementation requires the batch-envelope payload, webhook-backed streaming target DDL, no `NOT NULL` column options on webhook tables, and synchronized allocation/send of monotonic `dml_batch_id`.
