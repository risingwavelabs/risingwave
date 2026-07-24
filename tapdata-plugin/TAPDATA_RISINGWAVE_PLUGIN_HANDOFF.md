# TapData RisingWave Connector Maintainer Handoff

This document explains the implementation boundaries and the work required to maintain or release
the connector. User installation and configuration instructions are in `README.md` and the localized
connection help under `src/main/resources/docs/`.

## Current status

- Target-only TapData PDK connector.
- WebSocket streaming is the default write mode and requires RisingWave 3.0.0 or later.
- WebSocket JSONB append-only and JDBC fallback modes are also supported.
- JDBC remains available for writes and is always used for metadata, DDL, version, schema, and
  privilege checks.
- The connector is versioned as `1.0.0` and uses TapData PDK/API `2.0.8-SNAPSHOT`.
- The shaded runtime dependencies are pgjdbc `42.7.12` and Jackson `2.18.9`.
- No artifact built before the latest source commit should be distributed. Build and qualify one
  final JAR after the canonical repository and release workflow are agreed.

## Code map

| File | Responsibility |
|---|---|
| `RisingWaveConnector.java` | PDK lifecycle, capabilities, discovery, DDL, and three write modes |
| `RisingWaveConnectionTester.java` | Connection, version, schema, write, and WebSocket pre-checks |
| `RisingWaveConfig.java` | Validated immutable connection configuration |
| `RisingWaveJdbc.java` | PostgreSQL wire-protocol connection creation |
| `RisingWaveSql.java` | SQL quoting, type mapping, and webhook DDL fragments |
| `RisingWaveCdcNormalizer.java` | Shared CDC row semantics for WebSocket and JDBC |
| `RisingWaveValueConverter.java` | Target-aware JDBC and JSON value conversion |
| `RisingWaveWebhookSecret.java` | RisingWave Secret creation, lookup, validation, and cleanup |
| `streaming/WsIngestClient.java` | WebSocket init, DML batches, ACK handling, signing, and payload limits |
| `spec_risingwave.json` | TapData form, capabilities, data types, and localized help wiring |

## Write-mode contracts

### WebSocket streaming

- Requires a primary key and RisingWave 3.0.0 or later.
- Auto-creates a webhook-backed table when the target does not exist.
- Converts inserts and updates into complete-row upserts.
- Converts deletes into primary-key retractions.
- Sends ordered `dml_batch_id` frames and reports success only after RisingWave ACKs the frame.
- Splits multi-record payloads below an 8 MiB safety limit; one record larger than the limit fails
  explicitly.
- The supported workflow lets the connector own the target table schema. Target-only columns added
  manually are outside that contract and may become `NULL` during a full-row upsert.

### WebSocket JSONB append-only

- Creates exactly one `data JSONB` column and does not require a primary key.
- Accepts inserts only; updates and deletes fail explicitly.
- Provides at-least-once delivery. Retrying after a lost ACK can append a duplicate document.
- Stores arbitrary-precision integers and decimals as JSON strings to avoid silent precision loss.

### JDBC fallback

- Uses SQL `INSERT`, `UPDATE`, and `DELETE` through the PostgreSQL wire protocol.
- Supports keyless models when a complete before image is available.
- Serializes writes over the connector's shared JDBC connection.
- Uses `FLUSH` only where RisingWave read-after-write ordering requires it.

## CDC semantics

Both typed write transports consume the same normalized update:

1. Validate source fields against the TapData table model.
2. Combine available `before`, `after`, and top-level `removedFields` into a complete post-image.
3. Treat replace events as authoritative; omitted known fields become SQL `NULL`.
4. Preserve old and new primary-key identities and emit delete plus upsert when the key changes.
5. Fail closed when a complete row or required old identity cannot be reconstructed.

Source requirements:

- PostgreSQL must provide enough row data to reconstruct typed updates. Unavailable TOAST values
  fail explicitly when neither image contains the value.
- MySQL must use `binlog_row_image=FULL`.
- MongoDB must keep TapData Update Field Completion enabled
  (`enableFillingModifiedData=true`, the TapData default).
- Kafka was qualified through JSONB append-only mode.
- SQL Server 2022 was qualified with database/table Change Tracking enabled.
- Oracle 26ai was qualified with ARCHIVELOG, primary-key supplemental logging, manual LogMiner,
  and `SET CONTAINER` for the common mining user.
- SQL Server and Oracle passed snapshot, insert/update/delete, primary-key change, and type
  mapping through both WebSocket streaming and JDBC fallback. WebSocket tasks also passed
  stop/start checkpoint recovery. The target update condition must be the real primary key,
  not TapFlow's generic `_id` default.

## WebSocket protocol

The client connects to:

```text
<ingest-endpoint>/ingest/<database>/<schema>/<table>
```

It sends a signed init frame when a webhook secret is configured, followed by ordered DML frames:

```json
{"type":"init","timestamp":1760000000000}
{"dml_batch_id":1,"items":[{"op":"upsert","data":{"id":1,"name":"A"}}]}
```

RisingWave replies with `{"ack":1}` after persistence or a terminal `{"fatal":"..."}` response.
The connector discards a failed client so the next TapData retry establishes a new stream. Keyed
replay is idempotent because full-row upserts overwrite the same primary key.

## Secrets and TLS

- When a Webhook Secret is configured, generated tables reference a RisingWave Secret using
  `VALIDATE SECRET`; the secret value is not embedded in `SHOW CREATE TABLE` output.
- The client retains the value only to sign the WebSocket init payload.
- JDBC SSL modes are `prefer`, `require`, and `disable`.
- WSS uses the Java runtime trust store. The connector does not expose custom CA or mTLS upload
  controls.
- Connector logs exclude record payloads, passwords, webhook secrets, signatures, and generated
  secret DDL.

## Build and test

```bash
cd tapdata-plugin
mvn clean test
mvn -Drisingwave.it=true -Dtest=RisingWaveConnectionTestIT test
mvn clean package
```

Optional suites:

```bash
# Persisted-but-lost-ACK replay
python3 scripts/ws_ack_drop_proxy.py
mvn -Drisingwave.it=true \
  -Drisingwave.ackLossProxyEndpoint=ws://127.0.0.1:4561 \
  -Dtest=RisingWaveAckLossIT test

# Local JDBC/WebSocket comparison
mvn -Drisingwave.it=true -Drisingwave.benchmark=true \
  -Dtest=RisingWaveWriteBenchmarkIT test
```

The live suite expects RisingWave SQL on port `4566` and WebSocket ingest on port `4560` unless
overridden with test properties.

## Release handoff

Before publishing:

1. Confirm whether the canonical source belongs in this repository or `tapdata/tapdata-connectors`.
2. Rebase onto the destination repository's current main branch.
3. Ensure CI builds and tests the connector when its files change.
4. Build from a clean committed worktree using the frozen TapData dependency set.
5. Run unit, live RisingWave, ACK-loss, and exact-JAR TapData smoke tests.
6. Record the final Git commit, manifest, JDK/Maven versions, artifact size, and SHA-256 in
   `TAPDATA_RISINGWAVE_PRODUCTION_READINESS.md`.
7. Archive the exact JAR and frozen dependency subset in an immutable artifact store.

Do not add transport state machines or source-specific heuristics without a reproducible failure in
a supported workflow. The current shared CDC normalizer is the intended semantic boundary between
TapData events and the WebSocket/JDBC transports.
