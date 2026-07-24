# Tapdata PDK Connector for RisingWave

A [Tapdata PDK](https://github.com/tapdata/tapdata) connector that lets Tapdata write data to
RisingWave. Supports three write modes:

- **WebSocket streaming mode** (default): streaming DML via RisingWave's WebSocket ingest
  endpoint with asynchronous ACKs
- **WebSocket JSONB append-only mode**: stores each inserted source record as one JSONB document
  in a `data` column; supports keyless models but rejects updates and deletes
- **JDBC mode**: compatible fallback using standard SQL `INSERT`/`UPDATE`/`DELETE` via the
  PostgreSQL wire protocol

Maintainer architecture and release requirements are summarized in
[`TAPDATA_RISINGWAVE_PLUGIN_HANDOFF.md`](TAPDATA_RISINGWAVE_PLUGIN_HANDOFF.md) and
[`TAPDATA_RISINGWAVE_PRODUCTION_READINESS.md`](TAPDATA_RISINGWAVE_PRODUCTION_READINESS.md).
The complete manual and automated acceptance workflow is in
[`TAPDATA_RISINGWAVE_ACCEPTANCE_TEST.md`](TAPDATA_RISINGWAVE_ACCEPTANCE_TEST.md).
The final release decision must use CI and one exact, checksummed artifact.

## What it does

- The `1.0.0` source matrix passed PostgreSQL, MySQL 8.4 with `binlog_row_image=FULL`,
  MongoDB 7 with `enableFillingModifiedData=true`, SQL Server 2022 Change Tracking,
  Oracle 26ai LogMiner, and Kafka 3.9.1 through JSONB append-only mode. SQL Server and
  Oracle were exercised through both WebSocket streaming and JDBC fallback. The final
  canonical JAR must be identified by its release checksum rather than its version alone
- Supports batch snapshot sync and CDC (change-data-capture) streaming
- In streaming mode, auto-creates webhook-backed tables (`WITH (connector = 'webhook')`)
- Handles RisingWave SQL dialect differences from PostgreSQL automatically
  (strips `varchar(N)` lengths, `numeric(p,s)` precision, etc.)
- HMAC-SHA256 signed WebSocket init frame for `VALIDATE`-protected tables

## Prerequisites

| Component  | Version | Notes |
|------------|---------|-------|
| Java       | 11+     | Build only; runtime provided by Tapdata engine |
| Maven      | 3.8+    | |
| Docker     | any     | Runs Tapdata |
| RisingWave | 3.0.0+  | WebSocket streaming requires 3.0.0+; JDBC mode works with any version |

## Build

The connector depends on Tapdata PDK API published to Tapdata's Nexus snapshot repository.

```bash
cd tapdata-plugin
mvn package -DskipTests
# Produces: target/risingwave-connector-1.0.0.jar
```

> **Note:** The `pom.xml` uses `io.tapdata:tapdata-pdk-api:2.0.8-SNAPSHOT` and
> `io.tapdata:tapdata-api:2.0.8-SNAPSHOT` from `https://nexus.tapdata.net/repository/maven-snapshots/`.
> Match the PDK version to the Tapdata engine version in your deployment (check
> `BOOT-INF/lib/tapdata-pdk-api-*.jar` inside the engine's `tm.jar`).

## Test

Run version parsing and compatibility tests without external services:

```bash
mvn test
```

With a local RisingWave instance listening on SQL port `4566` and WebSocket ingest port `4560`,
run the live connection pre-check tests:

```bash
mvn -Drisingwave.it=true -Dtest=RisingWaveConnectionTestIT test
```

The live tests create a temporary webhook-backed table and perform a real WebSocket init,
DML write, and ACK exchange. They also cover signed ingestion, an unreachable endpoint,
a missing schema, logins that lack JDBC or streaming DDL privileges, and cleanup of temporary
probe tables and users.

To reproduce the persisted-but-lost-ACK boundary, install the Python `websockets` package, run the
fault proxy in one terminal, and run the opt-in integration test in another:

```bash
python3 scripts/ws_ack_drop_proxy.py

mvn -Drisingwave.it=true \
  -Drisingwave.ackLossProxyEndpoint=ws://127.0.0.1:4561 \
  -Dtest=RisingWaveAckLossIT test
```

The test proves that retrying a persisted keyed insert remains one row, while retrying the same
keyless JSONB document appends a second row. The proxy and test are not part of the default suite.

The local WebSocket/JDBC comparison is also opt-in:

```bash
mvn -Drisingwave.it=true -Drisingwave.benchmark=true \
  -Drisingwave.benchmark.rows=10000 -Drisingwave.benchmark.batchSize=1000 \
  -Dtest=RisingWaveWriteBenchmarkIT test
```

## Deploy to Tapdata

### 1. Start Tapdata

```bash
TAPDATA_IMAGE='ghcr.io/tapdata/tapdata:REPLACE_WITH_APPROVED_VERSION'

docker run -d --name tapdata \
  --add-host=host.docker.internal:host-gateway \
  -p 3030:3030 \
  -e app_type=DAAS \
  "$TAPDATA_IMAGE"
```

For production qualification and deployment, use an internally approved TapData version or pin
the image by digest (`ghcr.io/tapdata/tapdata@sha256:...`). Do not use the mutable `latest` tag:
the TapData UI, Agent, and connector APIs must come from a compatible release set.

Wait ~3 minutes for startup, then open http://localhost:3030.
Default login: `admin@admin.com` / `admin`.

### 2. Register the connector

```bash
# Copy JAR into the container
docker cp target/risingwave-connector-1.0.0.jar tapdata:/tmp/rw-connector.jar

# Register via pdk-deploy (use -l to replace latest version)
docker exec tapdata java -jar /tapdata/apps/lib/pdk-deploy.jar register \
  -t http://localhost:3030 \
  -a <your-access-code> \
  -l \
  /tmp/rw-connector.jar
```

Your access code is shown in the Tapdata UI under **Settings -> Access Code**.

### 3. Restart Tapdata to load the new connector

```bash
docker restart tapdata
```

### 4. Verify in UI

Go to **Connections -> Create Connection**. You should see **RisingWave** with an icon.
Click it to see the connection configuration help (from `docs/risingwave_en_US.md`).

## Connection parameters

| Field | Required | Default | Description |
|-------|----------|---------|-------------|
| host | Yes | - | RisingWave hostname or IP |
| port | Yes | 4566 | RisingWave SQL port |
| database | Yes | dev | Database name |
| schema | No | public | Target schema |
| user | Yes | root | Database user |
| password | No | - | Database password |
| Write Mode | No | streaming | `streaming` (recommended), `streaming_jsonb` (append-only), or `jdbc` (compatible fallback) |
| Ingest Endpoint | No | Blank | Leave blank to use `ws://<Host>:4560` automatically; set explicitly for TLS or a separate ingest host |
| Webhook Secret | No | - | HMAC secret for WebSocket init frame (streaming only) |
| SSL Mode | No | prefer | `prefer`, `require`, or `disable` |
| Extra Parameters | No | - | Additional JDBC params, e.g. `socketTimeout=60` |
| Timezone | No | - | RisingWave session timezone as an IANA name, e.g. `UTC` or `Asia/Shanghai` |

### Cloud vs Local

| Deployment | SQL Port | WS Ingest Port | SSL |
|------------|----------|---------------|-----|
| Local (`risedev`) | 4566 | 4560 | prefer (no TLS) |
| RisingWave Cloud | 4566 | 443 (`wss://`) | require (TLS SNI routing) |

For RisingWave Cloud, set:
- **SSL Mode**: `require`
- **Ingest Endpoint**: `wss://<your-cloud-sql-host>` (no port; Cloud uses 443)

TLS uses the Java runtime's trusted CA store. Custom CA files and mutual-TLS client certificates
aren't supported by this connector; the generic TapData certificate-upload panel is intentionally
not exposed. RisingWave Cloud's publicly trusted server certificate doesn't require these uploads.
- **Webhook Secret**: the secret used in the table's `VALIDATE` clause

## Run the end-to-end demo (PostgreSQL -> RisingWave)

The `setup_pipeline.py` script creates a full pipeline using the TapFlow Python SDK.

### Prerequisites for the demo

1. **PostgreSQL** running locally on port 5432 with logical replication enabled
2. **RisingWave** running (local via `./risedev d` or cloud)
3. **TapFlow SDK** installed: `pip install tapflow`

### Run the pipeline

```bash
TAPDATA_SERVER=127.0.0.1:3030 \
TAPDATA_ACCESS_CODE=<your-access-code> \
TAPDATA_RW_HOST=<rw-host> \
TAPDATA_RW_PORT=4566 \
TAPDATA_RW_INGEST_ENDPOINT=ws://host.docker.internal:4560 \
python3 setup_pipeline.py
```

## Architecture

WebSocket streaming requires every replicated table to have a primary key. Each update is adapted
once into a complete target row: normal events combine the available `before` and `after` images,
and top-level `removedFields` become SQL `NULL`. The update fails if those images cannot safely
form every target column. Replace events treat `after` as authoritative, so omitted target columns
become `NULL`. An after-only MongoDB event with a sole `_id` key uses the same authoritative rule.
TapData's **Update Field Completion** (`enableFillingModifiedData`) must remain enabled for MongoDB
source tasks; TapData enables it by default. If it is disabled, a partial patch is indistinguishable
from a complete sparse document at the target and omitted columns may become `NULL`.

WebSocket and JDBC consume that same complete row. WebSocket sends an upsert; JDBC updates all
non-key columns. If a primary key changes, both modes write the new row and retract the old
identity. Nested document changes must include the complete parent column.

Keyed updates and deletes must carry the old primary-key identity. The sole `_id` primary-key case
accepts an after-only update because MongoDB `_id` is immutable. Other missing old identities fail
closed instead of leaving an unretracted row. Unknown relational fields also fail explicitly while
automatic schema evolution is unsupported.

WebSocket JSONB append-only mode creates each target as `data JSONB` without a primary key and
stores the complete source record as the JSON document. It accepts insert events only. Update and
delete events fail explicitly, and retries can append duplicates because this mode has no row
identity or deduplication key. To avoid silent floating-point rounding in RisingWave's JSON-number
decoder, Java `BigDecimal`/`BigInteger` values are stored as JSON strings in this mode.

Runtime diagnostics use Tapdata's connector logger and contain batch metadata only. Record payloads,
webhook secrets, signatures, and generated DDL are not logged by the connector.

```
RisingWaveConnector.java    - PDK connector (init, DDL, DML, three write modes)
RisingWaveCdcNormalizer.java - small adapter from TapData updates to complete target rows
RisingWaveValueConverter.java - field-aware JDBC and WebSocket value conversion
WsIngestClient.java         - WebSocket client (init, batch, ACK, HMAC signing)
spec_risingwave.json        - Tapdata connector spec (form schema, i18n, data types)
icons/risingwave.png        - Connector icon
docs/risingwave_en_US.md    - English documentation
docs/risingwave_zh_CN.md    - Chinese documentation
```

## Known limitations

- JDBC keyed upsert uses an explicit update-then-insert compatibility path
- WebSocket streaming rejects source models without a primary key; use JDBC mode for keyless tables
- WebSocket JSONB append-only supports keyless inserts but not updates, deletes, or deduplicated retries
- WebSocket JSONB append-only stores arbitrary-precision decimal/integer values as JSON strings
  so their exact value is preserved
- JDBC updates on keyless tables use the full before image; retries remain at-least-once because
  a keyless row has no stable deduplication identity
- Typed updates require a complete post-image. PostgreSQL/MySQL images must form the whole row;
  MongoDB requires **Update Field Completion** (`enableFillingModifiedData=true`) on the source task
  node; this is TapData's default and must not be disabled
- The legacy Kafka source connector used in qualification required
  `--add-exports=java.security.jgss/sun.security.krb5=ALL-UNNAMED` when TapData ran on Java 17;
  this is a Kafka source/runtime requirement, not a RisingWave target requirement
- That legacy Kafka source inferred arrays as `STRING`. JSONB mode preserved the source-emitted
  value; native array typing still requires qualification with TapData's current Kafka connector
- SQL Server and Oracle tasks must use their real primary key as the target update condition;
  TapFlow-generated tasks can otherwise retain the generic `_id` default
- Oracle 19c+ must use manual LogMiner (`autoLog=false`); Continuous Miner is no longer supported
- PostgreSQL updates with unavailable TOAST values fail if `before` and `after` cannot reconstruct
  the column; the connector never guesses the missing value
- Unknown relational fields fail explicitly because DDL/schema changes are not propagated
  automatically; JSONB append-only documents may add nested document fields without target DDL
- Array and composite PostgreSQL types are not mapped; they fall back to `text`
- Webhook tables reject `NOT NULL`; streaming mode omits it in DDL
- Existing plain tables cannot be upgraded to webhook tables in-place
- `varchar(N)` and `numeric(p,s)` are simplified for RisingWave compatibility
- Target-only (cannot be used as a source)
