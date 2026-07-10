# Tapdata PDK Connector for RisingWave

A [Tapdata PDK](https://github.com/tapdata/tapdata) connector that lets Tapdata write data to
RisingWave. Supports two write modes:

- **JDBC mode** (default): standard SQL `INSERT`/`UPDATE`/`DELETE` via PostgreSQL wire protocol
- **WebSocket streaming mode**: high-throughput streaming DML via RisingWave's WebSocket ingest
  endpoint with asynchronous ACKs

## What it does

- Replicates tables from any Tapdata source (e.g. PostgreSQL, MySQL, Mock Source) into RisingWave
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
| RisingWave | 3.0.0+  | WebSocket streaming requires 3.0.0+ (PR #25444); JDBC mode works with any version |

## Build

The connector depends on Tapdata PDK API published to Tapdata's Nexus snapshot repository.

```bash
cd tapdata-plugin
mvn package -DskipTests
# Produces: target/risingwave-connector-1.0-SNAPSHOT.jar
```

> **Note:** The `pom.xml` uses `io.tapdata:tapdata-pdk-api:2.0.8-SNAPSHOT` and
> `io.tapdata:tapdata-api:2.0.8-SNAPSHOT` from `https://nexus.tapdata.net/repository/maven-snapshots/`.
> Match the PDK version to the Tapdata engine version in your deployment (check
> `BOOT-INF/lib/tapdata-pdk-api-*.jar` inside the engine's `tm.jar`).

## Deploy to Tapdata

### 1. Start Tapdata

```bash
docker run -d --name tapdata \
  --add-host=host.docker.internal:host-gateway \
  -p 3030:3030 \
  -e app_type=DAAS \
  ghcr.io/tapdata/tapdata:latest
```

Wait ~3 minutes for startup, then open http://localhost:3030.
Default login: `admin@admin.com` / `admin`.

### 2. Register the connector

```bash
# Copy JAR into the container
docker cp target/risingwave-connector-1.0-SNAPSHOT.jar tapdata:/tmp/rw-connector.jar

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
| Write Mode | No | jdbc | `jdbc` or `streaming` |
| Ingest Endpoint | No | ws://host.docker.internal:4560 | WebSocket ingest base URL (streaming only) |
| Webhook Secret | No | - | HMAC secret for WebSocket init frame (streaming only) |
| SSL Mode | No | prefer | `prefer`, `require`, or `disable` |
| Extra Parameters | No | - | Additional JDBC params, e.g. `socketTimeout=60` |
| Timezone | No | - | Timezone for datetime processing |

### Cloud vs Local

| Deployment | SQL Port | WS Ingest Port | SSL |
|------------|----------|---------------|-----|
| Local (`risedev`) | 4566 | 4560 | prefer (no TLS) |
| RisingWave Cloud | 4566 | 443 (`wss://`) | require (TLS SNI routing) |

For RisingWave Cloud, set:
- **SSL Mode**: `require`
- **Ingest Endpoint**: `wss://your-cluster.risingwave.cloud`
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
TAPDATA_RW_HOST=<rw-host> \
TAPDATA_RW_PORT=4566 \
TAPDATA_RW_INGEST_ENDPOINT=ws://host.docker.internal:4560 \
python3 setup_pipeline.py
```

## Architecture

```
RisingWaveConnector.java    - PDK connector (init, DDL, DML, two write modes)
WsIngestClient.java         - WebSocket client (init, batch, ACK, HMAC signing)
spec_risingwave.json        - Tapdata connector spec (form schema, i18n, data types)
icons/risingwave.png        - Connector icon
docs/risingwave_en_US.md    - English documentation
docs/risingwave_zh_CN.md    - Chinese documentation
```

## Known limitations

- `ON CONFLICT DO UPDATE` (upsert) requires the target table to have a primary key
- Array and composite PostgreSQL types are not mapped; they fall back to `text`
- Webhook tables reject `NOT NULL`; streaming mode omits it in DDL
- Existing plain tables cannot be upgraded to webhook tables in-place
- DDL/schema changes are not propagated automatically
- `varchar(N)` and `numeric(p,s)` are simplified for RisingWave compatibility
- Target-only (cannot be used as a source)
