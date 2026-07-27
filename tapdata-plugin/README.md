# TapData Target Connector for RisingWave

This connector writes TapData snapshot and CDC events to RisingWave. It is a target connector and
cannot be used as a TapData source.

## Choose a write mode

| Mode | Use it for | Requirements |
|---|---|---|
| WebSocket streaming | Keyed tables with inserts, updates, and deletes. This is the default. | RisingWave 3.0+ and a primary key |
| WebSocket JSONB append-only | Keyless insert streams, such as Kafka events | RisingWave 3.0+; updates and deletes are rejected |
| JDBC | Compatibility fallback | PostgreSQL-compatible SQL endpoint |

WebSocket streaming usually provides the best throughput and latency. JDBC is still used in all
modes for version, schema, metadata, DDL, and privilege checks.

## Supported source requirements

The `1.0.0` qualification matrix covers:

| Source | Required source configuration |
|---|---|
| PostgreSQL | Row images must reconstruct the complete target row |
| MySQL 8.4 | `binlog_row_image=FULL` |
| MongoDB 7 | TapData Update Field Completion, `enableFillingModifiedData=true` |
| Kafka 3.9.1 | Use JSONB append-only for keyless JSON events |
| SQL Server 2022 | Enable database and table Change Tracking |
| Oracle 26ai | Use LogMiner with `autoLog=false` and primary-key supplemental logging |

SQL Server and Oracle passed both WebSocket streaming and JDBC tests. Use the real source primary
key as the TapData update condition.

## Build and test

The connector targets Java 11.

```bash
cd tapdata-plugin
mvn clean test
mvn package
```

The JAR is written to:

```text
target/risingwave-connector-1.0.0.jar
```

Release owners must build from a clean clone and distribute one checksummed JAR. A successful local
build alone is not a release artifact. See
[`TAPDATA_RISINGWAVE_ACCEPTANCE_TEST.md`](TAPDATA_RISINGWAVE_ACCEPTANCE_TEST.md).

## Install in TapData

Get the Access Code from **Settings -> Access Code**.

```bash
docker cp risingwave-connector-1.0.0.jar \
  tapdata:/tmp/risingwave-connector-1.0.0.jar

docker exec tapdata java \
  -jar /opt/app/tapdata/lib/pdk-deploy.jar register \
  -t http://localhost:3030 \
  -a <access-code> \
  /tmp/risingwave-connector-1.0.0.jar

docker restart tapdata
```

The deployer is commonly located at `/opt/app/tapdata/lib/pdk-deploy.jar` or
`/tapdata/apps/lib/pdk-deploy.jar`. Find it with:

```bash
docker exec tapdata sh -lc \
  'find / -name pdk-deploy.jar -print -quit'
```

Use a new connector version for an upgrade. Do not replace a released non-SNAPSHOT JAR with another
build that has the same version.

After restart, open **Connections -> Create Connection -> RisingWave**.

## Create a connection

For local RisingWave when TapData runs in Docker:

```text
Host: host.docker.internal
Port: 4566
Database: dev
Schema: public
User: root
Password: <your password, or blank for local development>
Write Mode: WebSocket streaming
Ingest Endpoint: <blank>
SSL Mode: prefer or disable
```

Do not use `localhost` to reach a database on the Docker host. Leaving **Ingest Endpoint** blank
uses `ws://<Host>:4560`.

For RisingWave Cloud:

```text
Host: <cloud SQL hostname>
Port: 4566
SSL Mode: require
Ingest Endpoint: wss://<cloud SQL hostname>
```

Cloud WebSocket ingest uses port 443 and WSS uses the Java trust store. JDBC `sslmode=require`
encrypts the SQL connection but does not provide `verify-full` hostname verification.

Click **Test** before saving. WebSocket modes should pass:

```text
Connection
Version
schema
Write
ingest_endpoint
```

JDBC mode does not run the `ingest_endpoint` check. WebSocket modes fail clearly on RisingWave
versions older than 3.0; JDBC remains available.

## Create a replication task

Use **Data Replication -> Create Task** for production CDC pipelines.

Example:

```text
Source: PostgreSQL.public.orders
Target: RisingWave.public.orders
Sync Type: Full & Incremental Sync
Update Condition: id
```

`id` must be the real source primary key in WebSocket streaming mode.

Verify the target directly in RisingWave:

```sql
select count(*) from public.orders;

select count(*) as rows,
       count(distinct id) as distinct_ids
from public.orders;

select *
from public.orders
order by id
limit 20;
```

For a keyed task, test one insert, update, delete, and primary-key change at the source. Confirm that
the old key is removed, the new row is complete, and the TapData task remains **Running**.

## Important behavior

### WebSocket streaming

- The connector creates a webhook-backed target table when needed.
- Inserts and updates are complete-row upserts.
- A primary-key change is delete-old plus upsert-new.
- Updates and deletes require the old primary-key identity.
- A single serialized record larger than 8 MiB fails explicitly.
- Automatic relational schema evolution is not supported. Unknown fields fail instead of being
  silently dropped.

### WebSocket JSONB append-only

- The target table contains one `data JSONB` column.
- Only inserts are accepted.
- Delivery is at-least-once. A retry after an ambiguous ACK loss may append a duplicate.
- Arbitrary-precision integer and decimal values are stored as JSON strings.

### JDBC

- Uses SQL `INSERT`, `UPDATE`, and `DELETE`.
- JSONB columns are bound as PostgreSQL `jsonb`.
- JDBC writes are serialized on the connector connection for correctness.

## Optional webhook secret

Set **Webhook Secret** and **RisingWave Secret Name** to validate signed WebSocket initialization.
The connector creates or reuses the named RisingWave Secret and references it from table DDL. The
secret value is not embedded in `SHOW CREATE TABLE`.

## Documentation

- User-facing connection help:
  [`risingwave_en_US.md`](src/main/resources/docs/risingwave_en_US.md) and
  [`risingwave_zh_CN.md`](src/main/resources/docs/risingwave_zh_CN.md)
- Maintainer acceptance runbook:
  [`TAPDATA_RISINGWAVE_ACCEPTANCE_TEST.md`](TAPDATA_RISINGWAVE_ACCEPTANCE_TEST.md)
- Release checklist:
  [`TAPDATA_RISINGWAVE_PRODUCTION_READINESS.md`](TAPDATA_RISINGWAVE_PRODUCTION_READINESS.md)
