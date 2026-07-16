## Connection configuration help

### Overview

RisingWave is a Postgres-compatible streaming database. This connector writes data to RisingWave tables using the PostgreSQL wire protocol (JDBC mode) or the RisingWave WebSocket ingest endpoint (streaming mode).

### Supported versions

- **WebSocket streaming mode**: requires RisingWave **3.0.0+** (WebSocket ingest endpoint)
- **JDBC mode**: works with any RisingWave version (PostgreSQL wire protocol)

### Write modes

- **WebSocket streaming (default and recommended)** — streaming DML batches over the RisingWave WebSocket ingest endpoint with asynchronous ACKs. Designed for high throughput and lower latency. Requires RisingWave 3.0.0+.
- **WebSocket JSONB append-only** — creates a single `data JSONB` column and stores each inserted
  source record as one JSON document. It permits keyless models but rejects update and delete
  events. Retries may append duplicates because there is no row identity. Arbitrary-precision
  decimal and integer values are stored as JSON strings to prevent silent precision loss.
- **JDBC (compatible fallback)** — standard SQL `INSERT`/`UPDATE`/`DELETE` via PostgreSQL JDBC. Simple setup and works with any RisingWave version.

WebSocket streaming requires every source model to have a primary key. Streaming inserts and
same-primary-key updates are sent as upserts. When an update changes a
primary-key value, the connector sends `delete(before)` and `upsert(after)` in the same WebSocket
batch. Deletes use the before image. For keyless tables, use WebSocket JSONB append-only when the
source produces insert events only; use JDBC when update or delete events must be supported.

### Prerequisites

#### 1. Target table creation (automatic)

In streaming mode, the connector **automatically creates** the target table with `WITH (connector = 'webhook')` when it does not exist. You do **not** need to create the table manually.

If a **Webhook Secret** is configured, the connector references a RisingWave catalog Secret so
the value is not exposed by `SHOW CREATE TABLE`. The connector creates and rotates that Secret
from **Webhook Secret**. Leave **RisingWave Secret Name** blank to generate a per-table name, or
set it to choose a stable name that may be shared by multiple target tables. This requires Secret
Management and `CREATE SECRET` permission.

```sql
-- Auto-generated DDL (example):
CREATE TABLE "public"."orders" (
    "id" integer,
    "customer_name" varchar,
    "amount" numeric,
    PRIMARY KEY ("id")
) WITH (connector = 'webhook') VALIDATE SECRET "tapdata_webhook_..." AS secure_compare(
    headers->>'x-rw-signature',
    'sha256=' || encode(hmac("tapdata_webhook_...", payload, 'sha256'), 'hex')
);
```

If no Webhook Secret is configured, the table is created **without** `VALIDATE`, meaning anyone with network access to the ingest endpoint can write to it.

> **Note:** An existing plain table cannot be converted to a webhook table automatically. If the table already exists as a non-webhook table, drop it first.

#### 2. WebSocket ingest endpoint

The WebSocket ingest endpoint is separate from the SQL port:

| Deployment | SQL port | WebSocket ingest port |
|------------|----------|----------------------|
| Local (`risedev`) | 4566 | 4560 |
| RisingWave Cloud | 4566 | 443 (via `wss://`) |

**Ingest Endpoint** should be the base URL without the path:
- Local: `ws://host:4560`
- Cloud: `wss://<your-cloud-sql-host>` (no port; Cloud uses 443)

The connector appends `/ingest/{database}/{schema}/{table}` automatically.

### Connection pre-checks

The connection test validates the SQL login, RisingWave version, and configured schema. In
JDBC mode it creates a temporary table and verifies SQL insert, update, delete, and drop. In
WebSocket streaming mode it creates a temporary webhook-backed table, opens the configured
WebSocket endpoint, sends a signed init frame when a secret is configured, writes one DML
batch, waits for a RisingWave ACK, and drops the temporary table. This verifies the endpoint,
ingest route, signature configuration, and required target-table DDL privileges together.

#### 3. SSL

- **Local deployments**: SSL is not required. The connector uses `sslmode=prefer` by default.
- **RisingWave Cloud**: SSL is required for the cloud proxy to route connections via TLS SNI. Use `sslmode=require` or `prefer`.
- TLS uses the Java runtime's trusted CA store. Custom CA uploads and mutual-TLS client certificates
  are not supported; RisingWave Cloud's publicly trusted certificate does not require them.

### Connection fields

| Field | Required | Default | Description |
|-------|----------|---------|-------------|
| Host | Yes | — | RisingWave server hostname or IP |
| Port | Yes | 4566 | RisingWave SQL port |
| Database | Yes | dev | RisingWave database name |
| Schema | No | public | Target schema |
| User | Yes | root | Database user |
| Password | No | — | Database password |
| Write Mode | No | streaming | `streaming` (recommended), `streaming_jsonb` (append-only), or `jdbc` (compatible fallback) |
| Ingest Endpoint | No | Blank | Leave blank to use `ws://<Host>:4560` automatically; set explicitly for TLS or a separate ingest host |
| Webhook Secret | No | — | HMAC secret for signing the WebSocket init frame (streaming mode only) |
| RisingWave Secret Name | No | Auto-generated | Optional stable name for the protected Secret created and rotated from Webhook Secret; blank generates a per-table name |

### Limitations

1. **Webhook tables reject `NOT NULL`**: In streaming mode, the connector omits `NOT NULL` constraints when creating tables.
2. **No in-place upgrade**: An existing plain table cannot be converted to a webhook table automatically. Drop the table first or use a new name.
3. **DDL not supported**: Schema changes (add/drop/alter column) are not propagated automatically.
4. **`varchar(N)` and `numeric(p,s)` are simplified**: The connector strips length/precision in DDL generation for RisingWave compatibility.
5. **Target-only**: This connector can only be used as a target (sink), not as a source.
6. **JSONB exact numbers use strings**: In JSONB append-only mode, arbitrary-precision decimal and
   integer values are stored as JSON strings because RisingWave JSON numbers can round them.
7. **Streaming updates require a complete row**: RisingWave WebSocket upserts replace the full
   row. When a source emits a partial update image, the connector reconstructs the complete row
   from its `before` and `after` images. If neither image contains all target columns, the update
   fails rather than silently converting absent columns to `NULL`. A source `removedFields` entry
   for a top-level column is written as SQL `NULL`. Removing a nested field such as `profile.name`
   requires the source to provide a complete post-image for the parent `profile` column. MySQL
   sources must use `binlog_row_image=FULL`; TapData's MySQL CDC reader rejects `MINIMAL` row
   images before they reach this target connector.
8. **Large records have a frame limit**: The connector splits batches into ordered WebSocket
   payloads below 8 MiB. A single source record larger than that cannot be split safely and fails
   with an explicit error.
9. **JDBC visibility barriers**: RisingWave applies JDBC inserts asynchronously. The connector
   issues `FLUSH` only before an update or delete that depends on an unflushed insert, and at the
   end of a write batch; independent updates and deletes can remain in the same batch.
10. **Binary values in JSONB mode**: `byte[]` values are stored as JSON strings using PostgreSQL
    bytea hex text (`\\x<hex>`). JSONB has no native binary scalar, so consumers should decode this
    representation when they need the original bytes.
