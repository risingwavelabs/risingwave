## Connection configuration help

### Overview

RisingWave is a Postgres-compatible streaming database. This connector writes data to RisingWave tables using the PostgreSQL wire protocol (JDBC mode) or the RisingWave WebSocket ingest endpoint (streaming mode).

### Supported versions

- **JDBC mode**: works with any RisingWave version (PostgreSQL wire protocol)
- **WebSocket streaming mode**: requires RisingWave **3.0.0+** (WebSocket ingest endpoint, PR #25444)

### Write modes

- **JDBC (compatible)** — standard SQL `INSERT`/`UPDATE`/`DELETE` via PostgreSQL JDBC. Simple setup, works with any RisingWave version.
- **WebSocket streaming** — streaming DML batches over the RisingWave WebSocket ingest endpoint with asynchronous ACKs. High throughput, lower latency. Requires RisingWave 3.0.0+.

### Prerequisites

#### 1. Target table creation (automatic)

In streaming mode, the connector **automatically creates** the target table with `WITH (connector = 'webhook')` when it does not exist. You do **not** need to create the table manually.

If a **Webhook Secret** is configured in the Tapdata connection, the auto-created table includes a `VALIDATE` clause for HMAC-SHA256 signature verification:

```sql
-- Auto-generated DDL (example):
CREATE TABLE IF NOT EXISTS "public"."orders" (
    "id" integer,
    "customer_name" varchar,
    "amount" numeric,
    PRIMARY KEY ("id")
) WITH (connector = 'webhook') VALIDATE AS secure_compare(
    headers->>'x-rw-signature',
    'sha256=' || encode(hmac('your-secret', payload, 'sha256'), 'hex')
);
```

If no Webhook Secret is configured, the table is created **without** `VALIDATE`, meaning anyone with network access to the ingest endpoint can write to it.

> **Note:** An existing plain table cannot be converted to a webhook table with `CREATE TABLE IF NOT EXISTS`. If the table already exists as a non-webhook table, drop it first.

#### 2. WebSocket ingest endpoint

The WebSocket ingest endpoint is separate from the SQL port:

| Deployment | SQL port | WebSocket ingest port |
|------------|----------|----------------------|
| Local (`risedev`) | 4566 | 4560 |
| RisingWave Cloud | 4566 | 443 (via `wss://`) |

**Ingest Endpoint** should be the base URL without the path:
- Local: `ws://host:4560`
- Cloud: `wss://your-cluster.risingwave.cloud`

The connector appends `/ingest/{database}/{schema}/{table}` automatically.

#### 3. SSL

- **Local deployments**: SSL is not required. The connector uses `sslmode=prefer` by default.
- **RisingWave Cloud**: SSL is required for the cloud proxy to route connections via TLS SNI. Use `sslmode=require` or `prefer`.

### Connection fields

| Field | Required | Default | Description |
|-------|----------|---------|-------------|
| Host | Yes | — | RisingWave server hostname or IP |
| Port | Yes | 4566 | RisingWave SQL port |
| Database | Yes | dev | RisingWave database name |
| Schema | No | public | Target schema |
| User | Yes | root | Database user |
| Password | No | — | Database password |
| Write Mode | No | jdbc | `jdbc` or `streaming` |
| Ingest Endpoint | No | ws://host.docker.internal:4560 | WebSocket ingest base URL (streaming mode only) |
| Webhook Secret | No | — | HMAC secret for signing the WebSocket init frame (streaming mode only) |

### Limitations

1. **Webhook tables reject `NOT NULL`**: In streaming mode, the connector omits `NOT NULL` constraints when creating tables.
2. **No in-place upgrade**: An existing plain table cannot be converted to a webhook table with `CREATE TABLE IF NOT EXISTS`. Drop the table first or use a new name.
3. **DDL not supported**: Schema changes (add/drop/alter column) are not propagated automatically.
4. **`varchar(N)` and `numeric(p,s)` are simplified**: The connector strips length/precision in DDL generation for RisingWave compatibility.
5. **Target-only**: This connector can only be used as a target (sink), not as a source.
