## RisingWave connection

This is a target-only connector. It writes TapData snapshot and CDC events to RisingWave.

### Choose a write mode

| Mode | Use it for | Requirements |
|---|---|---|
| **WebSocket streaming** | Keyed inserts, updates, and deletes. Default and recommended. | RisingWave 3.0+ and a primary key |
| **WebSocket JSONB append-only** | Keyless insert streams | RisingWave 3.0+; updates and deletes are rejected |
| **JDBC** | Compatibility fallback | PostgreSQL-compatible SQL endpoint |

WebSocket streaming normally provides the best throughput and latency. JDBC is still used in every
mode for version, schema, metadata, DDL, and privilege checks.

### Local connection example

When TapData runs in Docker and RisingWave runs on the Docker host:

```text
Host: host.docker.internal
Port: 4566
Database: dev
Schema: public
User: root
Write Mode: WebSocket streaming
Ingest Endpoint: <blank>
SSL Mode: prefer or disable
```

Do not use `localhost` to reach the Docker host. Leaving **Ingest Endpoint** blank uses
`ws://<Host>:4560`.

### RisingWave Cloud example

```text
Host: <cloud SQL hostname>
Port: 4566
SSL Mode: require
Ingest Endpoint: wss://<cloud SQL hostname>
```

Cloud WebSocket ingest uses port 443. WSS uses the Java trust store. JDBC `sslmode=require`
encrypts the SQL connection but does not provide `verify-full` hostname verification.

### Connection test

The test checks:

- SQL login and RisingWave version;
- target schema;
- table create/drop and write privileges;
- for WebSocket modes, endpoint connectivity, signed initialization when configured, a DML write,
  and a RisingWave ACK.

WebSocket modes require RisingWave 3.0 or later. JDBC remains available for older compatible
versions.

### Optional webhook secret

**Webhook Secret** signs the WebSocket initialization frame. When it is set, the connector creates
or reuses **RisingWave Secret Name** and references that Secret from table DDL. The secret value is
not embedded in `SHOW CREATE TABLE`.

The database user needs `CREATE SECRET` permission. If the secret name is blank, the connector
generates a per-table name.

### Task requirements

- Use **Data Replication** for production snapshot and CDC tasks.
- WebSocket streaming requires the real source primary key as the update condition.
- MySQL requires `binlog_row_image=FULL`.
- MongoDB requires TapData Update Field Completion:
  `enableFillingModifiedData=true`.
- SQL Server requires Change Tracking.
- Oracle requires LogMiner with `autoLog=false` and primary-key supplemental logging.

### Important limits

- WebSocket streaming writes complete-row upserts. Updates or deletes without the old primary-key
  identity fail.
- Automatic relational schema evolution is not supported. Unknown fields fail instead of being
  silently dropped.
- JSONB append-only creates one `data JSONB` column and accepts inserts only.
- JSONB delivery is at-least-once. A retry after an ambiguous ACK loss may append a duplicate.
- A single serialized WebSocket record larger than 8 MiB fails explicitly.
- JDBC is a compatibility mode and normally has lower throughput than WebSocket streaming.

The `1.0.0` qualification matrix covers PostgreSQL, MySQL 8.4, MongoDB 7, Kafka 3.9.1 through JSONB
append-only, SQL Server 2022, and Oracle 26ai. SQL Server and Oracle passed both WebSocket streaming
and JDBC tests.
