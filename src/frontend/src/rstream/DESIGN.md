# RStream: S2-like Stream HTTP API for RisingWave

## Context

Implement HTTP stream APIs similar to [S2](https://s2.dev/) in RisingWave. Each "stream" is backed by a single-vnode append-only table with a JSONB body column. Each stream lives in its own database for checkpoint isolation. The write path uses the existing webhook/fast_insert code path (bypass SQL optimizer, send chunks directly to CN's DML executor).

## Architecture

```
HTTP Client
  │  POST /v1/streams/{name}/records  (JSON body)
  ▼
Frontend HTTP Server (Axum, port 4560, shared with webhook)
  │  Parse JSON → DataChunk (JsonbArrayBuilder)
  │  Build FastInsertRequest
  ▼
choose_fast_insert_client() → route to CN via streaming vnode mapping
  ▼
CN TaskService::fast_insert() → FastInsertExecutor
  │  DataChunk → StreamChunk (with _row_id = NULL, filled by RowIdGenExecutor)
  │  DmlManager → TableDmlHandle → WriteHandle
  ▼
SourceExecutor → RowIdGenExecutor → MaterializeExecutor → Storage
```

## Design Decisions

### 1. HTTP Server: extend existing webhook Axum server

Mount rstream routes alongside webhook on the same port 4560. No new port/listener.

```
/webhook/{db}/{schema}/{table}  ← existing
/v1/streams                     ← new (create, list)
/v1/streams/{name}              ← new (get, delete)
/v1/streams/{name}/records      ← new (append)
```

### 2. Stream → DB + Table mapping

- Stream `foo` → database `rstream_foo`, table `public._records`
- Table: `CREATE TABLE _records (body JSONB) APPEND ONLY`
- `_row_id` (Serial) used as opaque monotonic cursor — NOT a clean 0,1,2,3 sequence
- Session configs: `streaming_max_parallelism = 1`, `streaming_parallelism = 1`

### 3. Per-database barrier interval (100 ms default)

Each stream database is created with `barrier_interval_ms = 100` via
`catalog_writer.alter_database_param()` (called directly, bypassing the SQL
`ALTER DATABASE` handler's license check). This gives each stream low-latency
write batching independent of the cluster-wide barrier interval.

The call uses the `dev_session`'s `catalog_writer()` since it only needs the
database ID, not a session on the target database.

### 4. Allow `streaming_max_parallelism = 1`

Modified `check_streaming_max_parallelism()` in `src/common/src/session_config/mod.rs`
to allow 1 (previously rejected with a TODO comment). Gives true single vnode for
total ordering.

### 5. No explicit seq_num column — use `_row_id` as opaque cursor

`_row_id` bit layout: `| 41 bits: timestamp_ms | 10 bits: vnode(=0) | 12 bits: sequence |`

Within a millisecond, sequence is contiguous (0,1,2,...). Across milliseconds,
there are gaps. So `_row_id` is monotonically increasing but not a clean integer
sequence.

Trade-off: `_row_id` cannot be mapped to continuous seq_num without a full table
scan (`ROW_NUMBER()`). Instead, use `_row_id` as an opaque position token:
- Append response returns `count` only (_row_id is assigned later by streaming
  RowIdGenExecutor, not available at HTTP response time)
- Future read path: records returned with their `_row_id`; client uses last seen
  `_row_id` as cursor for next read (`WHERE _row_id > ?`)

### 6. Internal DDL execution

Use `SESSION_MANAGER` + `create_dummy_session()` + `run_statement()`. Reuses all
existing catalog/meta/optimizer/fragmenter machinery.

Create stream flow:
1. Check catalog — if database `rstream_{name}` exists, return 409
2. Create dummy session on `dev` database
3. Execute `CREATE DATABASE rstream_{name}` via `run_statement()`
4. Call `catalog_writer.alter_database_param()` to set `barrier_interval_ms = 100`
5. Create dummy session on `rstream_{name}` database
6. Set `streaming_max_parallelism = 1`, `streaming_parallelism = 1` via session config
7. Execute `CREATE TABLE _records (body JSONB) APPEND ONLY` via `run_statement()`

## API Spec

### POST /v1/streams — Create stream
```json
// Request
{"name": "my_stream"}

// Response 201
{"stream": "my_stream"}

// Error 409
{"error": "stream 'my_stream' already exists"}
```

### DELETE /v1/streams/{name} — Delete stream
```
// Response 200

// Error 404
{"error": "stream 'foo' not found"}
```

### POST /v1/streams/{name}/records — Append records
```json
// Request (array of JSON values, each becomes one row)
{"records": [{"key": "val"}, {"key2": "val2"}]}

// Response 200
{"count": 2}
```

Each element of `records` is stored as-is into the `body JSONB` column.

### GET /v1/streams — List streams
```json
// Response 200
{"streams": ["my_stream", "other_stream"]}
```

### GET /v1/streams/{name} — Get stream info
```json
// Response 200
{"name": "my_stream"}

// Error 404
{"error": "stream 'foo' not found"}
```

### 7. Bearer Token Authentication

Token = RisingWave database user where username = password = token string.

- Token format: `rstream_` + 32 hex chars (e.g., `rstream_a8f3bc91d2e4f567890abcdef1234567`)
- `POST /v1/tokens` creates a DB user: `CREATE USER <token> WITH PASSWORD '<token>' LOGIN`
- Password stored as MD5: `md5(token + token)` (since password=username=token)
- Auth middleware extracts `Authorization: Bearer <token>`, looks up user, verifies hash
- Token management protected by `RSTREAM_ADMIN_SECRET` env var (open in dev mode)

RBAC privilege mapping:

| RStream action | Required privilege |
|---|---|
| Read stream | `CONNECT` on database + `SELECT` on `_records` |
| Write stream | `INSERT` on `_records` |
| Create stream | `CREATEDB` user attribute |
| Delete stream | Database owner or superuser |
| List streams | Only shows streams with `CONNECT` |
| Get stream info | `CONNECT` on database |

When creating a stream, the creator is automatically granted `CONNECT` on the
database and `SELECT, INSERT` on `_records`.

## Implementation Status

### Done (Write + Read + Auth)

| File | What |
|------|------|
| `src/frontend/src/rstream/mod.rs` | Router with auth middleware, data routes + token routes |
| `src/frontend/src/rstream/auth.rs` | Token generation, extraction, password verification |
| `src/frontend/src/rstream/handlers.rs` | 9 handlers: stream CRUD, records read/write, token CRUD |
| `src/frontend/src/rstream/types.rs` | Request/response structs, error type with WWW-Authenticate |
| `src/frontend/src/rstream/README.md` | User-facing docs |
| `src/frontend/src/webhook/mod.rs` | Mount rstream router via `.nest("/v1", ...)` |
| `src/frontend/src/lib.rs` | `pub mod rstream;` |
| `src/common/src/session_config/mod.rs` | Allow `streaming_max_parallelism = 1` |

### Read Path Details

Single endpoint `GET /v1/streams/{name}/records` serves two modes:

- **Unary** (default): `run_statement(SELECT _row_id, body ...)` → JSON response with
  `records` array and `next_cursor`
- **SSE** (`Accept: text/event-stream`): spawned tokio task polls with SELECT in
  a loop, sends records as SSE events via `mpsc::channel` + `ReceiverStream`,
  keepalive every 15s. Backoff: 100ms base × consecutive_empty (cap 5×)

### TODO (Future)

- User-configurable barrier interval via create stream request
- Stream metadata (record count, last _row_id, creation time)
- HTTP API for granting per-stream access (`POST /v1/tokens/{token}/grants`)
- Multi-frontend request counter coordination
- Stream grouping (multiple streams in one database) for high stream counts

## Key Files Reference

| File | Purpose |
|------|---------|
| `src/frontend/src/rstream/auth.rs` | Token generation, bearer extraction, password verification |
| `src/frontend/src/webhook/mod.rs` | Template: HTTP handler + fast_insert flow |
| `src/frontend/src/webhook/utils.rs` | Error type pattern |
| `src/frontend/src/scheduler/fast_insert.rs` | `choose_fast_insert_client()` for CN routing |
| `src/batch/src/executor/fast_insert.rs` | FastInsertExecutor: DataChunk → StreamChunk → DML |
| `src/frontend/src/session.rs` | `run_statement()`, `create_dummy_session()` |
| `src/frontend/src/handler/alter_database_param.rs` | ALTER DATABASE barrier_interval reference |
| `src/common/src/session_config/mod.rs` | `check_streaming_max_parallelism()` |
| `src/common/src/util/row_id.rs` | RowIdGenerator: _row_id bit layout |
