# RStream: HTTP Stream API for RisingWave

RStream provides S2-like HTTP stream APIs on top of RisingWave. Each stream is
backed by a single-vnode append-only table, giving you a totally-ordered,
durable log accessible over plain HTTP.

## How it works

| Concept | RisingWave mapping |
|---------|--------------------|
| Stream `foo` | Database `rstream_foo` with table `public._records` |
| Record body | `body JSONB` column |
| Ordering | Single vnode (`streaming_max_parallelism=1`) guarantees total order |
| Write batching | Per-database `barrier_interval_ms` (default 100 ms) |
| Write path | Reuses the webhook/fast_insert path (bypasses SQL optimizer) |

Because each stream lives in its own database, checkpoints are isolated per
stream.

## Quick start

Start RisingWave (the HTTP API shares port 4560 with the webhook service):

```bash
./risedev d          # start the cluster
```

### Create a stream

```bash
curl -X POST http://localhost:4560/v1/streams \
  -H 'Content-Type: application/json' \
  -d '{"name": "events"}'
# {"stream":"events"}  (201 Created)
```

### Append records

Each element of the `records` array is stored as-is into the `body` JSONB
column.

```bash
curl -X POST http://localhost:4560/v1/streams/events/records \
  -H 'Content-Type: application/json' \
  -d '{"records": [{"action": "click", "ts": 1000}, {"action": "view", "ts": 1001}]}'
# {"count":2}  (200 OK)
```

### Read records (unary fetch)

```bash
# read all records
curl http://localhost:4560/v1/streams/events/records
# {"records":[{"seq_no":"665562464464666624","body":{"action":"click","ts":1000}}, ...],"next_cursor":"665562464464666625"}

# paginate: read 1 record after cursor
curl "http://localhost:4560/v1/streams/events/records?after=665562464464666624&limit=1"
# {"records":[{"seq_no":"665562464464666625","body":{"action":"view","ts":1001}}],"next_cursor":"665562464464666625"}
```

`seq_no` is a monotonically increasing i64 (as string) that serves as an opaque
cursor. Within the same millisecond values are contiguous; across milliseconds
there may be gaps, but ordering is always preserved.

### Tail records (SSE)

Add `Accept: text/event-stream` to receive a continuous stream of records:

```bash
curl -N -H 'Accept: text/event-stream' http://localhost:4560/v1/streams/events/records

# data: {"seq_no":"665562464464666624","body":{"action":"click","ts":1000}}
#
# data: {"seq_no":"665562464464666625","body":{"action":"view","ts":1001}}
# (connection stays open, new records appear as they are appended)
```

Use `after` to start tailing from a position:

```bash
curl -N -H 'Accept: text/event-stream' \
  "http://localhost:4560/v1/streams/events/records?after=665562464464666624"
```

### Read data via SQL

Since every stream is just a regular RisingWave table, you can also query it
with SQL, create materialized views over it, or join it with other streams:

```bash
psql -h localhost -p 4566 -d rstream_events -U root \
  -c "SELECT _row_id, body FROM _records ORDER BY _row_id;"

# create a materialized view to count actions
psql -h localhost -p 4566 -d rstream_events -U root \
  -c "CREATE MATERIALIZED VIEW action_counts AS
      SELECT body->>'action' AS action, COUNT(*) AS cnt
      FROM _records GROUP BY body->>'action';"
```

### List streams

```bash
curl http://localhost:4560/v1/streams
# {"streams":["events"]}
```

### Get stream info

```bash
curl http://localhost:4560/v1/streams/events
# {"name":"events"}  (200 OK)

curl http://localhost:4560/v1/streams/nonexistent
# {"error":"stream 'nonexistent' not found"}  (404)
```

### Delete a stream

This drops the underlying table and database.

```bash
curl -X DELETE http://localhost:4560/v1/streams/events
# (200 OK)
```

## API reference

All endpoints are served on the webhook HTTP port (default `4560`).

| Method | Path | Description | Success | Error codes |
|--------|------|-------------|---------|-------------|
| `POST` | `/v1/streams` | Create a stream | 201 | 400, 409 |
| `GET` | `/v1/streams` | List all streams | 200 | |
| `GET` | `/v1/streams/{name}` | Get stream info | 200 | 404 |
| `DELETE` | `/v1/streams/{name}` | Delete a stream | 200 | 404 |
| `POST` | `/v1/streams/{name}/records` | Append records | 200 | 400, 404 |
| `GET` | `/v1/streams/{name}/records` | Read records (unary or SSE) | 200 | 400, 404 |

### Create stream request

```json
{"name": "my_stream"}
```

Stream names: alphanumeric + underscore, max 63 chars, cannot start with a
digit.

### Append records request

```json
{"records": [<json_value>, <json_value>, ...]}
```

Each element can be any valid JSON value (object, array, string, number, etc.).

### Read records query parameters

| Param | Type | Default | Description |
|-------|------|---------|-------------|
| `after` | string | (none) | `seq_no` cursor — return records after this position |
| `limit` | integer | 100 | Max records to return (1–1000) |

**Unary response** (default or `Accept: application/json`):

```json
{
  "records": [{"seq_no": "665562464464666624", "body": {...}}, ...],
  "next_cursor": "665562464464666625"
}
```

`next_cursor` is absent when `records` is empty.

**SSE response** (`Accept: text/event-stream`):

```
data: {"seq_no":"665562464464666624","body":{...}}

data: {"seq_no":"665562464464666625","body":{...}}
```

Connection stays open. A `: keepalive` comment is sent every 15 seconds.

### Error response format

```json
{"error": "description of what went wrong"}
```

## Architecture

```
HTTP Client
  |  POST /v1/streams/{name}/records
  v
Frontend HTTP Server (Axum, port 4560)
  |  JSON array -> DataChunk (JsonbArrayBuilder)
  |  Build FastInsertRequest
  v
choose_fast_insert_client() -> route to CN via streaming vnode mapping
  v
CN FastInsertExecutor
  |  DataChunk -> StreamChunk
  |  DmlManager -> WriteHandle -> txn channel
  v
SourceExecutor -> RowIdGenExecutor -> MaterializeExecutor -> Storage
```

## Limitations

- **SSE polling.** The SSE tailing mode polls with `SELECT` queries rather than
  using RisingWave's subscription cursor system. Latency floor is ~100 ms
  (matching `barrier_interval_ms`).
- **Single frontend.** The request counter used for CN routing is per-frontend
  process. Multi-frontend HA deployments work but don't coordinate counters.
- **No authentication.** All requests run as the `root` super user.
- **Fixed barrier interval.** Each stream database is created with
  `barrier_interval_ms = 100`. This is not yet user-configurable via the
  HTTP API.
