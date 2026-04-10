# Records HTTP Demo

This demo wires a tiny HTTP records API into frontend to validate the end-to-end path:

- HTTP handler calls internal SQL instead of touching storage directly.
- The demo uses a fixed table, `rw_records_demo.records`.
- Hidden `_row_id` is reused as the returned `seq_num` token.
- `POST /demo/records` serializes appends inside one frontend process, inserts one row, runs `FLUSH`, and then returns the visible tail row.

It is intentionally narrower than the S2 records API:

- single stream only
- single frontend process only
- no batch append
- no `match_seq_num`
- no fencing, trim, or long-poll
- `body` is stored as `varchar`

`seq_num` in this demo is only a demo token. It is not presented as the final records sequencing design.

## Startup

Run everything from the demo worktree:

```sh
cd /Users/li0k/Documents/github/risingwave-records-demo
./risedev clean-data
./risedev pre-start-dev
./risedev dev meta-1cn-1fe-sqlite
```

Default ports:

- SQL: `localhost:4566`
- frontend HTTP: `localhost:4560`

## CLI

The branch also includes a small standalone CLI binary that talks to the demo HTTP endpoints directly:

```sh
cargo run -p risingwave_records_demo_cli -- append 'hello'
cargo run -p risingwave_records_demo_cli -- append-repl
cargo run -p risingwave_records_demo_cli -- tail
cargo run -p risingwave_records_demo_cli -- latest
cargo run -p risingwave_records_demo_cli -- watch
cargo run -p risingwave_records_demo_cli -- read --seq-num 0 --limit 10
```

By default it connects to `http://127.0.0.1:4560/demo`.

To point it at a different frontend address:

```sh
cargo run -p risingwave_records_demo_cli -- --url http://127.0.0.1:4561/demo tail
```

You can also use the `RW_RECORDS_DEMO_URL` environment variable.

`latest` is a one-shot command. `watch` is the terminal-friendly version of a playground live view:

- it prints a small banner
- it prints the current latest visible record once
- then it polls for new records and prints each one in a human-readable single-line format
- stop it with `Ctrl-C`

`append-repl` is the matching write-side terminal:

- it prints a prompt
- each non-empty line is appended as one record
- it prints `OK: seq=..., timestamp=...` after each append
- stop it with `Ctrl-D` or `Ctrl-C`

Examples:

```sh
cargo run -p risingwave_records_demo_cli -- append-repl
cargo run -p risingwave_records_demo_cli -- watch
cargo run -p risingwave_records_demo_cli -- watch --interval-ms 200
cargo run -p risingwave_records_demo_cli -- watch --seq-num 0 --limit 50
```

Stop the cluster with:

```sh
cd /Users/li0k/Documents/github/risingwave-records-demo
./risedev k
```

## Demo Table

The HTTP handler lazily bootstraps the demo catalog on the first request:

```sql
CREATE SCHEMA IF NOT EXISTS rw_records_demo;

CREATE TABLE IF NOT EXISTS rw_records_demo.records (
    body varchar,
    ts_ms bigint
) APPEND ONLY;
```

The planner contains a demo-only special case for this exact table name so the row-id generator runs as a singleton. That is what allows `_row_id` to act as a monotonic token in this demo.

## HTTP API

### `POST /demo/records`

Append one record.

Request:

```json
{"body":"hello"}
```

Behavior:

1. Bootstrap the demo schema and table if they do not exist yet.
2. Acquire a frontend-local append mutex.
3. Read the current tail `_row_id`.
4. Insert one row with the provided `body` and current `ts_ms`.
5. Run `FLUSH`.
6. Poll until a row with `_row_id > old_tail` is visible, then return it.

Response:

```json
{"seq_num":"665241081658474496","ts_ms":1775841031873,"body":"hello"}
```

### `GET /demo/records`

Read records from an inclusive sequence position.

Query parameters:

- `seq_num`: inclusive lower bound, default `0`
- `limit`: max rows to return, default `100`, clamped to `1..1000`

Behavior:

- Executes a SQL query shaped like:

```sql
SELECT CAST(_row_id AS bigint) AS seq_num, ts_ms, body
FROM rw_records_demo.records
WHERE CAST(_row_id AS bigint) >= $seq_num
ORDER BY _row_id
LIMIT $limit;
```

Response:

```json
{
  "records": [
    {"seq_num":"665241062469533696","ts_ms":1775841026464,"body":"a"},
    {"seq_num":"665241081658474496","ts_ms":1775841031873,"body":"b"}
  ]
}
```

### `GET /demo/records/tail`

Read the current tail.

Behavior:

- Returns the largest visible `_row_id` and its `ts_ms`.
- Returns `{"seq_num":"0","ts_ms":0}` when the table is empty.

Response:

```json
{"seq_num":"665241081658474496","ts_ms":1775841031873}
```

## Example Commands

Append two rows:

```sh
curl -sS -X POST localhost:4560/demo/records \
  -H 'content-type: application/json' \
  -d '{"body":"a"}'

curl -sS -X POST localhost:4560/demo/records \
  -H 'content-type: application/json' \
  -d '{"body":"b"}'
```

Read the tail:

```sh
curl -sS localhost:4560/demo/records/tail
```

Read from the beginning:

```sh
curl -sS 'localhost:4560/demo/records?seq_num=0&limit=10'
```

## What To Validate

These are the useful checks for the demo:

1. Empty state:
   - `GET /demo/records/tail` returns `{"seq_num":"0","ts_ms":0}`
   - `GET /demo/records?seq_num=0&limit=10` returns `{"records":[]}`
2. Sequential append:
   - two `POST /demo/records` calls return strictly increasing `seq_num`
   - `tail.seq_num` equals the second append's `seq_num`
   - reading from `seq_num=0` returns records in append order
3. Resume from a sequence position:
   - reading from the first append's `seq_num` returns both rows
   - reading from the second append's `seq_num` returns only the second row
4. Restart without `clean-data`:
   - stop with `./risedev k`
   - start again with `./risedev dev meta-1cn-1fe-sqlite`
   - `tail` and `GET /demo/records` still see the previous rows

Concurrent append requests are also worth trying, but the expected order is request serialization order inside the frontend process, not caller submit order.

## Playground-Like Flow

One terminal watches, another appends:

Terminal A:

```sh
cd /Users/li0k/Documents/github/risingwave-records-demo
cargo run -p risingwave_records_demo_cli -- watch
```

Terminal B:

```sh
cd /Users/li0k/Documents/github/risingwave-records-demo
cargo run -p risingwave_records_demo_cli -- append-repl
```

If you want to replay from the beginning and then keep following:

```sh
cargo run -p risingwave_records_demo_cli -- watch --seq-num 0
```
