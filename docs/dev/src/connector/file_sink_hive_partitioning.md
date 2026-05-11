# File sink Hive-style partitioning

RisingWave's file sink (S3, GCS, Azure Blob, WebHDFS, local FS) can write
Parquet/JSON output into Hive-compatible partition directories so downstream
query engines (Trino, Athena, DuckDB, Spark, Iceberg readers, etc.) can prune
files at query time without scanning them.

Two related sink options control the layout. The newer
`path_partition_format` supersedes the older `path_partition_prefix` enum and
should be preferred for any new pipeline.

## Options

| Option                 | Type   | Behaviour                                                                                                                                                                                                                                                                                                                                  |
| ---------------------- | ------ | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `path_partition_prefix`| enum   | One of `none` (default), `day`, `month`, `hour`. Files are bucketed by the writer's creation time into a single, non-Hive directory like `2025-06-25 13:00/`. Kept for backwards compatibility.                                                                                                                                            |
| `path_partition_format`| string | Free-form template applied per row to compute the partition directory. Supports chrono `strftime` tokens (e.g. `%Y`, `%m`, `%d`, `%H`) and `{column_name}` placeholders that reference columns of the sink's input schema. When set, this option takes precedence over `path_partition_prefix`. Use `{{` / `}}` to emit literal `{` / `}`. |

The user is responsible for including a trailing `/` if a directory is desired.

## Time-only partitioning

If the template contains only chrono tokens, every row inside the same flush
goes to a single file in one partition (same behaviour as
`path_partition_prefix`, just with arbitrary naming).

```sql
CREATE SINK ohlcv_1d AS SELECT * FROM ohlcv_mv WITH (
  connector            = 's3',
  s3.region_name       = 'us-east-1',
  s3.bucket_name       = 'metrics-live',
  s3.path              = 'ohlcv/exchange=binance-futures/',
  path_partition_format = 'year=%Y/month=%m/day=%d/hour=%H/',
  type                 = 'append-only',
  force_append_only    = 'true'
) FORMAT PLAIN ENCODE PARQUET(force_append_only = 'true');
```

Produces:

```
s3://metrics-live/ohlcv/exchange=binance-futures/year=2025/month=06/day=25/hour=13/<uuid>_<unix_ts>_<seq>.parquet
```

## Column-based partitioning

If the template includes `{column_name}` placeholders, the sink groups the rows
of every incoming chunk by the rendered partition path and opens one file per
distinct key. This lets a single sink fan out to many directories — useful
when `symbol`, `account_id`, or another high-cardinality dimension determines
where the data should live.

```sql
CREATE SINK ohlcv_1d AS SELECT exchange, symbol, time, open, high, low, close, volume FROM ohlcv_mv WITH (
  connector            = 's3',
  s3.region_name       = 'us-east-1',
  s3.bucket_name       = 'metrics-live',
  s3.path              = 'ohlcv/period=1d/',
  path_partition_format = 'exchange={exchange}/symbol={symbol}/year=%Y/month=%m/day=%d/hour=%H/',
  type                 = 'append-only',
  force_append_only    = 'true'
) FORMAT PLAIN ENCODE PARQUET(force_append_only = 'true');
```

Produces files like:

```
s3://metrics-live/ohlcv/period=1d/exchange=binance-futures/symbol=BTC-USDT/year=2025/month=06/day=25/hour=13/<uuid>_<unix_ts>_<seq>.parquet
s3://metrics-live/ohlcv/period=1d/exchange=binance-futures/symbol=ETH-USDT/year=2025/month=06/day=25/hour=13/<uuid>_<unix_ts>_<seq>.parquet
…
```

### Behaviour and caveats

- Partition columns are **kept in the parquet/JSON output** alongside the
  partition directory. This matches the data layout most Hive-compatible
  readers expect when partition columns are also present in the data file.
- `NULL` column values render as `__HIVE_DEFAULT_PARTITION__/`, matching
  Hive's convention.
- Values containing `/`, `\`, or control characters are percent-encoded
  (`/` → `%2F`) so a malformed column value cannot escape its partition
  directory.
- Rendering is done in UTC. The writer's creation time (not the row's event
  time) supplies the chrono tokens — so the `year=/month=/day=/hour=/`
  partition reflects when the file was opened, not when each individual row
  was produced. Use `rollover_seconds` and `max_row_count` to control the
  cadence.
- With many distinct partition keys, expect many concurrently open object
  writers per flush. Tune `rollover_seconds` and `max_row_count` accordingly
  to keep per-file size and memory bounded.
- A `path_partition_format` that references a column the sink doesn't expose
  is rejected at sink creation time.

## Validation

The template is parsed and column references are resolved at
`CREATE SINK` time. Unknown columns or unterminated `{` placeholders produce
configuration errors:

```text
ERROR: path_partition_format references unknown column `symbol`; available columns: [...]
```
