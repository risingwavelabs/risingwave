# StarRocks Sink Stream Load Options

The StarRocks sink writes JSON through the
[Stream Load transaction interface](https://docs.starrocks.io/docs/loading/Stream_Load_transaction_interface/).
One transaction can contain multiple HTTP load requests. `starrocks.max_batch_size_bytes`
limits each request's encoded JSON payload, while `commit_checkpoint_interval` controls
how many RisingWave checkpoints are included before preparing and committing a transaction.
Splitting a request does not commit the transaction.

## Request and transaction options

| RisingWave sink option | Default | Meaning |
| --- | --- | --- |
| `starrocks.ignore_json_size` | `false` | Send `ignore_json_size: true` on load requests to skip the StarRocks JSON size check. |
| `starrocks.load_mem_limit` | Not sent | Send `load_mem_limit` on load requests, in bytes. Accepts an integer from `0` through `9223372036854775807`. An explicit `0` is forwarded unchanged; its meaning follows the StarRocks server version. |
| `starrocks.transaction_timeout_seconds` | Not sent | Send `timeout` when beginning a transaction, in seconds. Accepts a positive 32-bit signed integer (`1` through `2147483647`), subject to the server's configured timeout range. Controls the server-side lifetime from begin to prepared. |

When an optional limit or timeout is omitted, StarRocks uses its server default.
RisingWave does not replace those defaults with fixed values. These options can be
changed with `ALTER SINK ... CONNECTOR WITH`; the connector update rebuilds the writer
and the new settings apply to its subsequent requests and transactions.
Use `SET sink_decouple = true` before creating a sink that needs these runtime updates,
so its log reader can rewind while rebuilding the writer.

`starrocks.transaction_timeout_seconds` is independent of
`starrocks.stream_load.http.timeout.ms`, which controls client-side HTTP waiting.
Changing the client HTTP timeout does not change the server-side transaction lifetime.
StarRocks also validates the transaction timeout against its FE settings
`min_load_timeout_second` and `max_stream_load_timeout_second` (defaults: 1 and
259200 seconds). RisingWave leaves that configurable range to the server.

See the StarRocks documentation for
[JSON size checks and load memory limits](https://docs.starrocks.io/docs/sql-reference/sql-statements/loading_unloading/STREAM_LOAD/)
and [transaction timeouts](https://docs.starrocks.io/docs/loading/Stream_Load_transaction_interface/#transaction-timeout-management).
Check these settings against the StarRocks version used by your deployment.

## Example

The following example limits requests to 5 MiB, sets a load memory limit of 256 MiB,
and allows 900 seconds for a transaction to reach the prepared state:

```sql
SET sink_decouple = true;

CREATE SINK shipment_sink FROM shipment_mv WITH (
    connector = 'starrocks',
    type = 'upsert',
    primary_key = 'shipment_id',
    starrocks.host = 'starrocks-fe',
    starrocks.mysqlport = '9030',
    starrocks.httpport = '8030',
    starrocks.user = 'sink_user',
    starrocks.password = '<password>',
    starrocks.database = 'shipments',
    starrocks.table = 'shipment',
    starrocks.max_batch_size_bytes = '5242880',
    starrocks.load_mem_limit = '268435456',
    starrocks.transaction_timeout_seconds = '900',
    commit_checkpoint_interval = '1'
);
```

If the JSON size check must be skipped after reviewing backend capacity:

```sql
ALTER SINK shipment_sink CONNECTOR WITH (
    starrocks.ignore_json_size = 'true'
);
```

To restore the check:

```sql
ALTER SINK shipment_sink CONNECTOR WITH (
    starrocks.ignore_json_size = 'false'
);
```

## Size checks and memory

StarRocks defaults to a 100 MiB JSON size check. In its transaction loading path,
the check can apply to bytes accumulated across requests using the same transaction
label, depending on the server version and request transfer mode. A small
`starrocks.max_batch_size_bytes` therefore does not guarantee that an entire
transaction stays below the StarRocks limit. Reducing `commit_checkpoint_interval`
can reduce accumulation, but no checkpoint count guarantees a particular byte size.

Enabling `starrocks.ignore_json_size` does not disable RisingWave's request splitting
or its rejection of a single encoded row larger than `starrocks.max_batch_size_bytes`.
It also does not bypass other StarRocks limits. The option can increase backend
memory use; keep requests bounded and monitor backend memory and sink progress.

`starrocks.load_mem_limit` configures the StarRocks load job. It is not a guarantee
that all HTTP receive buffers, JSON processing, or total BE/CN memory are bounded
by that value. Test the selected limits and transaction duration against the
target StarRocks version before applying them to a production workload.
