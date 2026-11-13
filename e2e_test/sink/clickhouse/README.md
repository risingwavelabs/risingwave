# ClickHouse sink tests

- `basic.slt` covers basic sink behavior and data types.
- `distributed_table.slt` covers Distributed tables and requires the cluster
  configuration described below.

## Distributed table setup

`distributed_table.slt` uses the one-shard cluster `rw_sink_test` defined in
`ci/clickhouse-conf/distributed.xml`. CI mounts this configuration into its existing
ClickHouse server and runs this test through `e2e-clickhouse-sink-test.sh`.

For a local RiseDev environment:

```sh
./risedev d local-clickhouse-sink-test
docker cp ci/clickhouse-conf/distributed.xml risedev-clickhouse:/etc/clickhouse-server/config.d/rw-distributed.xml
# The config watcher loads the new cluster; reload explicitly before running the test.
docker exec risedev-clickhouse clickhouse-client --user default --password default --query 'SYSTEM RELOAD CONFIG'
./risedev slt './e2e_test/sink/clickhouse/distributed_table.slt'
```

If using different ClickHouse credentials, adjust the reload command. The cluster
configuration reads `CLICKHOUSE_USER` and `CLICKHOUSE_PASSWORD` from the server's
environment, as set by RiseDev and CI.
