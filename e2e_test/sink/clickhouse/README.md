# ClickHouse sink tests

- `basic.slt` covers basic sink behavior and data types.
- `distributed_table.slt` covers Distributed tables and requires the cluster
  configuration described below.
- `distributed_remote.slt` covers append-only and upsert sinks with the
  Distributed table and its storage tables on separate ClickHouse instances.

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

## Remote storage table setup

CI starts a second instance, `clickhouse-shard`, and mounts
`ci/clickhouse-conf/remote-distributed.xml` on the router (`clickhouse-server`).
The `rw_sink_remote_test` cluster contains only that shard. 

For local testing, start the RiseDev profile above, then connect its ClickHouse
container and a separate shard to a dedicated Docker network:

```sh
docker network create rw-clickhouse-sink-test
docker network connect rw-clickhouse-sink-test risedev-clickhouse
docker run -d --name rw-clickhouse-shard --network rw-clickhouse-sink-test \
  --network-alias clickhouse-shard -p 127.0.0.1:19000:9000 \
  -e CLICKHOUSE_USER=default -e CLICKHOUSE_PASSWORD=default \
  clickhouse/clickhouse-server:26.9
docker cp ci/clickhouse-conf/remote-distributed.xml risedev-clickhouse:/etc/clickhouse-server/config.d/rw-remote-distributed.xml
docker exec risedev-clickhouse clickhouse-client --user default --password default --query 'SYSTEM RELOAD CONFIG'
export CLICKHOUSE_SHARD_HOST=127.0.0.1
export CLICKHOUSE_SHARD_NATIVE_PORT=19000
# Used by the clickhouse helper when no native client is installed on the host.
export CLICKHOUSE_SHARD_CONTAINER=rw-clickhouse-shard
./risedev slt './e2e_test/sink/clickhouse/distributed_remote.slt'
```

Both instances use `CLICKHOUSE_USER`, `CLICKHOUSE_PASSWORD`, and
`CLICKHOUSE_DATABASE` from the RiseDev environment. The test creates and removes
its own tables and remote database; it can be rerun after a failure. After testing:

```sh
docker rm -f rw-clickhouse-shard
docker network disconnect rw-clickhouse-sink-test risedev-clickhouse
docker network rm rw-clickhouse-sink-test
```
