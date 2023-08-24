CREATE SINK bhv_iceberg_sink
FROM
    bhv_mv WITH (
    connector = 'cassandra',
    type = 'append-only',
    force_append_only='true',
    cassandra.url = 'http://127.0.0.1:9042',
    cassandra.keyspace  = 'mykeyspace',
    cassandra.table = 'demo_test',
    cassandra.datacenter = 'datacenter1',
);