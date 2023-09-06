CREATE SINK bhv_cassandra_sink
FROM
    bhv_mv WITH (
    connector = 'cassandra',
    type = 'append-only',
    force_append_only='true',
    cassandra.url = 'cassandra:9042',
    cassandra.keyspace  = 'my_keyspace',
    cassandra.table = 'demo_test',
    cassandra.datacenter = 'datacenter1',
);