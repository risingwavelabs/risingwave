CREATE SINK bhv_cassandra_sink
FROM
    bhv_mv WITH (
    connector = 'cassandra',
    type = 'append-only',
    force_append_only='true',
    cassandra.url = 'cassandra:9042',
    cassandra.keyspace  = 'demo',
    cassandra.table = 'demo_bhv_table',
    cassandra.datacenter = 'datacenter1',
);

CREATE SINK bhv_scylla_sink
FROM
    bhv_mv WITH (
    connector = 'cassandra',
    type = 'append-only',
    force_append_only='true',
    cassandra.url = 'scylladb:9042',
    cassandra.keyspace  = 'demo',
    cassandra.table = 'demo_bhv_table',
    cassandra.datacenter = 'datacenter1',
);
