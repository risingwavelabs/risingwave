set sink_decouple = false;

CREATE SINK bhv_clickhouse_sink
FROM
    bhv_mv WITH (
    connector = 'clickhouse',
    type = 'append-only',
    force_append_only='true',
    clickhouse.url = 'http://clickhouse-server:8123',
    clickhouse.user = 'default',
    clickhouse.password = 'default',
    clickhouse.database = 'default',
    clickhouse.table='demo_test',
);

CREATE SINK null_clickhouse_sink
FROM
    bhv_mv WITH (
    connector = 'clickhouse',
    type = 'append-only',
    force_append_only='true',
    clickhouse.url = 'http://clickhouse-server:8123',
    clickhouse.user = 'default',
    clickhouse.password = 'default',
    clickhouse.database = 'default',
    clickhouse.table='demo_test_null',
);

CREATE SINK ck_types_sink
FROM
    ck_types WITH (
    connector = 'clickhouse',
    type = 'append-only',
    force_append_only='true',
    clickhouse.url = 'http://clickhouse-server:8123',
    clickhouse.user = 'default',
    clickhouse.password = 'default',
    clickhouse.database = 'default',
    clickhouse.table='ck_types',
);
