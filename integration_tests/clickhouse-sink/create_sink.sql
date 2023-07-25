CREATE SINK bhv_iceberg_sink
FROM
    bhv_mv WITH (
    connector = 'clickhouse',
    type = 'append-only',
    force_append_only='true',
    clickhouse.url = 'http://127.0.0.1:8123',
    clickhouse.user = 'default',
    clickhouse.password = '',
    clickhouse.database = 'default',
    clickhouse.table='demo_test',
);