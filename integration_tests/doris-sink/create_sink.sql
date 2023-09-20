CREATE SINK bhv_doris_sink
FROM
    bhv_mv WITH (
    connector = 'doris',
    type = 'append-only',
    doris.url = 'http://fe:8030',
    doris.user = 'users',
    doris.password = '123456',
    doris.database = 'demo',
    doris.table='demo_bhv_table',
    force_append_only='true'
);