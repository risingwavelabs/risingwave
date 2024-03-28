CREATE SINK bhv_doris_sink
FROM
    bhv_mv WITH (
    connector = 'doris',
    type = 'append-only',
    doris.url = 'http://doris:8030',
    doris.user = 'users',
    doris.password = '123456',
    doris.database = 'demo',
    doris.table='demo_bhv_table',
    force_append_only='true'
);

CREATE SINK upsert_doris_sink
FROM
    upsert_bhv_mv WITH (
    connector = 'doris',
    type = 'upsert',
    doris.url = 'http://doris:8030',
    doris.user = 'users',
    doris.password = '123456',
    doris.database = 'demo',
    doris.table='upsert_table',
    primary_key = 'user_id'
);
