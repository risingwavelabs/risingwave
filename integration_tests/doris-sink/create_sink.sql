set sink_decouple = false;

create secret doris_secret with (backend = 'meta') as '123456';

CREATE SINK bhv_doris_sink
FROM
    bhv_mv WITH (
    connector = 'doris',
    type = 'append-only',
    doris.url = 'http://fe:8030',
    doris.user = 'users',
    doris.password = secret doris_secret,
    doris.database = 'demo',
    doris.table='demo_bhv_table',
    force_append_only='true'
);

CREATE SINK upsert_doris_sink
FROM
    upsert_bhv_mv WITH (
    connector = 'doris',
    type = 'upsert',
    doris.url = 'http://fe:8030',
    doris.user = 'users',
    doris.password = secret doris_secret,
    doris.database = 'demo',
    doris.table='upsert_table',
    primary_key = 'user_id'
);
