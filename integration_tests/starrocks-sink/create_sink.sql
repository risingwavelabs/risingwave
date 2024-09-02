set sink_decouple = false;

create secret starrocks_secret with (backend = 'meta') as '123456';

CREATE SINK bhv_starrocks_sink_primary
FROM
    bhv_mv WITH (
    connector = 'starrocks',
    type = 'append-only',
    starrocks.host = 'starrocks-fe',
    starrocks.mysqlport = '9030',
    starrocks.httpport = '8030',
    starrocks.user = 'users',
    starrocks.password = secret starrocks_secret,
    starrocks.database = 'demo',
    starrocks.table = 'demo_primary_table',
    force_append_only='true'
);

CREATE SINK bhv_starrocks_sink_duplicate
FROM
    bhv_mv WITH (
    connector = 'starrocks',
    type = 'append-only',
    starrocks.host = 'starrocks-fe',
    starrocks.mysqlport = '9030',
    starrocks.httpport = '8030',
    starrocks.user = 'users',
    starrocks.password = secret starrocks_secret,
    starrocks.database = 'demo',
    starrocks.table = 'demo_duplicate_table',
    force_append_only='true'
);

CREATE SINK bhv_starrocks_sink_aggregate
FROM
    bhv_mv WITH (
    connector = 'starrocks',
    type = 'append-only',
    starrocks.host = 'starrocks-fe',
    starrocks.mysqlport = '9030',
    starrocks.httpport = '8030',
    starrocks.user = 'users',
    starrocks.password = secret starrocks_secret,
    starrocks.database = 'demo',
    starrocks.table = 'demo_aggregate_table',
    force_append_only='true'
);

CREATE SINK bhv_starrocks_sink_unique
FROM
    bhv_mv WITH (
    connector = 'starrocks',
    type = 'append-only',
    starrocks.host = 'starrocks-fe',
    starrocks.mysqlport = '9030',
    starrocks.httpport = '8030',
    starrocks.user = 'users',
    starrocks.password = secret starrocks_secret,
    starrocks.database = 'demo',
    starrocks.table = 'demo_unique_table',
    force_append_only='true'
);

CREATE SINK upsert_starrocks_sink
FROM
    upsert_bhv_mv WITH (
    connector = 'starrocks',
    type = 'upsert',
    starrocks.host = 'starrocks-fe',
    starrocks.mysqlport = '9030',
    starrocks.httpport = '8030',
    starrocks.user = 'users',
    starrocks.password = secret starrocks_secret,
    starrocks.database = 'demo',
    starrocks.table = 'upsert_table',
    primary_key = 'user_id'
);

CREATE SINK starrocks_types_sink
FROM
    starrocks_types WITH (
    connector = 'starrocks',
    type = 'append-only',
    starrocks.host = 'starrocks-fe',
    starrocks.mysqlport = '9030',
    starrocks.httpport = '8030',
    starrocks.user = 'users',
    starrocks.password = secret starrocks_secret,
    starrocks.database = 'demo',
    starrocks.table = 'starrocks_types',
    force_append_only='true'
);
