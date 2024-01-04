CREATE SINK bhv_starrocks_sink
FROM
    bhv_mv WITH (
    connector = 'starrocks',
    type = 'append-only',
    starrocks.host = 'starrocks-fe',
    starrocks.mysqlport = '9030',
    starrocks.httpport = '8030',
    starrocks.user = 'users',
    starrocks.password = '123456',
    starrocks.database = 'demo',
    starrocks.table = 'demo_bhv_table',
    force_append_only='true'
);