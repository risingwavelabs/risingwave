CREATE SINK bhv_starrocks_sink
FROM
    bhv_mv WITH (
    connector = 'starrocks',
    type = 'upsert',
    starrocks.host = 'starrocks-fe',
    starrocks.mysqlport = '9030',
    starrocks.httpport = '8030',
    starrocks.user = 'users',
    starrocks.password = '123456',
    starrocks.database = 'demo',
    starrocks.table = 'demo_bhv_table',
    primary_key = 'user_id'
);