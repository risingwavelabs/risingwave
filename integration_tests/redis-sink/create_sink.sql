CREATE SINK bhv_redis_sink_1
FROM
    bhv_mv WITH (
    primary_key = 'user_id',
    connector = 'redis',
    type = 'append-only',
    force_append_only='true',
    redis.url= 'redis://127.0.0.1:6379/',
);

CREATE SINK bhv_redis_sink_2
FROM
    bhv_mv WITH (
    primary_key = 'user_id',
    connector = 'redis',
    type = 'append-only',
    force_append_only='true',
    redis.url= 'redis://127.0.0.1:6379/',
    redis.keyformat='user_id:{user_id}',
    redis.valueformat='username:{username},event_timestamp{event_timestamp}'
);