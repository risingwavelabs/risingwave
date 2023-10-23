CREATE SINK bhv_redis_sink_1
FROM
    bhv_mv WITH (
    primary_key = 'user_id',
    connector = 'redis',
    redis.url= 'redis://127.0.0.1:6379/',
)FORMAT PLAIN ENCODE JSON(force_append_only='true');

CREATE SINK bhv_redis_sink_2
FROM
    bhv_mv WITH (
    primary_key = 'user_id',
    connector = 'redis',
    redis.url= 'redis://127.0.0.1:6379/',
)FORMAT PLAIN ENCODE TEMPLATE(force_append_only='true', key_format = 'UserID:{user_id}', value_format = 'TargetID:{target_id},EventTimestamp{event_timestamp}');