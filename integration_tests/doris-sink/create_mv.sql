CREATE MATERIALIZED VIEW bhv_mv AS
SELECT
    user_id,
    target_id,
    event_timestamp AT TIME ZONE 'Asia/Shanghai' as event_timestamp_local
FROM
    user_behaviors;

CREATE MATERIALIZED VIEW upsert_bhv_mv AS
SELECT
    user_id,
    target_id,
    event_timestamp AT TIME ZONE 'Asia/Shanghai' as event_timestamp_local
FROM
    upsert_user_behaviors;
