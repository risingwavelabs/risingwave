CREATE MATERIALIZED VIEW bhv_mv AS
SELECT
    user_id,
    target_id,
    event_timestamp
FROM
    user_behaviors;