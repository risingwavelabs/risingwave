CREATE MATERIALIZED VIEW bhv_mv AS
SELECT
    user_id,
    target_id,
    target_type
FROM
    user_behaviors;