CREATE MATERIALIZED VIEW bhv_mv AS
SELECT
    user_id,
    target_id
FROM
    user_behaviors;
