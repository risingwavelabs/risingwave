CREATE MATERIALIZED VIEW orders_rw_count AS
SELECT
    COUNT(*) as cnt
FROM
    orders_rw;
