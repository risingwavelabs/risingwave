CREATE MATERIALIZED VIEW orders_rw_count AS
SELECT
    COUNT(*) as cnt
FROM
    orders_rw;

CREATE MATERIALIZED VIEW citus_all_types_count AS
SELECT
    COUNT(*) as cnt
FROM
    citus_all_types;
