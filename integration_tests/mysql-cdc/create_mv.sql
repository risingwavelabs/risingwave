CREATE MATERIALIZED VIEW lineitem_rw_count AS
SELECT
    COUNT(*) as cnt
FROM
    lineitem_rw;
