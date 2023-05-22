CREATE MATERIALIZED VIEW lineitem_count AS
SELECT
    COUNT(*) as cnt
FROM
    lineitem_rw;
