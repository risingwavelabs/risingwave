-- CREATE MATERIALIZED VIEW product_count AS
-- SELECT
--     product_id,
--     COUNT(*) as product_count
-- FROM
--     orders_rw
-- GROUP BY
--     product_id;

CREATE MATERIALIZED VIEW lineitem_count AS
SELECT
    COUNT(*) as cnt
FROM
    lineitem_rw;
