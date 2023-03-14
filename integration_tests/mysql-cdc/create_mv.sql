CREATE MATERIALIZED VIEW product_count AS
SELECT
    product_id,
    COUNT(*) as product_count
FROM
    orders
GROUP BY
    product_id;