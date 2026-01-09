-- Covers EOWC sort + EOWC over window.
-- Note: we intentionally avoid the group-top-n/row_number pattern, which won't plan an over window.

CREATE MATERIALIZED VIEW nexmark_q107_eowc AS
SELECT
    auction,
    bidder,
    date_time,
    price,
    AVG(price) OVER (
        PARTITION BY auction
        ORDER BY date_time
        ROWS BETWEEN 10 PRECEDING AND CURRENT ROW
    ) AS avg_price_10,
    MAX(price) OVER (
        PARTITION BY auction
        ORDER BY date_time
        ROWS BETWEEN 10 PRECEDING AND CURRENT ROW
    ) AS max_price_10
FROM
    bid
WHERE
    auction % 100 = 0
EMIT ON WINDOW CLOSE;
