-- Copied from q7, but runs in Emit-On-Window-Close mode to cover EOWC hash agg.

CREATE MATERIALIZED VIEW nexmark_q7_eowc
AS
SELECT
    B.auction,
    B.price,
    B.bidder,
    B.date_time
FROM
    bid B
JOIN (
    SELECT
        MAX(price) AS maxprice,
        window_end as date_time
    FROM
        TUMBLE(bid, date_time, INTERVAL '2' SECOND)
    GROUP BY
        window_end
) B1 ON B.price = B1.maxprice
WHERE
    B.date_time BETWEEN B1.date_time - INTERVAL '2' SECOND
    AND B1.date_time
EMIT ON WINDOW CLOSE;

