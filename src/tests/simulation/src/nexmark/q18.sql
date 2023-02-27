-- Covers group top-n (deduplication).
--
-- Note: the `ROW_NUMBER` is replaced with `RANK` to make the query result deterministic.

CREATE MATERIALIZED VIEW nexmark_q18
AS
SELECT auction, bidder, price, channel, url, date_time, extra
FROM (SELECT *, RANK() OVER (PARTITION BY bidder, auction ORDER BY date_time DESC) AS rank_number
      FROM bid)
WHERE rank_number <= 1;
