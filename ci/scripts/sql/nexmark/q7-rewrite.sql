-- noinspection SqlNoDataSourceInspectionForFile
-- noinspection SqlResolveForFile
CREATE SINK nexmark_q7_rewrite AS
SELECT
  B.auction,
  B.price,
  B.bidder,
  B.date_time
FROM (
  SELECT
    auction,
    price,
    bidder,
    date_time,
    /*use rank here to express top-N with ties*/
    rank() over (partition by window_end order by price desc) as price_rank
  FROM
    TUMBLE(bid, date_time, INTERVAL '10' SECOND)
) B
WHERE price_rank <= 1
WITH ( connector = 'blackhole', type = 'append-only', force_append_only = 'true');
