-- noinspection SqlNoDataSourceInspectionForFile
-- noinspection SqlResolveForFile
CREATE SINK nexmark_q7_temporal_filter
AS
SELECT B.auction,
       B.price,
       B.bidder,
       B.date_time
from bid_filtered B
         JOIN (SELECT MAX(price) AS maxprice,
                      window_end as date_time
               FROM
                   TUMBLE(bid_filtered, date_time, INTERVAL '10' SECOND)
               GROUP BY window_end) B1 ON B.price = B1.maxprice
WHERE B.date_time BETWEEN B1.date_time - INTERVAL '10' SECOND
          AND B1.date_time
WITH ( connector = 'blackhole', type = 'append-only', force_append_only = 'true');
