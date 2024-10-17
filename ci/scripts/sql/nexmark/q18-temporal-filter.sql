-- noinspection SqlNoDataSourceInspectionForFile
-- noinspection SqlResolveForFile
CREATE SINK nexmark_q18_temporal_filter AS
SELECT auction, bidder, price, channel, url, date_time
FROM (SELECT *,
             ROW_NUMBER() OVER (
                 PARTITION BY bidder, auction
                 ORDER BY date_time DESC
                 ) AS rank_number
      FROM bid_filtered)
WHERE rank_number <= 1
WITH ( connector = 'blackhole', type = 'append-only', force_append_only = 'true');
