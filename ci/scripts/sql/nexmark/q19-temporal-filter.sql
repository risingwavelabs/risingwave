-- noinspection SqlNoDataSourceInspectionForFile
-- noinspection SqlResolveForFile
CREATE SINK nexmark_q19_temporal_filter AS
SELECT *
FROM (SELECT *,
             ROW_NUMBER() OVER (
                 PARTITION BY auction
                 ORDER BY price DESC
             ) AS rank_number
      FROM bid_filtered)
WHERE rank_number <= 10
WITH ( connector = 'blackhole', type = 'append-only', force_append_only = 'true');
