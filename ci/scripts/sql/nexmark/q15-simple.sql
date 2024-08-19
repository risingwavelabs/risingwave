-- noinspection SqlNoDataSourceInspectionForFile
-- noinspection SqlResolveForFile
CREATE SINK nexmark_q15_simple AS
SELECT to_char(date_time, 'YYYY-MM-DD')                                          as "day",
       count(*)                                                                  AS total_bids
FROM bid
GROUP BY to_char(date_time, 'YYYY-MM-DD')
WITH ( connector = 'blackhole', type = 'append-only', force_append_only = 'true');
