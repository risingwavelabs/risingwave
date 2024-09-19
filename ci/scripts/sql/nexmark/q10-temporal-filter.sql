-- noinspection SqlNoDataSourceInspectionForFile
-- noinspection SqlResolveForFile
CREATE SINK nexmark_q10_temporal_filter AS
SELECT auction,
       bidder,
       price,
       date_time,
       TO_CHAR(date_time, 'YYYY-MM-DD') as date,
       TO_CHAR(date_time, 'HH:MI')      as time
FROM bid_filtered
WITH ( connector = 'blackhole', type = 'append-only', force_append_only = 'true');
