-- noinspection SqlNoDataSourceInspectionForFile
-- noinspection SqlResolveForFile
CREATE SINK nexmark_q1_temporal_filter
AS
SELECT auction,
       bidder,
       0.908 * price as price,
       date_time
FROM bid_filtered
WITH ( connector = 'blackhole', type = 'append-only', force_append_only = 'true');
