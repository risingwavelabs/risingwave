-- noinspection SqlNoDataSourceInspectionForFile
-- noinspection SqlResolveForFile
CREATE SINK nexmark_q0_temporal_filter
AS
SELECT auction, bidder, price, date_time
FROM bid_filtered
WITH ( connector = 'blackhole', type = 'append-only', force_append_only = 'true');
