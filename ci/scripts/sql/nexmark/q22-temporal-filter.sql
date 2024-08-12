-- noinspection SqlNoDataSourceInspectionForFile
-- noinspection SqlResolveForFile
CREATE SINK nexmark_q22_temporal_filter AS
SELECT auction,
       bidder,
       price,
       channel,
       split_part(url, '/', 4) as dir1,
       split_part(url, '/', 5) as dir2,
       split_part(url, '/', 6) as dir3
FROM bid_filtered
WITH ( connector = 'blackhole', type = 'append-only', force_append_only = 'true');
