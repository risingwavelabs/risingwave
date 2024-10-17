-- noinspection SqlNoDataSourceInspectionForFile
-- noinspection SqlResolveForFile
CREATE SINK nexmark_q105_without_limit_temporal_filter AS
SELECT
    a.id AS auction_id,
    a.item_name AS auction_item_name,
    COUNT(b.auction) AS bid_count
FROM auction a
JOIN bid_filtered b ON a.id = b.auction
GROUP BY a.id, a.item_name
WITH ( connector = 'blackhole', type = 'append-only', force_append_only = 'true');
