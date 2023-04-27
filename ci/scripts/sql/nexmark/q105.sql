-- noinspection SqlNoDataSourceInspectionForFile
-- noinspection SqlResolveForFile
CREATE SINK nexmark_q105 AS
SELECT
    a.id AS auction_id,
    a.item_name AS auction_item_name,
    COUNT(b.auction) AS bid_count
FROM auction a
JOIN bid b ON a.id = b.auction
GROUP BY a.id, a.item_name
ORDER BY bid_count DESC
LIMIT 1000
WITH ( connector = 'blackhole', type = 'append-only', force_append_only = 'true');
