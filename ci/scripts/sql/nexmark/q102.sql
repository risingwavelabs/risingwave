-- noinspection SqlNoDataSourceInspectionForFile
-- noinspection SqlResolveForFile
CREATE SINK nexmark_q102 AS
SELECT
    a.id AS auction_id,
    a.item_name AS auction_item_name,
    COUNT(b.auction) AS bid_count
FROM auction a
JOIN bid b ON a.id = b.auction
GROUP BY a.id, a.item_name
HAVING COUNT(b.auction) >= (
    SELECT COUNT(*) / COUNT(DISTINCT auction) FROM bid
)
WITH ( connector = 'blackhole', type = 'append-only', force_append_only = 'true');
