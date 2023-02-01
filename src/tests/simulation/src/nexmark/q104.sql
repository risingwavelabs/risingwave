-- A self-made query that covers anti join.
--
-- This is the same as q103, which shows the auctions that have at least 20 bids.

CREATE MATERIALIZED VIEW nexmark_q104
AS
SELECT
    a.id AS auction_id,
    a.item_name AS auction_item_name
FROM auction a
WHERE a.id NOT IN (
    SELECT b.auction FROM bid b
    GROUP BY b.auction
    HAVING COUNT(*) < 20
);
