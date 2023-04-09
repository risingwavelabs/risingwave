-- A self-made query that covers semi join.
--
-- Show the auctions that have at least 20 bids.

CREATE MATERIALIZED VIEW nexmark_q103
AS
SELECT
    a.id AS auction_id,
    a.item_name AS auction_item_name
FROM auction a
WHERE a.id IN (
    SELECT b.auction FROM bid b
    GROUP BY b.auction
    HAVING COUNT(*) >= 20
);
