-- A self-made query that covers outer join.
--
-- Monitor ongoing auctions and track the current highest bid for each one in real-time. If
-- the auction has no bids, the highest bid will be NULL.

CREATE MATERIALIZED VIEW nexmark_q101
AS
SELECT
    a.id AS auction_id,
    a.item_name AS auction_item_name,
    b.max_price AS current_highest_bid
FROM auction a
LEFT OUTER JOIN (
    SELECT
        b1.auction,
        MAX(b1.price) max_price
    FROM bid b1
    GROUP BY b1.auction
) b ON a.id = b.auction;
