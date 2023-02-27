-- A self-made query that covers two-phase stateful simple aggregation.
--
-- Show the minimum final price of all auctions.

SELECT
    MIN(final) AS min_final
FROM
    (
        SELECT
            auction.id,
            MAX(price) AS final
        FROM
            auction,
            bid
        WHERE
            bid.auction = auction.id
            AND bid.date_time BETWEEN auction.date_time AND auction.expires
        GROUP BY
            auction.id
    )
