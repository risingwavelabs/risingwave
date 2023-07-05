-- noinspection SqlNoDataSourceInspectionForFile
-- noinspection SqlResolveForFile
CREATE SINK nexmark_q5
AS
SELECT
    AuctionBids.auction, AuctionBids.num
FROM (
    SELECT
        bid.auction,
        count(*) AS num,
        window_start AS starttime
    FROM
        HOP(bid, date_time, INTERVAL '2' SECOND, INTERVAL '10' SECOND)
    GROUP BY
        bid.auction,
        window_start
) AS AuctionBids
JOIN (
	SELECT
        max(CountBids.num) AS maxn,
        CountBids.starttime_c
	FROM (
		SELECT
            count(*) AS num,
            window_start AS starttime_c
		FROM
            HOP(bid, date_time, INTERVAL '2' SECOND, INTERVAL '10' SECOND)
        GROUP BY
            bid.auction,
            window_start
		) AS CountBids
	GROUP BY
        CountBids.starttime_c
	) AS MaxBids
ON
    AuctionBids.starttime = MaxBids.starttime_c AND
    AuctionBids.num >= MaxBids.maxn
WITH ( connector = 'blackhole', type = 'append-only', force_append_only = 'true');
