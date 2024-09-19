-- https://web.archive.org/web/20100620010601/http://datalab.cs.pdx.edu/niagaraST/NEXMark/
-- The original q5 is `[RANGE 60 MINUTE SLIDE 1 MINUTE]`.
-- However, using 60 minute may require running a very long period to see the effect.
-- Therefore, we change it to `[RANGE 5 MINUTE SLIDE 5 SECOND]` to generate many sliding windows.
-- The percentage between window size and hop interval stays the same as the one in original nexmark.
-- noinspection SqlNoDataSourceInspectionForFile
-- noinspection SqlResolveForFile
CREATE SINK nexmark_q5_many_windows_temporal_filter
AS
SELECT
    AuctionBids.auction, AuctionBids.num
FROM (
    SELECT
        bid.auction,
        count(*) AS num,
        window_start AS starttime
    FROM
        HOP(bid_filtered, date_time, INTERVAL '5' SECOND, INTERVAL '5' MINUTE) as bid
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
            HOP(bid_filtered, date_time, INTERVAL '5' SECOND, INTERVAL '5' MINUTE) as bid
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
