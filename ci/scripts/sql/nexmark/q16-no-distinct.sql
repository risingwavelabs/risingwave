-- noinspection SqlNoDataSourceInspectionForFile
-- noinspection SqlResolveForFile
SET rw_force_two_phase_agg = ${BENCHMARK_NEXMARK_RISINGWAVE_Q16_NO_DISTINCT_RW_FORCE_TWO_PHASE_AGG};
CREATE SINK nexmark_q16_no_distinct AS
SELECT channel,
       to_char(date_time, 'YYYY-MM-DD')                                          as "day",
       max(to_char(date_time, 'HH:mm'))                                          as "minute",
       count(*)                                                                  AS total_bids,
       count(*) filter (where price < 10000)                                     AS rank1_bids,
       count(*) filter (where price >= 10000 and price < 1000000)                AS rank2_bids,
       count(*) filter (where price >= 1000000)                                  AS rank3_bids,
       count(bidder)                                                    AS total_bidders,
       count(bidder) filter (where price < 10000)                       AS rank1_bidders,
       count(bidder) filter (where price >= 10000 and price < 1000000)  AS rank2_bidders,
       count(bidder) filter (where price >= 1000000)                    AS rank3_bidders,
       count(auction)                                                   AS total_auctions,
       count(auction) filter (where price < 10000)                      AS rank1_auctions,
       count(auction) filter (where price >= 10000 and price < 1000000) AS rank2_auctions,
       count(auction) filter (where price >= 1000000)                   AS rank3_auctions
FROM bid
GROUP BY to_char(date_time, 'YYYY-MM-DD'), channel
WITH ( connector = 'blackhole', type = 'append-only', force_append_only = 'true');
