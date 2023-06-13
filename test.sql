create source bid (
    auction BIGINT,
    bidder BIGINT,
    price BIGINT,
    channel VARCHAR,
    url VARCHAR,
    date_time TIMESTAMP,
    extra VARCHAR)
with (
    connector = 'nexmark',
    nexmark.table.type = 'Bid',
    nexmark.split.num = '4',
    nexmark.min.event.gap.in.ns = '100'
);


    create source auction (
      id BIGINT,
      item_name VARCHAR,
      description VARCHAR,
      initial_bid BIGINT,
      reserve BIGINT,
      date_time TIMESTAMP,
      expires TIMESTAMP,
      seller BIGINT,
      category BIGINT,
      extra VARCHAR)
    with (
      connector = 'nexmark',
      nexmark.table.type = 'Auction',
          nexmark.split.num = '4',
    nexmark.min.event.gap.in.ns = '100'
    );

    create source bid (
      auction BIGINT,
      bidder BIGINT,
      price BIGINT,
      channel VARCHAR,
      url VARCHAR,
      date_time TIMESTAMP,
      extra VARCHAR)
    with (
      connector = 'nexmark',
      nexmark.table.type = 'Bid',
          nexmark.split.num = '4',
    nexmark.min.event.gap.in.ns = '100'
    );

    create source person (
      id BIGINT,
      name VARCHAR,
      email_address VARCHAR,
      credit_card VARCHAR,
      city VARCHAR,
      state VARCHAR,
      date_time TIMESTAMP,
      extra VARCHAR)
    with (
      connector = 'nexmark',
      nexmark.table.type = 'Person',
          nexmark.split.num = '4',
    nexmark.min.event.gap.in.ns = '100'
    );

create materialized view q5 as 
SELECT AuctionBids.auction, AuctionBids.num FROM (
      SELECT
        bid.auction,
        count(*) AS num,
        window_start AS starttime
      FROM
        HOP(bid, date_time, INTERVAL '2' SECOND, INTERVAL '10' SECOND)
      GROUP BY
        window_start,
        bid.auction
    ) AS AuctionBids
    JOIN (
      SELECT
        max(CountBids.num) AS maxn,
        CountBids.starttime_c
      FROM (
        SELECT
          count(*) AS num,
          window_start AS starttime_c
        FROM HOP(bid, date_time, INTERVAL '2' SECOND, INTERVAL '10' SECOND)
        GROUP BY
          bid.auction,
          window_start
      ) AS CountBids
      GROUP BY
        CountBids.starttime_c
    ) AS MaxBids
    ON AuctionBids.starttime = MaxBids.starttime_c AND AuctionBids.num >= MaxBids.maxn;

create materialized view q7 as
SELECT
      B.auction,
      B.price,
      B.bidder,
      B.date_time
    FROM
      bid B
    JOIN (
      SELECT
        MAX(price) AS maxprice,
        window_end as date_time
      FROM
        TUMBLE(bid, date_time, INTERVAL '10' SECOND)
      GROUP BY
        window_end
    ) B1 ON B.price = B1.maxprice
    WHERE
      B.date_time BETWEEN B1.date_time - INTERVAL '10' SECOND
      AND B1.date_time;
