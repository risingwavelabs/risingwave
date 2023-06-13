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
