-- noinspection SqlNoDataSourceInspectionForFile
-- noinspection SqlResolveForFile
CREATE TABLE side_input(
  key BIGINT PRIMARY KEY,
  value VARCHAR
);
INSERT INTO side_input SELECT i::bigint, i::varchar FROM(SELECT generate_series(0,9999,1) as i);
CREATE SINK nexmark_q13 AS
SELECT B.auction, B.bidder, B.price, B.date_time, S.value
FROM bid B join side_input FOR SYSTEM_TIME AS OF PROCTIME() S on mod(B.auction, 10000) = S.key
WITH ( connector = 'blackhole', type = 'append-only');
