-- noinspection SqlNoDataSourceInspectionForFile
-- noinspection SqlResolveForFile
CREATE SINK nexmark_q22 AS
SELECT auction,
       bidder,
       price,
       channel,
       split_part(url, '/', 4) as dir1,
       split_part(url, '/', 5) as dir2,
       split_part(url, '/', 6) as dir3
FROM bid
WITH ( connector = 'blackhole', type = 'append-only');
