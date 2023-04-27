-- noinspection SqlNoDataSourceInspectionForFile
-- noinspection SqlResolveForFile
CREATE SINK nexmark_q0
AS
SELECT auction, bidder, price, date_time
FROM bid
WITH ( connector = 'blackhole', type = 'append-only');
