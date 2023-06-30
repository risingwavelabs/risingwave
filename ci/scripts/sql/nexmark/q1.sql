-- noinspection SqlNoDataSourceInspectionForFile
-- noinspection SqlResolveForFile
CREATE SINK nexmark_q1
AS
SELECT auction,
       bidder,
       0.908 * price as price,
       date_time
FROM bid
WITH ( connector = 'blackhole', type = 'append-only');
