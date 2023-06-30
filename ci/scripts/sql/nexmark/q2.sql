-- noinspection SqlNoDataSourceInspectionForFile
-- noinspection SqlResolveForFile
CREATE SINK nexmark_q2
AS
SELECT auction, price
FROM bid
WHERE auction = 1007
   OR auction = 1020
   OR auction = 2001
   OR auction = 2019
   OR auction = 2087
WITH ( connector = 'blackhole', type = 'append-only');
