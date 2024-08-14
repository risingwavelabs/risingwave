-- noinspection SqlNoDataSourceInspectionForFile
-- noinspection SqlResolveForFile
CREATE SINK nexmark_q107 AS
SELECT
    approx_percentile(0.01, 0.01) within group (order by price) as p01,
    approx_percentile(0.1, 0.01) within group (order by price) as p10,
    approx_percentile(0.5, 0.01) within group (order by price) as p50,
    approx_percentile(0.9, 0.01) within group (order by price) as p90,
    approx_percentile(0.99, 0.01) within group (order by price) as p99
FROM bid
WITH ( connector = 'blackhole', type = 'append-only', force_append_only = 'true');
