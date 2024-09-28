-- noinspection SqlNoDataSourceInspectionForFile
-- noinspection SqlResolveForFile
CREATE TABLE side_input(
    key BIGINT PRIMARY KEY,
    value VARCHAR
);
INSERT INTO side_input SELECT v, v::varchar FROM generate_series(0, ${BENCHMARK_NEXMARK_RISINGWAVE_Q13_SIDE_INPUT_ROW_COUNT} - 1) AS s(v);

CREATE SINK nexmark_q13_non_lookup_cond AS
SELECT B.auction, B.bidder, B.price, B.date_time, S.value
FROM bid B join side_input FOR SYSTEM_TIME AS OF PROCTIME() S
ON mod(B.auction, ${BENCHMARK_NEXMARK_RISINGWAVE_Q13_SIDE_INPUT_ROW_COUNT}) = S.key
  AND S.key % 4 != 1
WITH ( connector = 'blackhole', type = 'append-only', force_append_only = 'true');
