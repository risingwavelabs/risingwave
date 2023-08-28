#!/usr/bin/env bash

set -euo pipefail

# Lower = less precision, faster
# Higher = more precision, slower
PRECISION=2

./risedev d

echo "--- running ddl"
./risedev psql -c "
CREATE TABLE input (value BIGINT);
"

echo "--- running dml"
./risedev psql -c "
INSERT INTO input SELECT n FROM generate_series(1,10) AS n;
flush;
"


echo "--- creating hdr_histogram"
./risedev psql -c "
CREATE MATERIALIZED VIEW hdr_histogram AS
SELECT
  -- Sign
  CASE WHEN value<0 THEN -1 ELSE 1 END AS sign,

  -- Exponent
  trunc(log10(value))::int AS exponent,

  -- Mantissa
  trunc(pow(10.0, $PRECISION) * (value / pow(10.0, trunc(log10(value))::int) - 1.0))::int AS mantissa,

  --- Frequency of each bucket
  count(*) AS frequency,

  --- dummy to force stream nested loop join
  1 as dummy
FROM input
GROUP BY sign, exponent, mantissa;
"

echo "--- create hdr_distribution parts"
./risedev psql -c "
CREATE MATERIALIZED VIEW hdr_sum AS
SELECT
  sum(frequency) AS sum_frequency
FROM hdr_histogram;
"

echo "--- create hdr_distribution"
./risedev psql -c "
EXPLAIN CREATE MATERIALIZED VIEW hdr_distribution AS
SELECT
  h.sign*(1.0+h.mantissa/pow(10.0, $PRECISION))*pow(10.0,h.exponent) AS bucket,
  h.frequency,
  sum(g.frequency) AS cumulative_frequency
  -- Compute this in batch query to avoid nested loop join.
  -- sum(g.frequency) / (SELECT sum_frequency FROM hdr_sum) AS cumulative_distribution
FROM hdr_histogram g JOIN hdr_histogram h ON g.dummy = h.dummy
WHERE (g.sign,g.exponent,g.mantissa) <= (h.sign,h.exponent,h.mantissa)
GROUP BY h.sign, h.exponent, h.mantissa, h.frequency
ORDER BY cumulative_frequency;
" </dev/null

echo "--- create hdr_distribution"
./risedev psql -c "
CREATE MATERIALIZED VIEW hdr_distribution AS
SELECT
  h.sign*(1.0+h.mantissa/pow(10.0, $PRECISION))*pow(10.0,h.exponent) AS bucket,
  h.frequency,
  sum(g.frequency) AS cumulative_frequency
  -- Compute this in batch query to avoid nested loop join.
  -- sum(g.frequency) / (SELECT sum_frequency FROM hdr_sum) AS cumulative_distribution
FROM hdr_histogram g JOIN hdr_histogram h ON g.dummy = h.dummy
WHERE (g.sign,g.exponent,g.mantissa) <= (h.sign,h.exponent,h.mantissa)
GROUP BY h.sign, h.exponent, h.mantissa, h.frequency
ORDER BY cumulative_frequency;
"

./risedev psql -c "
CREATE INDEX hdr_distribution_idx ON hdr_distribution (cumulative_frequency);
"

echo "--- inserting more values"
./risedev psql -c "
INSERT INTO input SELECT n FROM generate_series(11,10001) AS n;
flush;
"

echo "--- dynamic filter for approx percentile 0.9"
./risedev psql -c "
DROP MATERIALIZED VIEW approx_percentile_90_percent;
DROP MATERIALIZED VIEW use_dynamic_filter;
CREATE MATERIALIZED VIEW use_dynamic_filter AS
SELECT sum_frequency * 0.9 AS scaled_sum_freq FROM hdr_sum;

CREATE MATERIALIZED VIEW approx_percentile_90_percent AS
SELECT bucket AS approximate_percentile
FROM hdr_distribution x, use_dynamic_filter y
WHERE x.cumulative_frequency >= y.scaled_sum_freq
ORDER BY cumulative_frequency
LIMIT 1;
SELECT * FROM approx_percentile_90_percent;
"

echo "--- batch for approx percentile 0.9"
./risedev psql -c "
SELECT bucket AS approximate_percentile
FROM hdr_distribution
WHERE (cumulative_frequency / (SELECT sum_frequency FROM hdr_sum)) >= 0.9
ORDER BY cumulative_frequency
LIMIT 1;
"

echo "--- reading updated approx_percentile"
./risedev psql -c "SELECT * FROM hdr_distribution ORDER BY cumulative_frequency;"

echo "--- show N of buckets"
./risedev psql -c "select * from hdr_distribution;"

# ./risedev k
