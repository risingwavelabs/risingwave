#!/usr/bin/env bash

set -euo pipefail

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
  CASE WHEN value<0 THEN -1 ELSE 1 END AS sign,

  -- rw
  trunc(log(2.0))::int AS exponent,
  -- presto
  -- trunc(log(2.0, abs(value)))::int AS exponent,

  -- rw
  trunc(pow(2.0, 4) * (value / pow(2.0, trunc(log(2.0))::int) - 1.0))::int AS mantissa,
  -- presto
  -- trunc(pow(2.0, 4) * (value / pow(2.0, trunc(log(2.0, abs(value)))::int) - 1.0))::int AS mantissa,

  count(*) AS frequency
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
CREATE VIEW hdr_distribution AS
SELECT
  h.sign*(1.0+h.mantissa/pow(2.0, 4))*pow(2.0,h.exponent) AS bucket,
  h.frequency,
  sum(g.frequency) AS cumulative_frequency,
  sum(g.frequency) / (SELECT sum_frequency FROM hdr_sum) AS cumulative_distribution
FROM hdr_histogram g, hdr_histogram h
WHERE (g.sign,g.exponent,g.mantissa) <= (h.sign,h.exponent,h.mantissa)
GROUP BY h.sign, h.exponent, h.mantissa, h.frequency
ORDER BY cumulative_distribution;
"

echo "--- check approx percentile 0.9"
./risedev psql -c "
SELECT bucket AS approximate_percentile
FROM hdr_distribution
WHERE cumulative_distribution >= 0.9
ORDER BY cumulative_distribution
LIMIT 1;
"

#./risedev psql -c "
#CREATE INDEX hdr_distribution_idx ON hdr_distribution (cumulative_distribution);
#"

echo "--- reading from approx_percentile"
./risedev psql -c "
SELECT * FROM hdr_distribution;
"

echo "--- inserting more values"
./risedev psql -c "
INSERT INTO input SELECT n FROM generate_series(11,10001) AS n;
flush;
"

echo "--- reading updated approx_percentile"
./risedev psql -c "SELECT * FROM hdr_distribution ORDER BY cumulative_distribution;"

./risedev k

