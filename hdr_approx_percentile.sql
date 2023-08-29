-- --------
-- Overview
-- --------
--
-- This is a demo test to show how to use hdr_histogram (http://hdrhistogram.org/)
-- to compute `approx_percentile` in the RisingWave stream engine.
--
-- The advantage of this approach is that it supports updatable approx_percentile.
-- The disadvantage is that it requires quadratic computation and storage
-- as the precision increases,
-- due to streaming nested loop join.
--
-- The way it works is that hdr_histogram stores N significant digits of each value,
-- thereby reducing the number of buckets needed to store the distribution.
--
-- ----------------------
-- Implementation details
-- ----------------------
--
-- First we create the histogram itself, where each value is encoded as a triple:
-- 1. The sign of the value (1 or -1)
-- 2. The exponent of the value (the power of 10, truncated to an integer)
-- 3. The significand of the value (the digits after the decimal point, truncated to $PRECISION digits)
--
-- With the following parameters:
-- precision=2 (significand is 2 digits)
-- value=123456
--
-- The exponent will be 5 (log10(123456) = 5.0915... ~ 5)
-- The significand will be 23 (123456 / 10^5 - 1 = 1.23... ).
-- Then the histogram will store the triple (1, 5, 23)
--
-- Next we do a window aggregation on the histogram to compute the
-- cumulative frequency of each bucket.
-- | Bucket       | 4 | 311 | 400 | 521 |
-- | Counts       | 3 | 6   | 7   | 8   |
-- | C. Frequency | 3 | 9   | 16  | 24  |
--
-- Finally we can compute the approximate percentile
-- 1. Compute the sum of all frequencies. (24)
-- 2. Select a percentile (90%)
-- 3. Compute the frequency corresponding to the percentile (24 * 0.9 = 21.6)
-- 4. Find the first bucket with the cumulative frequency >= 21.6 (521)
--
-- Step 3 and 4 are required for us to use dynamic filter,
-- since we don't support complex expressions in the dynamic filter.
-- In the future when our optimizer can simplify the dynamic filter, we can avoid it.
--
-- ----------
-- Test Notes
-- ----------
--
-- We will test the following parameters:
-- - 90 percentile.
-- - 50 percentile.
-- - Deletes.
-- - Compare with percentile_disc. The significand should match.

CREATE TABLE input (value BIGINT);

INSERT INTO input SELECT * FROM generate_series(1,10);

INSERT INTO input SELECT * FROM generate_series(1000, 1100);

INSERT INTO input SELECT * FROM generate_series(10000, 11000);

INSERT INTO input SELECT * FROM generate_series(100000, 110000);

FLUSH;

---------- Create HDR Histogram + HDR Distribution --------

-- This section contains the main logic of the HDR Histogram.

-- Here `pow(10.0, 4)` is responsible for setting the precision of the histogram.
-- "2" is the precision.
-- We also need a "dummy" column for as a workaround for stream nested loop join.
CREATE MATERIALIZED VIEW hdr_histogram AS
SELECT
  CASE WHEN value<0 THEN -1 ELSE 1 END AS sign,
  trunc(log10(value))::int AS exponent,
  trunc(
    pow(10.0, 4)
     * (value / pow(10.0, trunc(log10(value))::int) - 1.0))::int AS mantissa,
  count(*) AS frequency,
  1 as dummy
FROM input
GROUP BY sign, exponent, mantissa;

CREATE MATERIALIZED VIEW hdr_sum AS
SELECT
  sum(frequency) AS sum_frequency
FROM hdr_histogram;

-- Here `pow(10.0, 4)` is responsible for recovering the position of the mantissa, with the precision.
-- "2" is the precision.
CREATE MATERIALIZED VIEW hdr_distribution AS
SELECT
    (sign*(1.0+mantissa/pow(10.0, 4))*pow(10.0,exponent))::int AS bucket,
    -- This is window aggregation used to compute the cumulative frequency.
    (sum(frequency)
        OVER (PARTITION BY NULL
      ORDER BY (sign, exponent, mantissa)
      ROWS UNBOUNDED PRECEDING)
        ) AS cumulative_frequency
FROM hdr_histogram g
GROUP BY sign, exponent, mantissa, frequency
ORDER BY cumulative_frequency;

CREATE INDEX hdr_distribution_idx ON hdr_distribution (cumulative_frequency);

---------- 90-percentile ----------

CREATE MATERIALIZED VIEW frequency_at_90 AS
SELECT sum_frequency * 0.9 AS scaled_sum_freq FROM hdr_sum;

CREATE MATERIALIZED VIEW approx_percentile_90_percent AS
SELECT bucket AS approximate_percentile
FROM hdr_distribution x, frequency_at_90 y
WHERE x.cumulative_frequency >= y.scaled_sum_freq
ORDER BY cumulative_frequency
LIMIT 1;

---------- test-90-percentile ----------

SELECT * FROM approx_percentile_90_percent;
-- 108000.00

SELECT percentile_disc(0.9) WITHIN GROUP (ORDER BY value) FROM input;
-- 108889

SELECT
 (abs(percentile_disc - approximate_percentile) / percentile_disc) < 0.01
FROM approx_percentile_90_percent, (SELECT percentile_disc(0.9) WITHIN GROUP (ORDER BY value) FROM input);
-- t

DROP MATERIALIZED VIEW approx_percentile_90_percent;

DROP MATERIALIZED VIEW frequency_at_90;

---------- 50-percentile ----------

CREATE MATERIALIZED VIEW frequency_at_50 AS
SELECT sum_frequency * 0.5 AS scaled_sum_freq FROM hdr_sum;

CREATE MATERIALIZED VIEW approx_percentile_50_percent AS
SELECT bucket AS approximate_percentile
FROM hdr_distribution x, frequency_at_50 y
WHERE x.cumulative_frequency >= y.scaled_sum_freq
ORDER BY cumulative_frequency
LIMIT 1;

---------- test-50-percentile ----------

SELECT * FROM approx_percentile_50_percent;
-- 104000.00

SELECT percentile_disc(0.5) WITHIN GROUP (ORDER BY value) FROM input;
-- 104444

SELECT
 (abs(percentile_disc - approximate_percentile) / percentile_disc) < 0.01
FROM approx_percentile_50_percent, (SELECT percentile_disc(0.5) WITHIN GROUP (ORDER BY value) FROM input);
-- t

--- Delete > 100000

DELETE FROM input WHERE value > 100000;

flush;

SELECT * FROM approx_percentile_50_percent;
-- 10400.00

SELECT percentile_disc(0.5) WITHIN GROUP (ORDER BY value) FROM input;
-- 10445

SELECT
 (abs(percentile_disc - approximate_percentile) / percentile_disc) < 0.01
FROM approx_percentile_50_percent, (SELECT percentile_disc(0.5) WITHIN GROUP (ORDER BY value) FROM input);
-- t

--- Delete > 10000

DELETE FROM input WHERE value > 10000;

flush;

SELECT * FROM approx_percentile_50_percent;
-- 1040.00

SELECT percentile_disc(0.5) WITHIN GROUP (ORDER BY value) FROM input;
-- 1045

SELECT
 (abs(percentile_disc - approximate_percentile) / percentile_disc) < 0.01
FROM approx_percentile_50_percent, (SELECT percentile_disc(0.5) WITHIN GROUP (ORDER BY value) FROM input);
-- t


--- Delete > 100

DELETE FROM input WHERE value > 100;

flush;

SELECT * FROM approx_percentile_50_percent;
-- 5.0

SELECT percentile_disc(0.5) WITHIN GROUP (ORDER BY value) FROM input;
-- 5

SELECT
 percentile_disc = (approximate_percentile::int)
FROM approx_percentile_50_percent, (SELECT percentile_disc(0.5) WITHIN GROUP (ORDER BY value) FROM input);
-- t

DROP MATERIALIZED VIEW approx_percentile_50_percent;

DROP MATERIALIZED VIEW frequency_at_50;

---------- Cleanup ----------

DELETE FROM input WHERE value < 1400;

DROP INDEX hdr_distribution_idx;

DROP MATERIALIZED VIEW hdr_distribution;

DROP MATERIALIZED VIEW hdr_sum;

DROP MATERIALIZED VIEW hdr_histogram;

DROP TABLE input;