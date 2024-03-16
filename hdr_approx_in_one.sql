CREATE TABLE input (value BIGINT);

---------- NOTES ----------

-- In terms of preference,
-- I would prefer Option 1 > Option 2 > Option 3.
--
-- Here are the pros and cons of each approach:
-- Option 1:
-- Pros: Most general solution.
-- Cons: User needs to write a bit more sql for each percentile. But it can be wrapped in an sql function too.
--
-- Option 2:
-- Pros: Second most general solution, works for multiple percentiles. Can just select from it, filter for the percentile we want.
-- Cons: Can't be extended with new percentiles.
--
-- Option 3:
-- Pros: Can use the same syntax as in batch, write less sql.
-- Cons: Can't share state, can't be extended with new percentiles.

---------- Option 1: Create HDR Distribution, Create percentile from it --------

-- Definition:
-- hdr_distribution(table, column numeric, precision int);
-- This is a table function.
--
-- Returns:
-- | bucket::int | frequency::int |

-- Semantics of `hdr_distribution`:
CREATE MATERIALIZED VIEW hdr_distribution AS
WITH hdr_histogram as (SELECT
                CASE WHEN value<0 THEN -1 ELSE 1 END AS sign,
                trunc(log10(value))::int AS exponent,
                    trunc(
                            pow(10.0, 2)
                            * (value / pow(10.0, trunc(log10(value))::int) - 1.0))::int AS mantissa,
                    count(*) AS frequency
     FROM input
     GROUP BY sign, exponent, mantissa)
SELECT
    (sign*(1.0+mantissa/pow(10.0, 2))*pow(10.0,exponent))::int AS bucket,
    -- This is window aggregation used to compute the cumulative frequency.
        (sum(frequency)
            OVER (PARTITION BY NULL
      ORDER BY (sign, exponent, mantissa)
      ROWS UNBOUNDED PRECEDING)
            ) AS cumulative_frequency
FROM hdr_histogram g
GROUP BY sign, exponent, mantissa, frequency
ORDER BY cumulative_frequency;

-- User supplied: Maintain the max frequency.
-- Can be shared across different percentiles.
CREATE MATERIALIZED VIEW hdr_sum AS
SELECT
    max(cumulative_frequency) AS sum_frequency
FROM hdr_distribution;

-- User supplied: Dynamic filter.
-- Create 1 per percentile.
CREATE MATERIALIZED VIEW approx_percentile_90_percent AS
WITH frequency_at_90 as (SELECT sum_frequency * 0.9 AS scaled_sum_freq FROM hdr_sum)
SELECT bucket AS approximate_percentile
FROM hdr_distribution x, frequency_at_90 y
WHERE x.cumulative_frequency >= y.scaled_sum_freq
ORDER BY cumulative_frequency
LIMIT 1;

DROP MATERIALIZED VIEW approx_percentile_90_percent;
DROP MATERIALIZED VIEW hdr_sum;
DROP MATERIALIZED VIEW hdr_distribution;

---------- Option 2: Multiple percentiles, pre-defined, all in one query --------

-- Definition:
-- approx_percentile(table, column numeric, precision int, *percentiles float);
-- This is a table function.
-- Returns:
-- | approx_value::int | percentile::float |

-- semantics of approx_percentile:
EXPLAIN CREATE MATERIALIZED VIEW approx_percentile_90_and_50 AS
WITH hdr_histogram as
    (SELECT
        CASE WHEN value<0 THEN -1 ELSE 1 END AS sign,
        trunc(log10(value))::int AS exponent,
            trunc(
                    pow(10.0, 2)
                    * (value / pow(10.0, trunc(log10(value))::int) - 1.0))::int AS mantissa,
            count(*) AS frequency
     FROM input
     GROUP BY sign, exponent, mantissa),
    hdr_distribution as
        (SELECT
        (sign*(1.0+mantissa/pow(10.0, 2))*pow(10.0,exponent))::int AS bucket,
        -- This is window aggregation used to compute the cumulative frequency.
            (sum(frequency)
                OVER (PARTITION BY NULL
          ORDER BY (sign, exponent, mantissa)
          ROWS UNBOUNDED PRECEDING)
                ) AS cumulative_frequency
            FROM hdr_histogram g
        GROUP BY sign, exponent, mantissa, frequency
        ORDER BY cumulative_frequency),
    hdr_sum as
        (SELECT
            max(cumulative_frequency) AS sum_frequency
        FROM hdr_distribution),
    frequency_at_90 as (SELECT sum_frequency * 0.9 AS scaled_sum_freq FROM hdr_sum),
    approx_percentile_90 as (
        SELECT bucket AS approx_value, 0.9 AS percentile
                FROM hdr_distribution x, frequency_at_90 y
                WHERE x.cumulative_frequency >= y.scaled_sum_freq
                ORDER BY cumulative_frequency
                    LIMIT 1),
    frequency_at_50 as (SELECT sum_frequency * 0.5 AS scaled_sum_freq FROM hdr_sum),
    approx_percentile_50 as(
        SELECT bucket AS approx_value, 0.5 AS percentile
                FROM hdr_distribution x, frequency_at_50 y
                WHERE x.cumulative_frequency >= y.scaled_sum_freq
                ORDER BY cumulative_frequency
                    LIMIT 1)
    -- FIXME(kwannoel): The optimizer seems to infer stream nested loop join for this part.
    SELECT * FROM approx_percentile_90; -- UNION ALL SELECT * FROM approx_percentile_50;

---------- Option 3: Single percentile, pre-defined, all in one query --------

-- Definition:
-- approx_percentile(column numeric, precision int, *percentiles float)
-- This is an aggregate expression. The table will be inferred from the `FROM` clause.
-- Returns:
-- | approx_value::int |

-- NOTE(kwannoel): This disadvantage of this approach is that common streaming sub-graph
-- can't be shared across different percentiles.
-- This is the least general solution and I would discourage it.
-- The only advantage of this approach is that it follows the same syntax as in batch,
-- where the user can specify the percentile as a select item.
-- But because we can't share the HDR distribution state, whenever we want a new percentile
-- We re-compute the whole state again which seems wasteful.
EXPLAIN CREATE MATERIALIZED VIEW approx_percentile_90 AS
WITH hdr_histogram as
    (SELECT
        CASE WHEN value<0 THEN -1 ELSE 1 END AS sign,
        trunc(log10(value))::int AS exponent,
            trunc(
                    pow(10.0, 2)
                    * (value / pow(10.0, trunc(log10(value))::int) - 1.0))::int AS mantissa,
            count(*) AS frequency
     FROM input
     GROUP BY sign, exponent, mantissa),
    hdr_distribution as
        (SELECT
        (sign*(1.0+mantissa/pow(10.0, 2))*pow(10.0,exponent))::int AS bucket,
        -- This is window aggregation used to compute the cumulative frequency.
            (sum(frequency)
                OVER (PARTITION BY NULL
          ORDER BY (sign, exponent, mantissa)
          ROWS UNBOUNDED PRECEDING)
                ) AS cumulative_frequency
            FROM hdr_histogram g
        GROUP BY sign, exponent, mantissa, frequency
        ORDER BY cumulative_frequency),
    hdr_sum as
        (SELECT
            max(cumulative_frequency) AS sum_frequency
        FROM hdr_distribution),
    frequency_at_90 as (SELECT sum_frequency * 0.9 AS scaled_sum_freq FROM hdr_sum),
    approx_percentile_90 as (
        SELECT bucket, 0.9 AS approximate_percentile
                FROM hdr_distribution x, frequency_at_90 y
                WHERE x.cumulative_frequency >= y.scaled_sum_freq
                ORDER BY cumulative_frequency
                    LIMIT 1)
   SELECT * FROM approx_percentile_90;

---------- Cleanup --------
DROP TABLE input;