-- Understand dynamic filter.

-- Scenario: Product profit targets
-- Max is the dynamic filter here.
-- It will change as more sales are added.
CREATE TABLE sales (profit_margin int);
CREATE TABLE products (product_name varchar primary key, product_profit int);

EXPLAIN CREATE MATERIALIZED VIEW m1 AS
    WITH max_profit AS
      (SELECT max(profit_margin) max FROM sales)
    SELECT product_name
    FROM products, max_profit
    WHERE product_profit > max;
CREATE MATERIALIZED VIEW m1 AS
    WITH max_profit AS
      (SELECT max(profit_margin) max FROM sales)
    SELECT product_name
    FROM products, max_profit
    WHERE product_profit > max;

INSERT INTO products
VALUES
('A', 10),
('B', 20),
('C', 30),
('D', 40),
('E', 50),
('F', 60),
('G', 70),
('H', 80),
('I', 90),
('J', 100);

INSERT INTO sales
VALUES
(10),
(20),
(30),
(40),
(50),
(60),
(70),
(80),
(90),
(100);

INSERT INTO products
VALUES
('K', 110), -- This gets cached.
('L', 120),
('M', 130);

flush;

SELECT * from m1;

INSERT INTO sales
VALUES
(110);

flush;

SELECT * FROM m1;

-- There's 2 ways to evaluate the above scenario.
-- 1. Streaming Nested loop join.
-- When 1 record comes in, (110) in the above example,
-- The inner side (the filter side) will be re-evaluated. So max becomes 110.
-- The outer side, will need to be joined on the condition that product_profit > 110.
-- We need to scan the entire products table to find out which products have profit > 110.
-- So O(N) cost.
-- Then those are selected and form our results table.
-- Q: Scan entire products table is necessary because products table may not be ordered by product_profit.
-- Which means we can't do range scan (110, +inf) on the products table?
--
-- 2. Dynamic Filter.
-- When 1 record comes in, (110) in the above example,
-- The inner side (the filter side) will be re-evaluated. So max becomes 110.
-- The outer side, will need to be joined on the condition that product_profit > 110.
-- What we observe it that we moved from 100 -> 110.
-- Then all we need to do is prune results (100, 110] from the existing mv.

-- Q: When do we need to table scan then??
-- A: New result is produced.
-- LHS ordered by join key.
-- We need to scan the LHS table.
-- We scan it to know what we need to emit or retract downstream.

-- Implementation notes for dynamic filter.

-- Q: Why need to align the streams? Merging I understand, but what is the "align" meaning?
-- Q: How do we align the streams?
-- A: Produced by barrier_align? From chandy lamport algorithm read this!!!
-- A: For barrier align it is used by join and dynamic filter.
-- A: It has 2 upstream, every upstream do its own merging.

-- Q: Left is outer side, right is inner side?

-- Q: Flush difference between prev and cur val.
--
-- It has 2 internal state tables, one for LHS, one for RHS.
-- When receive chunk from LHS (outer side), we call `apply_batch`.
-- It will apply the entire LHS chunk to the LHS state table.

-- Table scan happens on barrier.
-- We call `get_range` to get range (what does this do???)
-- Then we table scan LHS with the range.
-- From the docs it seems to suggest `get_range` does 2 things.
-- 1. Checks if latest value (inner side) is lower bound. (unused)
-- 2. Checks if we should insert the new range OR delete.
-- It takes in 2 parameters, current and previous value in the current and prev epoch.
-- Then it checks what is the comparator for the conditional value we use in the dyn filter.
-- From there it gets the range.

-- When we adjust barrier interval higher, every epoch more data -> buffer more -> oom.
-- Kafka also takes memory.

-- So barrier interval can't be very big.

-- Problem is barrier interval can't be too small.
-- Increasing processing speed for barrier.

-- On processing barrier it should not involve I/O as much as possible.

-- For hash agg, it can almost follow this because it can use in-mem cache.

-- Dyn filter involve io op.
-- Dynamic filter always issue scan operation, which can't be handled by bloom filter. Cannot be handled by block cache as well.
-- SuRF is a storage cache proposal, long term project, no real implementation yet.
-- What we need to scan is some rows above NOW().
-- monotonically increasing.
-- We can catch at least 1 row.
-- Catch only 1 row as a MVP.
-- But it can also be time interval.
-- Cache rows around the current row more general solution.
-- Caching ["Top" M rows < prev_outer_value, "Bottom" N rows > prev_outer_value] sounds like a generic approach.
-- For a simple workaround, we can just use M=0 and N=1 for this case.
-- This won't prevent us from further optimization and also don't need to have special logic for timestamp data type.

-- Don't really need LHS table scan, we can just rely on updates to update the cache around NOW().
-- If delete comes, it could delete cache, still needs to read data from storage.

-- 256 * (M + N) M on top, N on bottom. 256 vnodes is default.

-- Use the above as an example:
-- Initial cache is just above MAX (100) -> which is 110.
--
-- LHS + 140.
-- Cache no update, because nearest is still 120.

-- Now barrier comes, and we find diff between prev epoch val (100) and cur epoch val (110).
-- This means we need to scan rows in range (100, 110], to know which to remove.
-- We check our cache:
-- So we know just above 100 is 110.
-- That forms our lower bound 110.
-- Then, for our upper bound it is 110 as well.
-- So it is (110, 110], i.e. nothing at to scan.
-- FALSE. There could be multiple 110 rows.
-- 110
-- 110
-- 110 --> End up we only cached this one.

-- So what is a positive case???
-- Suppose our RHS always increments small, e.g. for NOW() in 1s interval.
-- And on the other hand our LHS increments large.
-- In that case we could have:
-- 100 -> 120 on the RHS.
-- Then, we have 130 in cache, it is largest val > 100.
-- And because 130 in cache on

-- Optimizer limit to Temporal filter (NOW()).

-- Another case, suppose our RHS all less than LHS. In that case, even if RHS bumps up,
-- There's no change to our state, our cache is always none.