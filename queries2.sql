-- Understand dynamic filter.

-- Scenario: Product profit targets
-- Max is the dynamic filter here.
-- It will change as more sales are added.
CREATE TABLE sales (id int, price int, time timestamp);

EXPLAIN CREATE MATERIALIZED VIEW m1 AS
    SELECT id, price
    FROM sales
    WHERE time > NOW();

CREATE MATERIALIZED VIEW m1 AS
    SELECT id, price
    FROM sales
    WHERE time > NOW();

INSERT INTO sales
VALUES
    (1, 300, TIMESTAMP '2023-01-01 01:00:00'),
    (2, 400, TIMESTAMP '2023-01-01 02:00:00'),
    (3, 500, TIMESTAMP '2023-01-01 03:00:00'),
    (4, 600, TIMESTAMP '2023-01-01 04:00:00'),
    (5, 700, TIMESTAMP '2023-01-01 05:00:00'),
    (6, 800, TIMESTAMP '2023-01-01 06:00:00'),
    (7, 900, TIMESTAMP '2023-01-01 07:00:00'),
    (8, 100, TIMESTAMP '2023-01-01 08:00:00'),
    (9, 110, TIMESTAMP '2023-01-01 09:00:00'),
    (10, 120, TIMESTAMP '2023-01-01 10:00:00');

SELECT * FROM m1;