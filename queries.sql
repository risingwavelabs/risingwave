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
('J', 100),
('K', 110),
('L', 120),
('M', 130);

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

flush;

SELECT * from m1;

INSERT INTO sales
VALUES
(110);

flush;

SELECT * FROM m1;