CREATE TABLE sales (profit_margin int);
CREATE TABLE products (product_name varchar primary key, product_profit int);
WITH max_profit AS (SELECT max(profit_margin) max FROM sales) 
SELECT product_name FROM products, max_profit 
WHERE product_profit > max;
