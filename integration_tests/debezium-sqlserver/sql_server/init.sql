SELECT
  name
FROM
  sys.databases;

USE mydb;

EXEC sys.sp_cdc_enable_db;

CREATE TABLE orders (
  order_id INT PRIMARY KEY,
  order_date BIGINT,
  customer_name NVARCHAR(200),
  price DECIMAL,
  product_id INT,
  order_status SMALLINT
);

-- Insert data into the orders table
INSERT INTO
  orders (
    order_id,
    order_date,
    customer_name,
    price,
    product_id,
    order_status
  )
VALUES
  (1, 1558430840000, 'Bob', 10.50, 1, 1),
  (2, 1558430840001, 'Alice', 20.50, 2, 1),
  (3, 1558430840002, 'Alice', 18.50, 2, 1);