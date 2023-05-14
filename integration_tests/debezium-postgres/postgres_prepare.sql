-- Create the orders table
CREATE TABLE orders (
  order_id SERIAL PRIMARY KEY,
  order_date BIGINT,
  customer_name VARCHAR(200),
  price DECIMAL,
  product_id INT,
  order_status SMALLINT
);

-- FULL - Emitted events for UPDATE and DELETE operations contain the previous values of all columns in the table.
-- Refer to https://debezium.io/documentation/reference/stable/connectors/postgresql.html#postgresql-replica-identity
ALTER TABLE
  orders REPLICA IDENTITY FULL;

-- -- https://www.postgresql.org/docs/15/logical-replication-publication.html
-- CREATE PUBLICATION mz_source FOR TABLE orders;

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