DROP TABLE IF EXISTS orders;

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

DROP TABLE IF EXISTS data_types;

---
--- Testing, Not for Documentation --- 
---
CREATE TABLE data_types (
  id SERIAL PRIMARY KEY,
  varchar_column VARCHAR(255),
  char_column CHAR(10),
  text_column TEXT,
  integer_column INTEGER,
  smallint_column SMALLINT,
  bigint_column BIGINT,
  decimal_column DECIMAL(10, 2),
  numeric_column NUMERIC(8, 4),
  real_column REAL,
  double_column DOUBLE PRECISION,
  boolean_column BOOLEAN,
  date_column DATE,
  time_column TIME,
  timestamp_column TIMESTAMP,
  interval_column INTERVAL,
  uuid_column UUID,
  json_column JSON,
  jsonb_column JSONB,
  bytea_column BYTEA
);

INSERT INTO
  data_types (
    varchar_column,
    char_column,
    text_column,
    integer_column,
    smallint_column,
    bigint_column,
    decimal_column,
    numeric_column,
    real_column,
    double_column,
    boolean_column,
    date_column,
    time_column,
    timestamp_column,
    interval_column,
    uuid_column,
    json_column,
    jsonb_column,
    bytea_column
  )
VALUES
  (
    'John Doe',
    'ABC',
    'This is a sample text.',
    42,
    10,
    1234567890,
    1234.56,
    3.1416,
    3.14,
    2.718,
    true,
    '2023-05-16',
    '15:30:00',
    '2023-05-16 15:30:00',
    '2 days',
    '123e4567-e89b-12d3-a456-426614174000',
    '{"name": "John", "age": 30}',
    '{"name": "Doe", "age": 35}',
    E '\\x0123456789ABCDEF'
  ),
  (
    'Jane Smith',
    'XYZ',
    'Another sample text.',
    -10,
    -5,
    -9876543210,
    -5678.91,
    -1.2345,
    -2.718,
    -3.14,
    false,
    '2023-05-17',
    '09:45:00',
    '2023-05-17 09:45:00',
    '1 hour',
    '98765432-21dc-43f1-ba78-798456321000',
    '{"name": "Jane", "age": 25}',
    '{"name": "Smith", "age": 28}',
    E '\\x00FEEDFACEC0FFEE'
  ),
  (
    'Alice Johnson',
    'PQR',
    'Yet another sample text.',
    0,
    0,
    0,
    0.00,
    0.0000,
    0.0,
    0.0,
    true,
    '2023-05-18',
    '18:00:00',
    '2023-05-18 18:00:00',
    '1 week',
    '11111111-2222-3333-4444-555555555555',
    '{"name": "Alice", "age": 40}',
    '{"name": "Johnson", "age": 45}',
    E '\\x'
  ),
  (
    'Bob Williams',
    'LMN',
    'Sample text for Bob.',
    100,
    50,
    9876543210,
    9876.54,
    9.8765,
    8.765,
    7.654,
    false,
    '2023-05-19',
    '12:00:00',
    '2023-05-19 12:00:00',
    '3 hours',
    '22222222-1111-3333-4444-666666666666',
    '{"name": "Bob", "age": 35}',
    '{"name": "Williams", "age": 38}',
    E '\\x11223344556677889900'
  ),
  (
    'Eve Brown',
    'DEF',
    'Text sample for Eve.',
    999,
    333,
    1234567890123456789,
    98765.43,
    123.4567,
    456.789,
    789.123,
    true,
    '2023-05-20',
    '21:30:00',
    '2023-05-20 21:30:00',
    '5 minutes',
    '33333333-4444-5555-6666-777777777777',
    '{"name": "Eve", "age":29}',
    '{"name": "Brown", "age": 32}',
    E '\x5566778899AABBCCDDEE'
  ),
  (
    'Michael Davis',
    'JKL',
    'Sample text for Michael.',
    -500,
    -250,
    -1234567890123456789,
    -5432.10,
    -123.4567,
    -456.789,
    -789.123,
    false,
    '2023-05-21',
    '08:15:00',
    '2023-05-21 08:15:00',
    '1 day',
    '44444444-5555-6666-7777-888888888888',
    '{"name": "Michael", "age": 45}',
    '{"name": "Davis", "age": 42}',
    E '\xCAFEBABEDEADBEEF'
  );