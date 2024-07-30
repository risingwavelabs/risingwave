EXEC sys.sp_cdc_enable_db;

CREATE TABLE orders (
  order_id INT PRIMARY KEY,
  order_date BIGINT,
  customer_name NVARCHAR(200),
  price DECIMAL,
  product_id INT,
  order_status SMALLINT
);

EXEC sys.sp_cdc_enable_table
  @source_schema = 'dbo',
  @source_name = 'orders',
  @role_name = NULL;

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

CREATE TABLE sqlserver_all_data_types (
  id INT PRIMARY KEY,
  c_bit bit,
  c_tinyint tinyint,
  c_smallint smallint,
  c_int int,
  c_bigint bigint,
  c_decimal DECIMAL(28),
  c_real real,
  c_float float,
  c_varchar varchar(4),
  c_varbinary varbinary(4),
  c_date date,
  c_time time,
  c_datetime2 datetime2,
  c_datetimeoffset datetimeoffset
);

EXEC sys.sp_cdc_enable_table
  @source_schema = 'dbo',
  @source_name = 'sqlserver_all_data_types',
  @role_name = NULL;

INSERT INTO sqlserver_all_data_types VALUES (1, 'False', 0, 0, 0, 0, 0, 0, 0, '', NULL, '0001-01-01', '00:00:00', '0001-01-01 00:00:00', '0001-01-01 00:00:00');

INSERT INTO sqlserver_all_data_types VALUES (2, 'True', 255, -32768, -2147483648, -9223372036854775808, -10.0, -9999.999999, -10000.0, 'aa', 0xff, '1970-01-01', '13:59:59.1234567', '1000-01-01 11:00:00.1234567', '1970-01-01 00:00:01.1234567');

INSERT INTO sqlserver_all_data_types VALUES (3, 'True', 127, 32767, 2147483647, 9223372036854775807, -10.0, 9999.999999, 10000.0, 'zzzz', 0xffffffff, '9999-12-31', '23:59:59.999999', '9999-12-31 23:59:59.9999999', '9999-12-31 23:59:59.9999999')
