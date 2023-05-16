-- mysql -p123456 -uroot -h 127.0.0.1 mydb < mysql_prepare.sql
--
-- Mysql
USE mydb;

DROP TABLE IF EXISTS orders;

create table orders (
  order_id int,
  order_date bigint,
  customer_name varchar(200),
  price decimal,
  product_id int,
  order_status smallint,
  PRIMARY KEY (order_id)
);

insert into
  orders
values
  (1, 1558430840000, 'Bob', 10.50, 1, 1),
  (2, 1558430840001, 'Alice', 20.50, 2, 1),
  (
    3,
    1558430840002,
    'Alice',
    18.50,
    2,
    1
  );

DROP TABLE IF EXISTS data_types;

CREATE TABLE data_types (
  id INT AUTO_INCREMENT PRIMARY KEY,
  varchar_column VARCHAR(255),
  char_column CHAR(10),
  text_column TEXT,
  integer_column INT,
  tinyint_column TINYINT,
  smallint_column SMALLINT,
  mediumint_column MEDIUMINT,
  bigint_column BIGINT,
  float_column FLOAT,
  double_column DOUBLE,
  decimal_column DECIMAL(10, 2),
  date_column DATE,
  time_column TIME,
  datetime_column DATETIME,
  timestamp_column TIMESTAMP,
  year_column YEAR,
  binary_column BINARY(10),
  varbinary_column VARBINARY(255),
  blob_column BLOB,
  tinyblob_column TINYBLOB,
  mediumblob_column MEDIUMBLOB,
  longblob_column LONGBLOB,
  enum_column ENUM('Value1', 'Value2', 'Value3'),
  set_column
  SET
    ('Option1', 'Option2', 'Option3'),
    boolean_column BOOLEAN,
    json_column JSON
);

INSERT INTO
  data_types (
    varchar_column,
    char_column,
    text_column,
    integer_column,
    tinyint_column,
    smallint_column,
    mediumint_column,
    bigint_column,
    float_column,
    double_column,
    decimal_column,
    date_column,
    time_column,
    datetime_column,
    timestamp_column,
    year_column,
    binary_column,
    varbinary_column,
    blob_column,
    tinyblob_column,
    mediumblob_column,
    longblob_column,
    enum_column,
    set_column,
    boolean_column,
    json_column
  )
VALUES
  (
    'Hello',
    'World',
    'This is a text',
    123,
    1,
    10,
    100,
    1000,
    1.23,
    1.23456789,
    123.45,
    '2023-05-16',
    '10:30:45',
    '2023-05-16 10:30:45',
    '2023-05-16 10:30:45',
    '2023',
    '0101010101',
    '1010101010',
    'This is a blob',
    'Tiny Blob',
    'Medium Blob',
    'Long Blob',
    'Value1',
    'Option1,Option3',
    1,
    '{"key": "value"}'
  ),
  (
    'Goodbye',
    'MySQL',
    'Another text',
    -456,
    0,
    -20,
    -200,
    -2000,
    -3.14,
    -3.14159265359,
    -123.45,
    '2023-05-17',
    '22:15:30',
    '2023-05-17 22:15:30',
    '2023-05-17 22:15:30',
    '2023',
    '0011001100',
    '1100110011',
    'Another blob',
    'Small Blob',
    'Medium Blob 2',
    'Long Blob 2',
    'Value2',
    'Option2',
    0,
    '{"key2": "value2"}'
  ),
  (
    'OpenAI',
    'AI',
    'Lorem ipsum dolor sit amet',
    789,
    1,
    50,
    500,
    5000,
    2.71828,
    3.14159265359,
    987.65,
    '2023-05-18',
    '08:45:15',
    '2023-05-18 08:45:15',
    '2023-05-18 08:45:15',
    '2023',
    '1110000000',
    '0001110000',
    'Lorem ipsum blob',
    'Medium Blob',
    'Large Blob',
    'Largest Blob',
    'Value3',
    'Option1,Option2,Option3',
    1,
    '{"key3": "value3"}'
  );