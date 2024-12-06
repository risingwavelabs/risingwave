CREATE TABLE products (
    id INTEGER NOT NULL AUTO_INCREMENT PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    description VARCHAR(512)
) AUTO_INCREMENT = 101;

CREATE TABLE orders (
    order_id INTEGER NOT NULL AUTO_INCREMENT PRIMARY KEY,
    order_date DATETIME NOT NULL,
    customer_name VARCHAR(255) NOT NULL,
    price DECIMAL(10, 5) NOT NULL,
    product_id INTEGER NOT NULL,
    order_status BOOLEAN NOT NULL -- Whether order has been placed
) AUTO_INCREMENT = 10001;

CREATE TABLE mytable (
    v1 INTEGER NOT NULL PRIMARY KEY,
    v2 INTEGER NOT NULL,
    v3 VARCHAR(255) NOT NULL
);

DROP USER IF EXISTS 'dbz'@'%';
CREATE USER 'dbz'@'%' IDENTIFIED BY '123456';
GRANT SELECT, RELOAD, SHOW DATABASES, REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO 'dbz'@'%';

CREATE TABLE tt3 (v1 int primary key, v2 timestamp);

CREATE TABLE IF NOT EXISTS mysql_all_types(
    c_boolean boolean,
    c_bit bit,
    c_tinyint tinyint,
    c_smallint smallint,
    c_mediumint mediumint,
    c_integer integer,
    c_bigint bigint,
    c_decimal decimal,
    c_float float,
    c_double double,
    c_char_255 char(255),
    c_varchar_10000 varchar(10000),
    c_binary_255 binary(255),
    c_varbinary_10000 varbinary(10000),
    c_date date,
    c_time time,
    c_datetime datetime,
    c_timestamp timestamp,
    PRIMARY KEY (c_boolean,c_bigint,c_date)
);

INSERT INTO mysql_all_types VALUES ( False, 0, null, null, -8388608, -2147483647, 9223372036854775806, -10.0, -9999.999999, -10000.0, 'c', 'd', '', '', '1001-01-01', '-838:59:59.000000', '2000-01-01 00:00:00.000000', null);
INSERT INTO mysql_all_types VALUES ( True, 1, -128, -32767, -8388608, -2147483647, -9223372036854775807, -10.0, -9999.999999, -10000.0, 'a', 'b', '', '', '1001-01-01', '00:00:00', '1998-01-01 00:00:00.000000', '1970-01-01 00:00:01');

CREATE TABLE test_my_default_value (
    id int,
    name varchar(64),
    city varchar(200) default 'Shanghai',
    PRIMARY KEY (id)
);
