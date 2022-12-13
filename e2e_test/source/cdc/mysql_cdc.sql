DROP DATABASE IF EXISTS mydb;
CREATE DATABASE mydb;

USE mydb;

CREATE TABLE products (
    id INTEGER NOT NULL AUTO_INCREMENT PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    description VARCHAR(512)
);

ALTER TABLE products AUTO_INCREMENT = 101;

INSERT INTO products
VALUES (default,"scooter","Small 2-wheel scooter"),
       (default,"car battery","12V car battery"),
       (default,"12-pack drill bits","12-pack of drill bits with sizes ranging from #40 to #3"),
       (default,"hammer","12oz carpenter's hammer"),
       (default,"hammer","14oz carpenter's hammer"),
       (default,"hammer","16oz carpenter's hammer"),
       (default,"rocks","box of assorted rocks"),
       (default,"jacket","water resistent black wind breaker"),
       (default,"spare tire","24 inch spare tire");


CREATE TABLE orders (
    order_id INTEGER NOT NULL AUTO_INCREMENT PRIMARY KEY,
    order_date DATETIME NOT NULL,
    customer_name VARCHAR(255) NOT NULL,
    price DECIMAL(10, 5) NOT NULL,
    product_id INTEGER NOT NULL,
    order_status BOOLEAN NOT NULL -- Whether order has been placed
) AUTO_INCREMENT = 10001;

INSERT INTO orders
VALUES (default, '2020-07-30 10:08:22', 'Jark', 50.50, 102, false),
       (default, '2020-07-30 10:11:09', 'Sally', 15.00, 105, false),
       (default, '2020-07-30 12:00:30', 'Edward', 25.25, 106, false);

CREATE TABLE mytable (
    v1 INTEGER NOT NULL PRIMARY KEY,
    v2 INTEGER NOT NULL,
    v3 VARCHAR(255) NOT NULL
);

INSERT INTO mytable
VALUES (1,1,'no'),
       (2,2,'no'),
       (3,3,'no'),
       (4,4,'no');
