-- mysql -p123456 -uroot -h 127.0.0.1 mydb < mysql_prepare.sql
--
-- Mysql
USE mydb;

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