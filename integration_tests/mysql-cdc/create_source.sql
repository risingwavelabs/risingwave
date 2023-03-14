create table orders (
    order_id int,
    order_date bigint,
    customer_name varchar,
    price decimal,
    product_id int,
    order_status smallint,
    PRIMARY KEY (order_id)
) with (
    connector = 'mysql-cdc',
    hostname = 'mysql',
    port = '3306',
    username = 'root',
    password = '123456',
    database.name = 'mydb',
    table.name = 'orders',
    server.id = '1'
);