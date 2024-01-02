CREATE TABLE IF NOT EXISTS orders
(
    id INT PRIMARY KEY,
    user_id BIGINT,
    product_id BIGINT,
    status VARCHAR,
    quantity INT,
    total FLOAT,
    created_at BIGINT,
    updated_at BIGINT
);

insert into orders values (1, 10, 100, 'INIT', 1, 1.0, 1685421033000, 1685421033000);
insert into orders values (2, 10, 100, 'INIT', 1, 1.0, 1685421033000, 1685421033000);
insert into orders values (3, 10, 100, 'INIT', 1, 1.0, 1685421033000, 1685421033000);
