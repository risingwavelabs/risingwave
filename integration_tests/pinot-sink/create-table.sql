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