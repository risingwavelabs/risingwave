-- Populate supplier (insert 10,000 rows)
INSERT INTO supplier SELECT
    key AS s_suppkey,
    'Supplier#000000001' AS s_name,
    '123456' AS s_address,
    key AS s_nationkey,
    '8888888' AS s_phone,
    1000 AS s_acctbal,
    'Lorem ipsum dolor sit amet, consectetur adipiscing elit.' AS s_comment
    FROM generate_series(1, 10) t(key);


flush;