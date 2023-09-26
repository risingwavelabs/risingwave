insert into t1
SELECT
    generate_series,
    '{"orders": {"id": 1, "price": "2.30", "customer_id": 2}}'::jsonb
FROM generate_series(1, 100000);
FLUSH;