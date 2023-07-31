ALTER TABLE orders REPLICA IDENTITY FULL;

SELECT create_distributed_table('orders', 'o_orderkey');
