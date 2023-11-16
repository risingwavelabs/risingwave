CREATE TABLE orders (PRIMARY KEY(order_id)) with (
    connector = 'kafka',
    kafka.topic = 'postgres.public.orders',
    kafka.brokers = 'message_queue:29092',
    kafka.scan.startup.mode = 'earliest'
) FORMAT DEBEZIUM ENCODE AVRO (schema.registry = 'http://message_queue:8081');