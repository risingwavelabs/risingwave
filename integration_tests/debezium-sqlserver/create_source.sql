CREATE TABLE orders (PRIMARY KEY(order_id)) with (
    connector = 'kafka',
    kafka.topic = 'sqlserver.dbo.orders',
    kafka.brokers = 'message_queue:29092',
    kafka.scan.startup.mode = 'earliest'
) ROW FORMAT DEBEZIUM_AVRO (schema.registry = 'http://message_queue:8081');