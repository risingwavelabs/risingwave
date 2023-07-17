CREATE TABLE orders (PRIMARY KEY(order_id)) with (
    connector = 'kafka',
    kafka.topic = 'mysql.mydb.orders',
    kafka.brokers = 'message_queue:29092',
    kafka.scan.startup.mode = 'earliest'
) FORMAT DEBEZIUM ENCODE AVRO (schema.registry = 'http://message_queue:8081');