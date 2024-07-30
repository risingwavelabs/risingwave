CREATE TABLE orders (PRIMARY KEY(order_id)) with (
    connector = 'kafka',
    kafka.topic = 'sqlserver.mydb.dbo.orders',
    kafka.brokers = 'message_queue:29092',
    kafka.scan.startup.mode = 'earliest'
) FORMAT DEBEZIUM ENCODE AVRO (schema.registry = 'http://message_queue:8081');

CREATE TABLE sqlserver_all_data_types (PRIMARY KEY(id)) with (
    connector = 'kafka',
    kafka.topic = 'sqlserver.mydb.dbo.sqlserver_all_data_types',
    kafka.brokers = 'message_queue:29092',
    kafka.scan.startup.mode = 'earliest'
) FORMAT DEBEZIUM ENCODE AVRO (schema.registry = 'http://message_queue:8081');
