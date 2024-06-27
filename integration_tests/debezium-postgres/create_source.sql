CREATE TABLE orders (PRIMARY KEY(order_id)) with (
    connector = 'kafka',
    kafka.topic = 'pg.public.orders',
    kafka.brokers = 'message_queue:29092',
    kafka.scan.startup.mode = 'earliest'
) FORMAT DEBEZIUM ENCODE AVRO (schema.registry = 'http://message_queue:8081');

CREATE TABLE pg_all_data_types (PRIMARY KEY(id)) with (
    connector = 'kafka',
    kafka.topic = 'pg.public.pg_all_data_types',
    kafka.brokers = 'message_queue:29092',
    kafka.scan.startup.mode = 'earliest'
) FORMAT DEBEZIUM ENCODE AVRO (schema.registry = 'http://message_queue:8081');

CREATE TABLE pg_types2 (PRIMARY KEY(id)) with (
    connector = 'kafka',
    kafka.topic = 'pg.public.pg_types2',
    kafka.brokers = 'message_queue:29092',
    kafka.scan.startup.mode = 'earliest'
) FORMAT DEBEZIUM ENCODE AVRO (schema.registry = 'http://message_queue:8081');

CREATE TABLE pg_types3 (PRIMARY KEY(id)) with (
    connector = 'kafka',
    kafka.topic = 'pg.public.pg_types3',
    kafka.brokers = 'message_queue:29092',
    kafka.scan.startup.mode = 'earliest'
) FORMAT DEBEZIUM ENCODE AVRO (schema.registry = 'http://message_queue:8081');
