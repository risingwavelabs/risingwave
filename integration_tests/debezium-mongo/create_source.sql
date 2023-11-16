CREATE TABLE users (_id JSONB PRIMARY KEY, payload JSONB) WITH (
    connector = 'kafka',
    kafka.topic = 'dbserver1.random_data.users',
    kafka.brokers = 'message_queue:29092',
    kafka.scan.startup.mode = 'earliest'
) FORMAT DEBEZIUM_MONGO ENCODE JSON;