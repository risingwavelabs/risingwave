CREATE TABLE orders (order_id INT PRIMARY KEY) with (
    connector = 'kafka',
    kafka.topic = 'mysql.mydb.orders',
    kafka.brokers = 'message_queue:29092',
    kafka.scan.startup.mode = 'earliest'
) ROW FORMAT DEBEZIUM_AVRO ROW SCHEMA LOCATION CONFLUENT SCHEMA REGISTRY 'http://message_queue:8081';