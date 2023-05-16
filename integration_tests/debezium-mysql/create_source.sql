CREATE TABLE orders (order_id INT PRIMARY KEY) with (
    connector = 'kafka',
    kafka.topic = 'mysql.mydb.orders',
    kafka.brokers = 'message_queue:29092',
    kafka.scan.startup.mode = 'earliest'
) ROW FORMAT DEBEZIUM_AVRO ROW SCHEMA LOCATION CONFLUENT SCHEMA REGISTRY 'http://message_queue:8081';

--- Testing --- 
CREATE TABLE data_types (id INT PRIMARY KEY) with (
    connector = 'kafka',
    kafka.topic = 'mysql.mydb.data_types',
    kafka.brokers = 'message_queue:29092',
    kafka.scan.startup.mode = 'earliest'
) ROW FORMAT DEBEZIUM_AVRO ROW SCHEMA LOCATION CONFLUENT SCHEMA REGISTRY 'http://message_queue:8081';