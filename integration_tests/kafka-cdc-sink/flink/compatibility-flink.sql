CREATE TABLE topic_types (
    id INT,
    c_boolean BOOLEAN,
    c_smallint SMALLINT,
    c_integer INT,
    c_bigint BIGINT,
    c_decimal decimal,
    c_real FLOAT,
    c_double_precision DOUBLE,
    c_varchar VARCHAR,
    c_bytea BYTES
) WITH (
    'connector' = 'kafka',
    'topic' = 'flinktypes',
    'properties.bootstrap.servers' = 'message_queue:29092',
    'properties.group.id' = 'test-flink-client-1',
    'scan.startup.mode' = 'earliest-offset',
    'format' = 'debezium-json',
    'debezium-json.schema-include' = 'true'
);

CREATE TABLE pg_types (
    id INT,
    c_boolean BOOLEAN,
    c_smallint SMALLINT,
    c_integer INT,
    c_bigint BIGINT,
    c_decimal decimal,
    c_real FLOAT,
    c_double_precision DOUBLE,
    c_varchar VARCHAR,
    c_bytea BYTES,
    PRIMARY KEY (`id`) NOT ENFORCED
) WITH (
    'connector' = 'jdbc',
    'url' = 'jdbc:postgresql://postgres:5432/mydb?user=myuser&password=123456',
    'table-name' = 'flink_types'
);

INSERT INTO pg_types
SELECT * FROM topic_types;
