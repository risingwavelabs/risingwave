CREATE SOURCE user_behaviors (
    user_id VARCHAR,
    target_id VARCHAR,
    target_type VARCHAR,
    event_timestamp TIMESTAMPTZ,
    behavior_type VARCHAR,
    parent_target_type VARCHAR,
    parent_target_id VARCHAR
) WITH (
    connector = 'kafka',
    topic = 'user_behaviors',
    properties.bootstrap.server = 'message_queue:29092',
    scan.startup.mode = 'earliest'
) ROW FORMAT JSON;

CREATE TABLE data_types (
    id BIGINT PRIMARY KEY,
    varchar_column VARCHAR,
    text_column TEXT,
    integer_column INTEGER,
    smallint_column SMALLINT,
    bigint_column BIGINT,
    decimal_column DECIMAL,
    real_column REAL,
    double_column DOUBLE PRECISION,
    boolean_column BOOLEAN,
    date_column DATE,
    time_column TIME,
    timestamp_column TIMESTAMP,
    jsonb_column JSONB,
    bytea_column BYTEA
);

CREATE SINK data_types_mysql_sink
FROM
    target_count WITH (
        connector = 'jdbc',
        jdbc.url = 'jdbc:mysql://mysql:3306/mydb?user=root&password=123456',
        table.name = 'data_types',
        type = 'upsert'
    );

INSERT INTO data_types (id, varchar_column, text_column, integer_column, smallint_column, bigint_column, decimal_column, real_column, double_column, boolean_column, date_column, time_column, timestamp_column, jsonb_column, bytea_column)
VALUES
    (1, 'Varchar value 1', 'Text value 1', 123, 456, 789, 12.34, 56.78, 90.12, TRUE, '2023-05-22', '12:34:56', '2023-05-22 12:34:56', '{"key": "value"}', E'\\xDEADBEEF'),
    (2, 'Varchar value 2', 'Text value 2', 234, 567, 890, 23.45, 67.89, 01.23, FALSE, '2023-05-23', '23:45:01', '2023-05-23 23:45:01', '{"key": "value2"}', E'\\xFEEDBEEF'),
    (3, 'Varchar value 3', 'Text value 3', 345, 678, 901, 34.56, 78.90, 12.34, TRUE, '2023-05-24', '12:34:56', '2023-05-24 12:34:56', '{"key": "value3"}', E'\\xCAFEBABE'),
    (4, 'Varchar value 4', 'Text value 4', 456, 789, 012, 45.67, 89.01, 23.45, FALSE, '2023-05-25', '23:45:01', '2023-05-25 23:45:01', '{"key": "value4"}', E'\\xBABEC0DE'),
    (5, 'Varchar value 5', 'Text value 5', 567, 890, 123, 56.78, 90.12, 34.56, TRUE, '2023-05-26', '12:34:56', '2023-05-26 12:34:56', '{"key": "value5"}', E'\\xDEADBABE');
