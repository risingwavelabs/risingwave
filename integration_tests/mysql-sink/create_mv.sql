CREATE MATERIALIZED VIEW target_count AS
SELECT
    target_id,
    COUNT(*) AS target_count
FROM
    user_behaviors
GROUP BY
    target_id;

CREATE SINK target_count_mysql_sink
FROM
    target_count WITH (
        connector = 'jdbc',
        jdbc.url = 'jdbc:mysql://mysql:3306/mydb?user=root&password=123456',
        table.name = 'target_count',
        type = 'upsert'
    );

-- ingest the table back to RW
CREATE TABLE rw_types (
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
) WITH (
    connector = 'mysql-cdc',
    hostname = 'mysql',
    port = '3306',
    username = 'root',
    password = '123456',
    database.name = 'mydb',
    table.name = 'data_types',
    server.id = '3'
);
