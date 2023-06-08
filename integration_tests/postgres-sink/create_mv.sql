CREATE MATERIALIZED VIEW target_count AS
SELECT
    target_id,
    COUNT(*) AS target_count
FROM
    user_behaviors
GROUP BY
    target_id;

CREATE SINK target_count_postgres_sink
FROM
    target_count WITH (
        connector = 'jdbc',
        jdbc.url = 'jdbc:postgresql://postgres:5432/mydb?user=myuser&password=123456',
        table.name = 'target_count',
        type = 'upsert'
    );

-- ingest back to RW
CREATE table rw_types (
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
    timestamptz_column TIMESTAMPTZ,
    interval_column INTERVAL,
    jsonb_column JSONB,
    bytea_column BYTEA,
    array_column VARCHAR[]
) WITH (
      connector = 'postgres-cdc',
      hostname = 'postgres',
      port = '5432',
      username = 'myuser',
      password = '123456',
      database.name = 'mydb',
      schema.name = 'public',
      table.name = 'data_types',
      slot.name = 'data_types'
);
