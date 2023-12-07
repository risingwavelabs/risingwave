CREATE TABLE target_count (
  target_id VARCHAR(128) primary key,
  target_count BIGINT
);

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
    timestamptz_column TIMESTAMPTZ,
    interval_column INTERVAL,
    jsonb_column JSONB,
    bytea_column BYTEA,
    array_column VARCHAR[]
);