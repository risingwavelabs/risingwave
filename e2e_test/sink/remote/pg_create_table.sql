CREATE TABLE t_remote (
    id BIGINT PRIMARY KEY,
    varchar_column VARCHAR(255),
    text_column TEXT,
    integer_column INTEGER,
    smallint_column SMALLINT,
    bigint_column BIGINT,
    decimal_column DECIMAL(10,2),
    real_column REAL,
    double_column DOUBLE PRECISION,
    boolean_column BOOLEAN,
    date_column DATE,
    time_column TIME,
    timestamp_column TIMESTAMP,
    jsonb_column JSONB,
    bytea_column BYTEA
);