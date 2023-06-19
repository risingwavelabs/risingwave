CREATE TABLE target_count (
  target_id VARCHAR(128) primary key,
  target_count BIGINT
);


-- sink table
CREATE TABLE data_types (
    id BIGINT PRIMARY KEY,
    varchar_column VARCHAR(255),
    text_column TEXT,
    integer_column INT,
    smallint_column SMALLINT,
    bigint_column BIGINT,
    decimal_column DECIMAL(10,2),
    real_column FLOAT,
    double_column DOUBLE,
    boolean_column BOOLEAN,
    date_column DATE,
    time_column TIME,
    timestamp_column TIMESTAMP,
    jsonb_column JSON,
    bytea_column BLOB
);