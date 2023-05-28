CREATE TABLE t_remote (
    id integer PRIMARY KEY,
    v_varchar varchar(100),
    v_smallint smallint,
    v_integer integer,
    v_bigint bigint,
    v_decimal decimal,
    v_float real,
    v_double double precision,
    v_timestamp timestamp
);

CREATE TABLE t_types (
    id BIGINT PRIMARY KEY,
    varchar_column VARCHAR(100),
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
    interval_column INTERVAL,
    jsonb_column JSONB,
    array_column VARCHAR[],
    array_column2 DECIMAL[]
);