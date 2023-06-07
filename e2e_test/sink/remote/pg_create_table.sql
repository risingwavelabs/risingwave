CREATE TABLE t_remote_0 (
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

CREATE TABLE t_remote_1 (
    id BIGINT PRIMARY KEY,
    v_varchar VARCHAR(255),
    v_text TEXT,
    v_integer INTEGER,
    v_smallint SMALLINT,
    v_bigint BIGINT,
    v_decimal DECIMAL(10,2),
    v_real REAL,
    v_double DOUBLE PRECISION,
    v_boolean BOOLEAN,
    v_date DATE,
    v_time TIME,
    v_timestamp TIMESTAMP,
    v_timestamptz TIMESTAMPTZ,
    v_interval INTERVAL,
    v_jsonb JSONB,
    v_bytea BYTEA
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