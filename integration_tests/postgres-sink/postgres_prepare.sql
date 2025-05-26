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

CREATE TABLE pg_all_data_types (
  id BIGINT PRIMARY KEY,
  c_boolean boolean,
  c_smallint smallint,
  c_integer integer,
  c_bigint bigint,
  c_decimal decimal,
  c_real real,
  c_double_precision double precision,
  c_varchar varchar,
  c_bytea bytea,
  c_date date,
  c_time time,
  c_timestamp timestamp,
  c_timestamptz timestamptz,
  c_interval interval,
  c_jsonb jsonb,
  c_smallint_array smallint[],
  c_integer_array integer[],
  c_bigint_array bigint[],
  c_real_array real[],
  c_double_precision_array double precision[],
  c_varchar_array varchar[]
);

CREATE TABLE numeric256_types (
    id BIGINT PRIMARY KEY,
    int256_col NUMERIC(78, 0),
    uint256_col NUMERIC(78, 0),
    description VARCHAR
);
