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

CREATE TABLE cock_all_data_types (
  id BIGINT PRIMARY KEY,
  c_boolean BOOL,
  c_smallint INT2,
  c_integer INT4,
  c_bigint INT,
  c_decimal decimal,
  c_real real,
  c_double_precision double precision,
  c_varchar STRING,
  c_bytea BYTES,
  c_date date,
  c_time time,
  c_timestamp timestamp,
  c_timestamptz timestamptz,
  c_interval interval,
  c_jsonb jsonb,
  c_smallint_array INT2[],
  c_integer_array INT4[],
  c_bigint_array INT[],
  c_real_array real[],
  c_double_precision_array double precision[],
  c_varchar_array STRING[]
);
