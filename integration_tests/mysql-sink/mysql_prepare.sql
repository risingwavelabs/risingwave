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
    timestamp_column DATETIME,
    timestamptz_column TIMESTAMP,
    jsonb_column JSON,
    bytea_column BLOB
);

CREATE TABLE mysql_all_types (
  id integer PRIMARY KEY,
  c_boolean boolean,
  c_tinyint tinyint,
  c_smallint smallint,
  c_mediumint mediumint,
  c_integer integer,
  c_bigint bigint,
  c_decimal decimal,
  c_float float,
  c_double double,
  c_char_255 char(255),
  c_varchar_10000 varchar(10000),
  c_text text,
  c_blob BLOB,
  c_binary_255 binary(255),
  c_varbinary_10000 varbinary(10000),
  c_date date,
  c_time time(6),
  c_datetime datetime(6),
  c_timestamp timestamp(6),
  c_json JSON,
  c_smallint_array LONGTEXT,
  c_integer_array LONGTEXT,
  c_bigint_array LONGTEXT,
  c_real_array LONGTEXT,
  c_double_precision_array LONGTEXT,
  c_varchar_array LONGTEXT
);
