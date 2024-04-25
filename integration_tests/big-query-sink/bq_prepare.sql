DROP TABLE IF EXISTS `rwctest.bqtest.bq_sink`;
CREATE TABLE `rwctest.bqtest.bq_sink` (
    user_id INT64,
    target_id STRING,
    event_timestamp TIMESTAMP
);

DROP TABLE IF EXISTS `rwctest.bqtest.bq_sink_data_types`;
CREATE TABLE `rwctest.bqtest.bq_sink_data_types` (
    types_id INT64,
    c_boolean BOOL,
    c_smallint INT64,
    c_integer INT64,
    c_bigint INT64,
    c_decimal NUMERIC,
    c_double_precision FLOAT64,
    c_varchar STRING,
    c_bytea BYTES,
    c_date DATE,
    c_time TIME,
    c_timestamp DATETIME,
    c_timestamptz TIMESTAMP,
    c_interval INTERVAL,
    c_jsonb STRING,
    c_boolean_array ARRAY<BOOL>,
    c_smallint_array ARRAY<INT64>,
    c_integer_array ARRAY<INT64>,
    c_bigint_array ARRAY<INT64>,
    c_decimal_array ARRAY<NUMERIC>,
    c_double_precision_array ARRAY<FLOAT64>,
    c_varchar_array ARRAY<STRING>,
    c_bytea_array ARRAY<BYTES>,
    c_date_array ARRAY<DATE>,
    c_time_array ARRAY<TIME>,
    c_timestamp_array ARRAY<DATETIME>,
    c_timestamptz_array ARRAY<TIMESTAMP>,
    c_interval_array ARRAY<INTERVAL>,
    c_jsonb_array ARRAY<STRING>,
    c_struct STRUCT<s_int INT64, s_bool BOOL>
);
