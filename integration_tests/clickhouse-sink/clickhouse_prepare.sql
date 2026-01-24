SET enable_json_type = 1;

CREATE table demo_test(
    user_id Int32,
    target_id String,
    event_timestamp DateTime64,
)ENGINE = ReplacingMergeTree
PRIMARY KEY (user_id);

CREATE table ck_types (
  types_id Int32,
  c_boolean Bool,
  c_smallint Int16,
  c_integer Int32,
  c_bigint Int64,
  c_decimal Decimal(28, 28),
  c_real Float32,
  c_double_precision Float64,
  c_varchar String,
  c_date Date32,
  c_timestamptz DateTime64(6),
  c_boolean_array Array(Bool),
  c_smallint_array Array(Int16),
  c_integer_array Array(Int32),
  c_bigint_array Array(Int64),
  c_decimal_array Array(Decimal(28, 28)),
  c_real_array Array(Float32),
  c_double_precision_array Array(Float64),
  c_varchar_array Array(String),
  c_date_array Array(Date32),
  c_timestamptz_array Array(DateTime64(6)),
  c_struct Nested(s_int Int32, s_boolean Bool),
  c_jsonb: Json,
)ENGINE = ReplacingMergeTree
PRIMARY KEY (types_id);

CREATE TABLE demo_test_null(
    user_id Int32,
    target_id String,
    event_timestamp DateTime64,
) ENGINE = Null;

CREATE TABLE demo_test_target_null(
    user_id Int32,
    target_id String,
    event_timestamp DateTime64
) ENGINE = MergeTree()
ORDER BY (user_id, event_timestamp);

CREATE MATERIALIZED VIEW demo_mv_null TO demo_test_target_null AS
SELECT
    user_id,
    target_id,
    event_timestamp
FROM demo_test_null;
