create table delta.`s3a://deltalake/delta`(id int, name string) using delta;

create table delta.`s3a://deltalake/data_types`(
  types_id INT,
  c_boolean boolean,
  c_smallint short,
  c_integer integer,
  c_bigint long,
  c_decimal decimal(28),
  c_real float,
  c_double_precision double,
  c_varchar string,
  c_date date,
  c_timestamptz timestamp,
  c_boolean_array ARRAY<boolean>,
  c_smallint_array ARRAY<short>,
  c_integer_array ARRAY<integer>,
  c_bigint_array ARRAY<long>,
  c_decimal_array ARRAY<decimal(28)>,
  c_real_array ARRAY<float>,
  c_double_precision_array ARRAY<double>,
  c_varchar_array ARRAY<string>,
  c_date_array ARRAY<date>,
  c_timestamptz_array ARRAY<timestamp>,
  c_struct STRUCT<s_int:INTEGER, s_varchar:string>
) using delta;
