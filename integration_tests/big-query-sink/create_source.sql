CREATE table user_behaviors (
    user_id int,
    target_id VARCHAR,
    target_type VARCHAR,
    event_timestamp TIMESTAMPTZ,
    behavior_type VARCHAR,
    parent_target_type VARCHAR,
    parent_target_id VARCHAR,
    PRIMARY KEY(user_id)
) WITH (
    connector = 'datagen',
    fields.user_id.kind = 'sequence',
    fields.user_id.start = '1',
    fields.user_id.end = '100',
    datagen.rows.per.second = '10'
) FORMAT PLAIN ENCODE JSON;

CREATE TABLE bq_sink_data_types (
    types_id int,
    c_boolean boolean,
    c_smallint smallint,
    c_integer integer,
    c_bigint bigint,
    c_decimal decimal,
    c_double_precision double precision,
    c_varchar varchar,
    c_bytea bytea,
    c_date date,
    c_time time,
    c_timestamp timestamp,
    c_timestamptz timestamptz,
    c_interval interval,
    c_jsonb jsonb,
    c_boolean_array boolean[],
    c_smallint_array smallint[],
    c_integer_array integer[],
    c_bigint_array bigint[],
    c_decimal_array decimal[],
    c_double_precision_array double precision[],
    c_varchar_array varchar[],
    c_bytea_array bytea[],
    c_date_array date[],
    c_time_array time[],
    c_timestamp_array timestamp[],
    c_timestamptz_array timestamptz[],
    c_interval_array interval[],
    c_jsonb_array jsonb[],
    c_struct STRUCT<s_int INTEGER, s_bool boolean>,
    PRIMARY KEY(types_id)
);

INSERT INTO bq_sink_data_types VALUES (1, False, 0, 0, 0, 0, 0, '', '\x00', '0001-01-01', '00:00:00', '0001-01-01 00:00:00', '0001-01-01 00:00:00'::timestamptz, interval '0 second', '{}', array[]::boolean[], array[]::smallint[], array[]::integer[], array[]::bigint[], array[]::decimal[], array[]::double precision[], array[]::varchar[], array[]::bytea[], array[]::date[], array[]::time[], array[]::timestamp[], array[]::timestamptz[], array[]::interval[], array[]::jsonb[], ROW(1, False));

INSERT INTO bq_sink_data_types VALUES (2, False, -32767, -2147483647, -9223372036854775807, -10.0, -10000.0, 'aa', '\x00', '1970-01-01', '12:34:56.123456', '1970-01-01 00:00:00.123456', '1970-01-01 00:00:00.123456Z', interval '10 second', '{}', array[False::boolean]::boolean[], array[-32767::smallint]::smallint[], array[-2147483647::integer]::integer[], array[-9223372036854775807::bigint]::bigint[], array[-10.0::decimal]::decimal[], array[-10000.0::double precision]::double precision[], array[''::varchar]::varchar[], array['\x00'::bytea]::bytea[], array['1970-01-01'::date]::date[], array['23:00:00'::time]::time[], array['1970-01-01 00:00:00'::timestamp]::timestamp[], array['1970-01-01 00:00:00Z'::timestamptz]::timestamptz[], array[interval '0 second'::interval]::interval[], array['{}'::jsonb]::jsonb[], ROW(2, True));

INSERT INTO bq_sink_data_types VALUES (3, True, 32767, 2147483647, 9223372036854775807, -10.0, 10000.0, 'zzzzzz', '\xffffff', '9999-12-31', '23:59:59.999999', '9999-12-31 23:59:59.999999', '9999-12-31 23:59:59.999999Z', interval '9990 year', '{"whatever":"meaningless"}', array[True::boolean]::boolean[], array[32767::smallint]::smallint[], array[2147483647::integer]::integer[], array[9223372036854775807::bigint]::bigint[], array[-10.0::decimal]::decimal[], array[10000.0::double precision]::double precision[], array['zzzzzz'::varchar]::varchar[], array['\xffffff'::bytea]::bytea[], array['9999-12-31'::date]::date[], array['23:59:59'::time]::time[], array['9999-12-31 23:59:59'::timestamp]::timestamp[], array['9999-12-31 23:59:59Z'::timestamptz]::timestamptz[], array[interval '9990 year'::interval]::interval[], array['{"whatever":"meaningless"}'::jsonb]::jsonb[], ROW(3, True));
