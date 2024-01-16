CREATE SINK bhv_redis_sink_1
FROM
    bhv_mv WITH (
    primary_key = 'user_id',
    connector = 'redis',
    redis.url= 'redis://redis:6379/',
)FORMAT PLAIN ENCODE JSON(force_append_only='true');

CREATE SINK bhv_redis_sink_2
FROM
    bhv_mv WITH (
    primary_key = 'user_id',
    connector = 'redis',
    redis.url= 'redis://redis:6379/',
)FORMAT PLAIN ENCODE TEMPLATE(force_append_only='true', key_format = 'UserID:{user_id}', value_format = 'TargetID:{target_id},EventTimestamp{event_timestamp}');

CREATE SINK redis_types_json_sink
FROM
    redis_types WITH (
    primary_key = 'types_id',
    connector = 'redis',
    redis.url= 'redis://redis:6379/',
)FORMAT PLAIN ENCODE JSON(force_append_only='true');

CREATE SINK redis_types_template_sink
FROM
    redis_types WITH (
    primary_key = 'types_id',
    connector = 'redis',
    redis.url= 'redis://redis:6379/',
)FORMAT PLAIN ENCODE TEMPLATE(force_append_only='true',
key_format = 'TYPESID:{types_id}',
value_format = '
  CBoolean:{c_boolean},
  CSmallint:{c_smallint},
  CInteger:{c_integer},
  CBigint:{c_bigint},
  CDecimal:{c_decimal},
  CReal:{c_real},
  CDoublePrecision:{c_double_precision},
  CVarchar:{c_varchar},
  CBytea:{c_bytea},
  CDate:{c_date},
  CTime:{c_time},
  CTimestamp:{c_timestamp},
  CTimestamptz:{c_timestamptz},
  CInterval:{c_interval},
  CJsonb:{c_jsonb},
  CBooleanArray:{c_boolean_array},
  CSmallintArray:{c_smallint_array},
  CIntegerArray:{c_integer_array},
  CBigintArray:{c_bigint_array},
  CDecimalArray:{c_decimal_array},
  CRealArray:{c_real_array},
  CDoublePrecisionArray:{c_double_precision_array},
  CVarcharArray:{c_varchar_array},
  CByteaArray:{c_bytea_array},
  CDateArray:{c_date_array},
  CTimeArray:{c_time_array},
  CTimestampArray:{c_timestamp_array},
  CTimestamptzArray:{c_timestamptz_array},
  CIntervalArray:{c_interval_array},
  CJsonbArray:{c_jsonb_array},
  CStruct:{c_struct},
  '
);
