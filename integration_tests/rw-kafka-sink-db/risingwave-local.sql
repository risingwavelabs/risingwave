-- TODO: add struct and list type when we support them
CREATE TABLE type_test(
    bool_type boolean,
    smallint_type smallint,
    int_type int,
    bigint_type bigint,
    numeric_type numeric,
    real_type real,
    double_type double,
    varchar_type varchar,
    bytea_type bytea,
    date_type date,
    time_type time,
    timestamp_type timestamp,
    timestamptz_type timestamptz,
    interval_type interval,
);

CREATE SINK IF NOT EXISTS types
FROM type_test
WITH (
connector = 'kafka',
properties.bootstrap.server='localhost:9092',
topic = 'type_test',
type = 'debezium',
use_transaction = 'false',
primary_key = 'int_type'
);

insert into type_test values
    (true, 32767, 2147483647, 9223372036854775807,
     131.1313, 1313.212456, 248.2434982313, 'hello world',
     '\xDe00BeEf', '2022-04-08', '18:20:49', '2022-03-13 01:00:00'::timestamp,
     '2022-03-13 01:00:00Z'::timestamptz, interval '3 day');
