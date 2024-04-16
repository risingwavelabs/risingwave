CREATE database demo;
use demo;

CREATE table demo_primary_table(
    user_id int,
    target_id text,
    event_timestamp_local datetime
) ENGINE=OLAP
PRIMARY KEY(`user_id`)
DISTRIBUTED BY HASH(`user_id`) properties("replication_num" = "1");

CREATE table upsert_table(
    user_id int,
    target_id text,
    event_timestamp_local datetime
) ENGINE=OLAP
PRIMARY KEY(`user_id`)
DISTRIBUTED BY HASH(`user_id`) properties("replication_num" = "1");

CREATE table demo_duplicate_table(
    user_id int,
    target_id text,
    event_timestamp_local datetime
) ENGINE=OLAP
DUPLICATE KEY(`user_id`)
DISTRIBUTED BY HASH(`user_id`) properties("replication_num" = "1");

CREATE table demo_aggregate_table(
    user_id int,
    target_id text,
    event_timestamp_local datetime
) ENGINE=OLAP
AGGREGATE KEY(`user_id`,`target_id`,`event_timestamp_local`)
DISTRIBUTED BY HASH(`user_id`) properties("replication_num" = "1");

CREATE table demo_unique_table(
    user_id int,
    target_id text,
    event_timestamp_local datetime
) ENGINE=OLAP
UNIQUE KEY(`user_id`)
DISTRIBUTED BY HASH(`user_id`) properties("replication_num" = "1");

CREATE table starrocks_types(
    types_id int,
    c_boolean boolean,
    c_smallint smallint,
    c_integer int,
    c_bigint bigint,
    c_decimal decimal,
    c_real float,
    c_double_precision double,
    c_varchar varchar(65533),
    c_date date,
    c_timestamp datetime,
    c_jsonb JSON,
    c_boolean_array ARRAY<boolean>,
    c_smallint_array ARRAY<smallint>,
    c_integer_array ARRAY<int>,
    c_bigint_array ARRAY<bigint>,
    c_decimal_array ARRAY<decimal>,
    c_real_array ARRAY<float>,
    c_double_precision_array ARRAY<double>,
    c_varchar_array ARRAY<varchar(65533)>,
    c_date_array ARRAY<date>,
    c_timestamp_array ARRAY<datetime>,
    c_jsonb_array ARRAY<json>
) ENGINE=OLAP
UNIQUE KEY(`types_id`)
DISTRIBUTED BY HASH(`types_id`) BUCKETS 1
PROPERTIES (
    "replication_num" = "1"
);

CREATE USER 'users'@'%' IDENTIFIED BY '123456';
GRANT ALL ON *.* TO 'users'@'%';
