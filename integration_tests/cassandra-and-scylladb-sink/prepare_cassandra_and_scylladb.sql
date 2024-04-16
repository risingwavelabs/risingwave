CREATE KEYSPACE demo WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};
use demo;
CREATE table demo_bhv_table(
    user_id int primary key,
    target_id text,
    event_timestamp timestamp,
);

CREATE table cassandra_types (
  types_id int primary key,
  c_boolean boolean,
  c_smallint smallint,
  c_integer int,
  c_bigint bigint,
  c_decimal decimal,
  c_real float,
  c_double_precision double,
  c_varchar text,
  c_bytea blob,
  c_date date,
  c_time time,
  c_timestamptz timestamp,
  c_interval duration
);
