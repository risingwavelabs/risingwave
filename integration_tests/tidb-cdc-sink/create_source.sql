CREATE TABLE tweet (
  id varchar,
  text varchar,
  lang varchar,
  created_at timestamp, -- #13682: shall be timestamptz from TiDB timestamp, but canal is problematic
  author_id varchar,
  PRIMARY KEY (id)
) WITH (
    connector='kafka',
    topic='ticdc_test_tweet',
    properties.bootstrap.server='kafka:9092',
    scan.startup.mode='earliest'
) FORMAT CANAL ENCODE JSON;

create table user (
    id varchar,
    name varchar,
    username varchar,
    followers bigint,
    created_at timestamp,
    PRIMARY KEY (id)
) WITH (
    connector='kafka',
    topic='ticdc_test_user',
    properties.bootstrap.server='kafka:9092',
    scan.startup.mode='earliest'
) FORMAT CANAL ENCODE JSON;

create table datatype (
    id int,
    c0_boolean boolean,
    c1_tinyint smallint,
    c2_smallint smallint,
    c3_mediumint int,
    c4_int int,
    c5_bigint bigint,
    c6_float real,
    c7_double double,
    c8_decimal decimal,
    c9_date date,
    c10_datetime timestamp,
    c11_time time,
    c12_timestamp timestamp,
    c13_char varchar,
    c14_varchar varchar,
    c15_binary bytea,
    c16_varbinary bytea,
    c17_blob bytea,
    c18_text text,
    c19_json jsonb,
    PRIMARY KEY (id)
) WITH (
      connector='kafka',
      topic='ticdc_test_datatype',
      properties.bootstrap.server='kafka:9092',
      scan.startup.mode='earliest'
) FORMAT CANAL ENCODE JSON;
