CREATE TABLE tweet (
  id varchar,
  text varchar,
  lang varchar,
  created_at timestamp,
  author_id varchar,
  PRIMARY KEY (id)
) WITH (
    connector='kafka',
    topic='ticdc_test_tweet',
    properties.bootstrap.server='kafka:9092',
    scan.startup.mode='earliest'
) ROW FORMAT CANAL_JSON;

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
) ROW FORMAT CANAL_JSON;
