CREATE TABLE `tweet` (
  id varchar NOT NULL,
  text varchar DEFAULT NULL,
  lang varchar DEFAULT NULL,
  created_at timestamp NULL DEFAULT NULL,
  author_id varchar NOT NULL,
  PRIMARY KEY (id)
) WITH (
    connector='kafka',
    topic='ticdc_test_tweet',
    properties.bootstrap.server='kafka:9092',
    scan.startup.mode='earliest'
) ROW FORMAT CANAL_JSON;

create table user (
    id varchar NOT NULL,
    name varchar DEFAULT NULL,
    username varchar DEFAULT NULL,
    followers bigint not null,
    created_at timestamp NULL DEFAULT NULL,
    PRIMARY KEY (id)
) WITH (
    connector='kafka',
    topic='ticdc_test_user',
    properties.bootstrap.server='kafka:9092',
    scan.startup.mode='earliest'
) ROW FORMAT CANAL_JSON;
