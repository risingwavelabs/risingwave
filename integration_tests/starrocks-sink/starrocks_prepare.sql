CREATE database demo;
use demo;

CREATE table demo_bhv_table(
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

CREATE USER 'users'@'%' IDENTIFIED BY '123456';
GRANT ALL ON *.* TO 'users'@'%';
