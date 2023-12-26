CREATE database demo;
use demo;

CREATE table demo_bhv_table(
    user_id int,
    target_id text,
    event_timestamp_local datetime
) UNIQUE KEY(`user_id`)
DISTRIBUTED BY HASH(`user_id`) BUCKETS 1
PROPERTIES (
    "replication_allocation" = "tag.location.default: 1"
);

CREATE USER 'users'@'%' IDENTIFIED BY '123456';
GRANT ALL ON *.* TO 'users'@'%';
