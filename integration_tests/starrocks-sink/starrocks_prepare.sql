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

CREATE USER 'users'@'%' IDENTIFIED BY '123456';
GRANT ALL ON *.* TO 'users'@'%';
