# Demo: Sinking to Doris

In this demo, we want to showcase how RisingWave is able to sink data to Doris.

1. Launch the cluster:

```sh
docker-compose up -d
```

The cluster contains a RisingWave cluster and its necessary dependencies, a datagen that generates the data, a Doris fe and be for sink.


2. Create the Doris table via mysql:

Login to mysql
```sh
docker compose exec fe mysql -uroot -P9030 -h127.0.0.1
```

Run the following queries to create database and table.
```sql
CREATE database demo;
use demo;
CREATE table demo_bhv_table(
    user_id int,
    target_id text,
    event_timestamp datetime
) UNIQUE KEY(`user_id`)
DISTRIBUTED BY HASH(`user_id`) BUCKETS 1
PROPERTIES (
    "replication_allocation" = "tag.location.default: 1"
);
CREATE USER 'users'@'%' IDENTIFIED BY '123456';
GRANT ALL ON *.* TO 'users'@'%';
```

3. Execute the SQL queries in sequence:

- create_source.sql
- create_mv.sql
- create_sink.sql


Run the following query
```sql
select user_id, count(*) from demo.demo_bhv_table group by user_id;
```
