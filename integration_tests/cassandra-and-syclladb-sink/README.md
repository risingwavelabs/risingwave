# Demo: Sinking to Cassandra/Scylladb

In this demo, we want to showcase how RisingWave is able to sink data to Cassandra.

1. Set the compose profile accordingly:
Demo with Apache Cassandra:
```
export COMPOSE_PROFILES=cassandra
```

Demo with Scylladb
```
export COMPOSE_PROFILES=scylladb
```

2. Launch the cluster:

```sh
docker-compose up -d
```

The cluster contains a RisingWave cluster and its necessary dependencies, a datagen that generates the data, a Cassandra for sink.


3. Create the Cassandra table via cqlsh:

Login to cqlsh
```sh
# cqlsh into cassandra
docker compose exec cassandra cqlsh
# cqlsh into scylladb
docker compose exec scylladb cqlsh
```

Run the following queries to create keyspace and table.
```sql
CREATE KEYSPACE demo WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};
use demo;
CREATE table demo_bhv_table(
    user_id int primary key,
    target_id text,
    event_timestamp timestamp,
);
```

3. Execute the SQL queries in sequence:

- create_source.sql
- create_mv.sql
- create_sink.sql

4. Execute a simple query to check the sink results via csqlsh:

Login to cqlsh
```sh
# cqlsh into cassandra
docker compose exec cassandra cqlsh
# cqlsh into scylladb
docker compose exec scylladb cqlsh
```

Run the following query
```sql
select user_id, count(*) from my_keyspace.demo_test group by user_id;
```
