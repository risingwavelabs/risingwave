# Demo: Sinking to Cassandra

In this demo, we want to showcase how RisingWave is able to sink data to Cassandra.

1. Launch the cluster:

```sh
docker compose up -d
```

The cluster contains a RisingWave cluster and its necessary dependencies, a datagen that generates the data, a Cassandra for sink.


2. Create the Cassandra table:

```sh
docker compose exec cassandra bash /opt/cassandra/cassandra-sql/run-sql-file.sh create_cassandra_table
```

3. Execute the SQL queries in sequence:

- create_source.sql
- create_mv.sql
- create_sink.sql

4. Execute a simple query:

```sh
docker compose exec cassandra bash /opt/cassandra/cassandra-sql/run-sql-file.sh cassandra_query

```

```sql
select user_id, count(*) from my_keyspace.demo_test group by user_id
```
