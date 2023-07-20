# Demo: Sinking to Iceberg

In this demo, we want to showcase how RisingWave is able to sink data to ClickHouse.

1. Launch the cluster:

```sh
docker compose up -d
```

The cluster contains a RisingWave cluster and its necessary dependencies, a datagen that generates the data, a clichouse for sink.


2. Create the Iceberg table:

```sh
docker compose exec clickhouse-server_1 bash clickhouse-client create_clickhouse_table.sql
```

3. Execute the SQL queries in sequence:

- create_source.sql
- create_mv.sql
- create_sink.sql

4. Execute a simple query:

```sh
docker compose exec clickhouse-server_1 bash clickhouse-client clickhouse-query.sql

```

```sql
select user_id, count(*) from default.demo_test group by user_id
```
