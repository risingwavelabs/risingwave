# Demo: Sinking to Redis

In this demo, we want to showcase how RisingWave is able to sink data to Redis.

1. Launch the cluster:

```sh
docker compose up -d
```

The cluster contains a RisingWave cluster and its necessary dependencies, a datagen that generates the data, a Redis for sink.


2. Execute the SQL queries in sequence:

- create_source.sql
- create_mv.sql
- create_sink.sql

3. Execute a simple query:

```sh
docker compose exec redis redis-cli keys '*'

```
We also can use 'get' to query value
