# HOW-TO

This demo showcases how to sink RisingWave's data to an external Postgres. The data loader has been included in the docker compose so the data will be loaded to Postgres once the cluster is set up.

Here's what this demo does:

1. `docker compose up -d`: Start the cluster.
2. After 20-30s: `create_source.sql`, `create_mv.sql`, `create_sink.sql`.
3. After another 30s, the tester will check if the ingestion is successful by `SELECT COUNT(*) FROM target_count;` in Postgres.

To connect to the Postgres on your local PC:

```sh
psql postgresql://myuser:123456@127.0.0.1:5432/mydb
```
