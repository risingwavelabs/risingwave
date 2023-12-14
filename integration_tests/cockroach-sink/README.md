# HOW-TO

This demo showcases how to sink RisingWave's data to an external CockroachDB. A data loader is included in the docker compose. Once the cluster starts, it continuously ingests data into a Kafka topic `user_behaviors`.

During CI, the integration test will:

1. Run `docker compose up -d` and start the cluster.
2. After 20-30s, run `create_source.sql`, `create_mv.sql`, `create_sink.sql`
3. After another 30s, the tester will check if the ingestion is successful by `SELECT COUNT(*) FROM target_count;` in CockroachDB.

To connect to the Postgres outside the container via psql:

```sh
psql -h localhost -p 26257 -d defaultdb -U root
```
