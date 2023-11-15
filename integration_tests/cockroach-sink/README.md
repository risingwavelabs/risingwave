# HOW-TO

This demo showcases how to sink RisingWave's data to an external CockroachDB. A data loader is included in the docker compose. Once the cluster starts, it continuously ingests data into a Kafka topic `user_behaviors`.

During CI, the integration test will:

1. Run `docker compose up -d` and start the cluster.
2. After 20-30s, run `create_source.sql`.
3. After 10s, run `create_mv.sql`.
4. After another 10s, the tester will check if the ingestion is successful by creating a materialized view upon the source. It also checks if the MV created in the 3rd step has persisted the data.

To connect to the Postgres outside the container via psql:

```sh
psql -h localhost -p 26257 -d defaultdb -U root
```
