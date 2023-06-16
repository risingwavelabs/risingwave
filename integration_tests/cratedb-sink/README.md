# HOW-TO

This demo showcases how to sink RisingWave's data to an external CrateDB. The data loader has been included in the docker compose so the data will be loaded to CrateDB once the cluster is set up.

Here's what this demo does:

1. `docker compose up -d`: Start the cluster.
2. After 20-30s: `create_source.sql`.
3. After 10s: `create_mv.sql`.
4. After another 10s, the tester will check if the source has ingested some data by creating a materialized view upon the source. It also checks if the MV created in the 3rd step has some data.

To connect to the CrateDB on your local PC:

```sh
psql postgresql://crate@127.0.0.1:5433/crate
```
