# HOW-TO

This demo showcases how to sink Vector's data to RisingWave. Data is generated from source `demo_logs` of Vector.

During CI, the integration test will:

1. Run `docker compose up -d` and start the cluster.
2. After 20-30s: `create_source.sql`.
3. After 10s: `create_mv.sql`.
4. Check data.

To connect to the RisingWave outside the container via psql:

```sh
psql -h localhost -p 4566 -d dev -U root
```
You can also use the following command to query 10 records:

```sql
psql -h localhost -p 4566 -d dev -U root -f query.sql
```

`query.sql` is located in `$project/integration-tests/vector/`.
