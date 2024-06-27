# HOW-TO

This demo showcases how to ingest Vector logs into RisingWave. Data is generated via Vector's [demo_logs](https://vector.dev/docs/reference/configuration/sources/demo_logs/) source,
which generates fake JSON logs. Note that the log format (`format: "json"`) doesn't affect the final encoding that Vector pipes to RisingWave.

Eventually, the data in RisingWave will be like below:

| source_id | data_type | value        |
| --------- | --------- | ------------ |
| demo      | Log       | \x7b226d6... |

During CI, the integration test will:

1. Run `docker compose up -d` and start the cluster.
2. After 20-30s: `create_source.sql`.
3. After 10s: `create_mv.sql`.
4. Check data. See `query.sql` and `data_check`. The latter contains the RisingWave tables that are supposed to have data.

To connect to the RisingWave outside the container via psql:

```sh
psql -h localhost -p 4566 -d dev -U root
```
