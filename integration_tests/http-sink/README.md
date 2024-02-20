# Demo: Sinking to Http

In this demo, we want to showcase how RisingWave is able to sink data to Http.

1. Launch the cluster:

```sh
docker-compose up -d
```

The cluster contains a RisingWave cluster and its necessary dependencies, a datagen that generates the data.

2. Build an http server that can be built on its own

3. Execute the SQL queries in sequence:

- create_source.sql
- create_mv.sql
- create_sink.sql

3. Check the contents in Http:
On the http server side it will receive the json string, something like:
```
{"user_id":5,"target_id":"siFqrkdlCn"}
```
The number of json strings is 1000
