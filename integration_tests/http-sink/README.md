# Demo: Sinking to Http

In this demo, we want to showcase how RisingWave is able to sink data to Http. This feature is depended on https://github.com/getindata/flink-http-connector.

It has a few limitations:
1. It offers only two options for HTTP method, i.e, PUT and POST.
2. It can only execute one request-reply round to the service (session-less).
3. It cannot handle status codes in the SQL API.

Therefore, we suggest you to try Python UDF at first.

### Demo:
1. Launch the cluster:

```sh
docker-compose up -d
```

The cluster contains a RisingWave cluster and its necessary dependencies, a datagen that generates the data.

2. Build an Http Server that can be built on its own

3. Execute the SQL queries in sequence:

- create_source.sql
- create_mv.sql
- create_sink.sql

4. Check the contents in Http Server:
On the Http Server side it will receive the json string, something like:
```
{"user_id":5,"target_id":"siFqrkdlCn"}
```
The number of json is 1000
