# Demo: Sinking to Bigquery

In this demo, we want to showcase how RisingWave is able to sink data to Bigquery.

1. Launch the cluster:

(need put gcp key file in risingwave/gcp-rwctest.json)
```sh
docker-compose up -d
```

The cluster contains a RisingWave cluster and its necessary dependencies, a datagen that generates the data.

3. Create the Bigquery table in Bigquery

```sql
CREATE table '${project_id}'.'${dataset_id}'.'${table_id}'(
    user_id INT64,
    target_id STRING,
    event_timestamp TIMESTAMP
);
```

4. Execute the SQL queries in sequence:

- create_source.sql
- create_mv.sql
- create_sink.sql

    1. We need to obtain the JSON file for Google Cloud service accounts, which can be configured here: https://console.cloud.google.com/iam-admin/serviceaccounts.
    2. Because BigQuery has limited support for updates and deletes, we currently only support 'append only'
    3. Regarding file path, we can choose between S3 and local files, and the specific SQL statement is in the 'create_sink.sql'.

Run the following query
```sql
select user_id, count(*) from demo.demo_bhv_table group by user_id;
```
