# Demo: Sinking to Iceberg

RisingWave only provides limited capabilities to serve complex ad-hoc queries, which typically require optimizations such as columnar storage and code generation (https://www.vldb.org/pvldb/vol4/p539-neumann.pdf). However, RisingWave's internal storage format is row-based, and we have not paid much attention to improving its batch-processing capability. Therefore, we recommend sinking the stream into Iceberg or another data lake to build a "streaming data warehouse" solution.

In this demo, we want to showcase how RisingWave is able to sink data to Iceberg for big data analytics.

1. Launch the cluster:

```sh
docker compose up -d
```

The cluster contains a RisingWave cluster and its necessary dependencies, a Spark that will be used to create the Iceberg table, a datagen that generates the data, and a Presto for querying the Iceberg.


2. Create the Iceberg table:

```sh
docker compose exec spark bash /spark-script/run-sql-file.sh create-table
```

3. Execute the SQL queries in sequence:

- create_source.sql
- create_mv.sql
- create_sink.sql

4. Connect to the Presto that is pre-installed in the docker compose and execute a simple query:

```
docker compose exec presto presto-cli --server localhost:8080
```

```sql
select user_id, count(*) from iceberg.demo_db.demo_table group by user_id
```

## Demo: Iceberg Compaction With Airflow

We can use airflow to trigger iceberg's compaction task at regular intervals, It can avoid the slowdown of queries due to too many small files.
In this demo, The compression method uses the default configuration(https://iceberg.apache.org/docs/latest/spark-procedures). The interval of
rewrite small files and manifests is 1 hours, The interval of remove orphan snapshots and files is 1 days. We can modiy it by modifying DAGS(Storage directory: airflow_dags and iceberg-compaction-sql)

1. Build the image, and start docker:

```sh
docker-compose up --build
```

2. Build sink like the last demo:

3. Connect to the airflow ui:

We need create an user for airflow ui.
```sh
docker compose exec airflow-webserver \airflow users create \
    --username admin \
    --firstname xx \
    --lastname xx \
    --role Admin \
    --email risingwaveman@superhero.org
```

Then we can connect to the airflow ui(default: localhost:8080) with username and password.

4. Configure airflow:

In airflow ui, add `conn_id` for spark host. We can find it in `Admin`->`Connection`. Its `Connection Id` is `spark_local`, its `Connection Type` is `Spark`, and its `Host` is `spark://spark:7077`.
We can find it in `Admin`->`Connection`.

5. Stark airflow task:

In airflow ui(DAGS), we can find `remove_iceberg_orphan_files` and `rewrite_iceberg_small_files`, open it by clicking on them. They will trigger tasks at regular intervals.

This demo uses `spark` and `airflow`. We can find more at https://spark.apache.org/, https://airflow.apache.org/.

