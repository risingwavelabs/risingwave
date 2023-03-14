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
