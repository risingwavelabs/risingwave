# Demo: Sinking to Delta Lake

In this demo, we will create an append-only source via our datagen source,
and sink the data generated from source to the downstream delta lake table
stored on minio.

1. Launch the cluster via docker compose
```
docker compose up -d
```

2. Create a delta lake table on minio
```
docker compose exec minio-0 mkdir /data/deltalake
docker compose exec spark bash /spark-script/run-sql-file.sh create-table
```

3. Execute the SQL queries in sequence:
    - create_source.sql
    - create_sink.sql

4. Query delta lake table. The following command will query the total count of records.
```
docker compose exec spark bash /spark-script/run-sql-file.sh query-table
```