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

3. Create datagen source and delta lake sink
```
psql -h localhost -p 4566 -d dev -U root

# Within psql client

CREATE SOURCE source (id int, name varchar)
WITH (
    connector = 'datagen',
    fields.id.kind = 'sequence',
    fields.id.start = '1',
    fields.id.end = '10000',
    fields.name.kind = 'random',
    fields.name.length = '10',
    datagen.rows.per.second = '200'
) ROW FORMAT JSON;
 
create sink delta_lake_sink from source
with (
    connector = 'deltalake',
    type = 'append-only',
    location = 's3a://deltalake/delta',
    s3.access.key = 'hummockadmin',
    s3.secret.key = 'hummockadmin',
    s3.endpoint = 'http://minio-0:9301'
);

```

4. Query delta lake table. The following command will query the total count of records.
```
docker compose exec spark bash /spark-script/run-sql-file.sh query-table
```