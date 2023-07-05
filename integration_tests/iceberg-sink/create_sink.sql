CREATE SINK bhv_iceberg_sink
FROM
    bhv_mv WITH (
    connector = 'iceberg',
    type = 'upsert',
    primary_key = 'user_id, target_id, event_timestamp',
    warehouse.path = 's3://hummock001/iceberg-data',
    s3.endpoint = 'http://minio-0:9301',
    s3.access.key = 'hummockadmin',
    s3.secret.key = 'hummockadmin',
    database.name='demo_db',
    table.name='demo_table'
);