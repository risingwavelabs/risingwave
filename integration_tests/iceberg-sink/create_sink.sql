CREATE SINK bhv_iceberg_sink
FROM
    bhv_mv WITH (
    connector = 'iceberg',
    sink.mode='upsert',
    location.type='minio',
    warehouse.path='minio://hummockadmin:hummockadmin@minio-0:9301/hummock001/iceberg-data',
    database.name='demo_db',
    table.name='demo_table'
);