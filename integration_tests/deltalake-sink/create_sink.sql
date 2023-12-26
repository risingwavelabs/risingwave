create sink delta_lake_sink from source
with (
    connector = 'deltalake',
    type = 'append-only',
    location = 's3a://deltalake/delta',
    s3.access.key = 'hummockadmin',
    s3.secret.key = 'hummockadmin',
    s3.endpoint = 'http://minio-0:9301'
);

-- Wait docker image version
-- create sink delta_lake_sink_rust from source
-- with (
--     connector = 'deltalake_rust',
--     type = 'append-only',
--     location = 's3a://deltalake/delta-rust',
--     s3.access.key = 'hummockadmin',
--     s3.secret.key = 'hummockadmin',
--     s3.endpoint = 'http://minio-0:9301'
-- );