create sink delta_lake_sink from source
with (
    connector = 'deltalake',
    type = 'append-only',
    force_append_only='true',
    location = 's3a://deltalake/delta',
    s3.access.key = 'hummockadmin',
    s3.secret.key = 'hummockadmin',
    s3.endpoint = 'http://minio-0:9301'
);