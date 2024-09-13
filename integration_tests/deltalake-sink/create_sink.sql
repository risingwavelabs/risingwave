set sink_decouple = false;

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

create sink data_types_sink from data_types
with (
    connector = 'deltalake',
    type = 'append-only',
    force_append_only='true',
    location = 's3a://deltalake/data_types',
    s3.access.key = 'hummockadmin',
    s3.secret.key = 'hummockadmin',
    s3.endpoint = 'http://minio-0:9301'
);
