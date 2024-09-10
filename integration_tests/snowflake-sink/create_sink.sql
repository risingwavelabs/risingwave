CREATE SINK snowflake_sink FROM ss_mv WITH (
    connector = 'snowflake',
    type = 'append-only',
    s3.bucket_name = 'EXAMPLE_S3_BUCKET',
    s3.credentials.access = 'EXAMPLE_AWS_ACCESS',
    s3.credentials.secret = 'EXAMPLE_AWS_SECRET',
    s3.region_name = 'ap-EXAMPLE_REGION-2',
    s3.path = 'EXAMPLE_S3_PATH',
    -- depends on your mv setup, note that snowflake sink *only* supports
    -- `append-only` mode at present.
    force_append_only = 'true'
);