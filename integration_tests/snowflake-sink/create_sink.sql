CREATE SINK snowflake_sink FROM ss_mv WITH (
    connector = 'snowflake',
    type = 'append-only',
    snowflake.database = 'EXAMPLE_DB',
    snowflake.schema = 'EXAMPLE_SCHEMA',
    snowflake.pipe = 'EXAMPLE_SNOWFLAKE_PIPE',
    snowflake.account_identifier = '<ORG_NAME>-<ACCOUNT_NAME>',
    snowflake.user = 'XZHSEH',
    snowflake.rsa_public_key_fp = 'EXAMPLE_FP',
    snowflake.private_key = 'EXAMPLE_PK',
    snowflake.s3_bucket = 'EXAMPLE_S3_BUCKET',
    snowflake.aws_access_key_id = 'EXAMPLE_AWS_ID',
    snowflake.aws_secret_access_key = 'EXAMPLE_SECRET_KEY',
    snowflake.aws_region = 'EXAMPLE_REGION',
    snowflake.max_batch_row_num = '1030',
    snowflake.s3_path = 'EXAMPLE_S3_PATH',
    -- depends on your mv setup, note that snowflake sink only supports
    -- append-only at present.
    force_append_only = 'true'
);