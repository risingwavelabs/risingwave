USE EXAMPLE_DB;

-- snowflake table, note to keep the same column name(s).
CREATE OR REPLACE TABLE example_snowflake_sink_table (user_id INT, target_id VARCHAR, event_timestamp TIMESTAMP_TZ);

-- snowflake stage, we only supports json as sink format at present
CREATE OR REPLACE STAGE example_snowflake_stage
    url = '<S3_PATH>'
    credentials = ( AWS_KEY_ID = '<S3_CREDENTIALS>' AWS_SECRET_KEY = '<S3_CREDENTIALS>' )
    file_format = ( type = JSON );

-- snowflake pipe
CREATE OR REPLACE PIPE example_snowflake_pipe
    AS COPY INTO example_snowflake_sink_table
    FROM @example_snowflake_stage MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE;
-- you will get an AWS SQS ARN, please add it to the list of event notifications in s3

-- select from table after sinking from rw
SELECT * FROM example_snowflake_sink_table WHERE event_timestamp IS NOT NULL;
