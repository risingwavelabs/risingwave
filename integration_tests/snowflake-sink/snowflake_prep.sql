USE EXAMPLE_DB;

ALTER USER xzhseh SET RSA_PUBLIC_KEY='your local rsa public key';

-- set user permission to account admin level
GRANT ROLE ACCOUNTADMIN TO USER xzhseh;

-- you could either retrieve the fp from desc user's info panel,
-- or from the following select stmt.
DESC USER xzhseh;
-- also fine, see the documentation for details.
SELECT TRIM(
    (SELECT "value" FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()))
        WHERE "property" = 'RSA_PUBLIC_KEY_FP'),
    'SHA256:'
);

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

-- select from table after sinking from rw
SELECT * FROM example_snowflake_sink_table WHERE event_timestamp IS NOT NULL;
