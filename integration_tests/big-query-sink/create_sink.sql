set sink_decouple = false;

-- create sink with local file
CREATE SINK bhv_big_query_sink
FROM
    bhv_mv WITH (
    connector = 'bigquery',
    type = 'append-only',
    bigquery.local.path= '/gcp-rwctest.json',
    bigquery.project= 'rwctest',
    bigquery.dataset= 'bqtest',
    bigquery.table= 'bq_sink',
    force_append_only='true'
);


-- create sink with s3 file
-- CREATE SINK bhv_big_query_sink
-- FROM
    -- bhv_mv WITH (
    -- connector = 'bigquery',
    -- type = 'append-only',
    -- bigquery.s3.path= '${s3_service_account_json_path}',
    -- bigquery.project= '${project_id}',
    -- bigquery.dataset= '${dataset_id}',
    -- bigquery.table= '${table_id}',
    -- access_key = '${aws_access_key}',
    -- secret_key = '${aws_secret_key}',
    -- region = '${aws_region}',
    -- force_append_only='true',
-- );

CREATE SINK bq_sink_data_types_sink
FROM
    bq_sink_data_types WITH (
    connector = 'bigquery',
    type = 'append-only',
    bigquery.local.path= '/gcp-rwctest.json',
    bigquery.project= 'rwctest',
    bigquery.dataset= 'bqtest',
    bigquery.table= 'bq_sink_data_types',
    force_append_only='true'
);
