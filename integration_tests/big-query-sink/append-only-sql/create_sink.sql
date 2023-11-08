-- create sink with local file
CREATE SINK bhv_big_query_sink
FROM
    bhv_mv WITH (
    connector = 'bigquery',
    type = 'append-only',
    bigquery.local.path= '${bigquery_service_account_json_path}',
    bigquery.project= '${project_id}',
    bigquery.dataset= '${dataset_id}',
    bigquery.table= '${table_id}',
    force_append_only='true'
);


-- create sink with s3 file
CREATE SINK bhv_big_query_sink
FROM
    bhv_mv WITH (
    connector = 'bigquery',
    type = 'append-only',
    bigquery.s3.path= '${s3_service_account_json_path}',
    bigquery.project= '${project_id}',
    bigquery.dataset= '${dataset_id}',
    bigquery.table= '${table_id}',
    access_key = '${aws_access_key}',
    secret_access = '${aws_secret_access}',
    region = '${aws_region}',
    force_append_only='true',
);