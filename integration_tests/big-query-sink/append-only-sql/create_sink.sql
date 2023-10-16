CREATE SINK bhv_big_query_sink
FROM
    bhv_mv WITH (
    connector = 'bigquery',
    type = 'append-only',
    bigquery.path= '${bigquery_service_account_json_path}',
    bigquery.project= '${project_id}',
    bigquery.dataset= '${dataset_id}',
    bigquery.table= '${table_id}',
    force_append_only='true'
);