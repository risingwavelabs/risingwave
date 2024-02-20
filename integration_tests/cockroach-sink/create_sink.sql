CREATE SINK target_count_postgres_sink
FROM
    target_count WITH (
        connector = 'jdbc',
        jdbc.url = 'jdbc:postgresql://cockroachdb:26257/defaultdb?user=root',
        table.name = 'target_count',
        type = 'upsert',
        primary_key = 'target_id'
    );

-- sink data_type table to pg
CREATE SINK data_types_postgres_sink
FROM
    data_types WITH (
    connector = 'jdbc',
        jdbc.url = 'jdbc:postgresql://cockroachdb:26257/defaultdb?user=root',
    table.name = 'data_types',
    type='upsert',
    primary_key = 'id'
);
