set sink_decouple = false;

CREATE SINK target_count_postgres_sink
FROM target_count
WITH (
    connector = 'jdbc',
    jdbc.url = 'jdbc:postgresql://postgres:5432/mydb',
    user='myuser',
    password='123456',
    table.name = 'target_count',
    type = 'upsert',
    primary_key = 'target_id'
);

-- sink data_type table to pg
CREATE SINK data_types_postgres_sink
FROM data_types WITH (
    connector = 'jdbc',
    jdbc.url = 'jdbc:postgresql://postgres:5432/mydb',
    user='myuser',
    password='123456',
    table.name = 'data_types',
    type='upsert',
    primary_key = 'id'
);

CREATE SINK pg_all_data_types_sink
FROM
    pg_all_data_types WITH (
    connector = 'jdbc',
    jdbc.url = 'jdbc:postgresql://postgres:5432/mydb',
    user='myuser',
    password='123456',
    table.name = 'pg_all_data_types',
    type='append-only',
    primary_key = 'id',
    force_append_only = 'true',
);

CREATE SINK numeric256_types_postgres_sink
FROM numeric256_types WITH (
    connector = 'jdbc',
    jdbc.url = 'jdbc:postgresql://postgres:5432/mydb',
    user='myuser',
    password='123456',
    table.name = 'numeric256_types',
    type='upsert',
    primary_key = 'id'
);
