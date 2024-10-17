set sink_decouple = false;

CREATE SINK target_count_mysql_sink
FROM
    target_count WITH (
        connector = 'jdbc',
        jdbc.url = 'jdbc:mysql://mysql:3306/mydb?user=root&password=123456',
        table.name = 'target_count',
        type = 'upsert',
        primary_key = 'target_id'
    );

CREATE SINK data_types_mysql_sink
FROM
    data_types WITH (
        connector = 'jdbc',
        jdbc.url = 'jdbc:mysql://mysql:3306/mydb?user=root&password=123456',
        table.name = 'data_types',
        type = 'upsert',
        primary_key = 'id'
    );

CREATE SINK mysql_data_all_types_sink
FROM
    mysql_all_types WITH (
        connector = 'jdbc',
        jdbc.url = 'jdbc:mysql://mysql:3306/mydb?user=root&password=123456',
        table.name = 'mysql_all_types',
        type='append-only',
        force_append_only = 'true',
        primary_key = 'id'
    );
