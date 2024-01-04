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
