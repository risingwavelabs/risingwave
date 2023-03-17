CREATE MATERIALIZED VIEW target_count AS
SELECT
    target_id,
    COUNT(*) AS target_count
FROM
    user_behaviors
GROUP BY
    target_id;

CREATE SINK target_count_mysql_sink
FROM
    target_count WITH (
        connector = 'jdbc',
        jdbc.url = 'jdbc:mysql://mysql:3306/mydb?user=root&password=123456',
        table.name = 'target_count'
    );