CREATE MATERIALIZED VIEW target_count AS
SELECT
    target_id,
    COUNT(*) AS target_count
FROM
    user_behaviors
GROUP BY
    target_id;

CREATE SINK target_count_postgres_sink
FROM
    target_count WITH (
        connector = 'jdbc',
        jdbc.url = 'jdbc:postgresql://cratedb:5432/crate?user=crate',
        table.name = 'target_count',
        type = 'upsert'
    );