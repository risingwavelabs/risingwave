CREATE TABLE topic_counts (
    `id` BIGINT,
    `sum` BIGINT
) WITH (
    'connector' = 'kafka',
    'topic' = 'counts',
    'properties.bootstrap.servers' = 'message_queue:29092',
    'properties.group.id' = 'test-flink-client-1',
    'scan.startup.mode' = 'earliest-offset',
    'format' = 'debezium-json',
    'debezium-json.schema-include' = 'true'
);

CREATE TABLE pg_counts (
    `id` BIGINT,
    `sum` BIGINT,
    PRIMARY KEY (`id`) NOT ENFORCED
) WITH (
    'connector' = 'jdbc',
    'url' = 'jdbc:postgresql://postgres:5432/mydb?user=myuser&password=123456',
    'table-name' = 'counts'
);

INSERT INTO pg_counts
SELECT * FROM topic_counts;