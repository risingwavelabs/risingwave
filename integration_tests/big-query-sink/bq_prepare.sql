DROP TABLE IF EXISTS `rwctest.bqtest.bq_sink`;
CREATE TABLE `rwctest.bqtest.bq_sink` (
    user_id INT64,
    target_id STRING,
    event_timestamp TIMESTAMP
);
