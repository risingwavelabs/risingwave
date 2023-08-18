CREATE table demo_test(
    user_id Int32,
    target_id String,
    event_timestamp DateTime64,
)ENGINE = ReplacingMergeTree
PRIMARY KEY (user_id);