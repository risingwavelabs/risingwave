CREATE table demo_test(
    user_id String,
    target_id String,
    event_timestamp DateTime64,
)ENGINE = MergeTree
PRIMARY KEY (user_id);