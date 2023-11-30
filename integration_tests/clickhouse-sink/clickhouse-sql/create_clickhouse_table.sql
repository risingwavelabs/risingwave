CREATE table demo_test(
    user_id Int32,
    target_id String,
    target_type String,
)ENGINE = ReplacingMergeTree
PRIMARY KEY (user_id);