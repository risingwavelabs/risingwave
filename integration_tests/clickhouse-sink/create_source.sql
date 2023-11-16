CREATE TABLE user_behaviors (
    user_id INT,
    target_id VARCHAR,
    target_type VARCHAR,
    event_timestamp TIMESTAMP,
    behavior_type VARCHAR,
    parent_target_type VARCHAR,
    parent_target_id VARCHAR,
    PRIMARY KEY(user_id)
) WITH (
    connector = 'datagen',
    fields.user_id.kind = 'sequence',
    fields.user_id.start = '1',
    fields.user_id.end = '100',
    fields.user_name.kind = 'random',
    fields.user_name.length = '10',
    datagen.rows.per.second = '50'
) FORMAT PLAIN ENCODE JSON;