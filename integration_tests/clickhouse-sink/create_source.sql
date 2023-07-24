CREATE table user_behaviors (
    user_id VARCHAR,
    target_id VARCHAR,
    target_type VARCHAR,
    event_timestamp TIMESTAMP,
    behavior_type VARCHAR,
    parent_target_type VARCHAR,
    parent_target_id VARCHAR,
    PRIMARY KEY(user_id)
) WITH (
    connector = 'datagen',
    fields.seq_id.kind = 'sequence',
    fields.seq_id.start = '1',
    fields.seq_id.end = '10000000',
    fields.user_id.kind = 'random',
    fields.user_id.min = '1',
    fields.user_id.max = '10000000',
    fields.user_name.kind = 'random',
    fields.user_name.length = '10',
    datagen.rows.per.second = '20000'
) FORMAT PLAIN ENCODE JSON;