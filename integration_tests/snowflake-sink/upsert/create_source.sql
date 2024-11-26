-- please note that this will create a source that generates 1,000 rows in 10 seconds
-- you may want to change the configuration for better testing / demo purpose

CREATE table user_behaviors (
    user_id int,
    target_id VARCHAR,
    target_type VARCHAR,
    event_timestamp TIMESTAMPTZ,
    behavior_type VARCHAR,
    parent_target_type VARCHAR,
    parent_target_id VARCHAR,
    PRIMARY KEY(user_id)
) WITH (
    connector = 'datagen',
    fields.user_id.kind = 'sequence',
    fields.user_id.start = '1',
    fields.user_id.end = '1000',
    datagen.rows.per.second = '100'
) FORMAT PLAIN ENCODE JSON;
