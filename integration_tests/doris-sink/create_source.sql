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

CREATE table upsert_user_behaviors (
    user_id int,
    target_id VARCHAR,
    target_type VARCHAR,
    event_timestamp TIMESTAMPTZ,
    behavior_type VARCHAR,
    parent_target_type VARCHAR,
    parent_target_id VARCHAR,
    PRIMARY KEY(user_id)
);

INSERT INTO upsert_user_behaviors VALUES
  (1,'1','1','2020-01-01T01:01:01Z','1','1','1'),
  (2,'2','2','2020-01-01T01:01:02Z','2','2','2'),
  (3,'3','3','2020-01-01T01:01:03Z','3','3','3'),
  (4,'4','4','2020-01-01T01:01:04Z','4','4','4');
