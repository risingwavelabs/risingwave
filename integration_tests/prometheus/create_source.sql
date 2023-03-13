CREATE SOURCE prometheus (
    labels STRUCT < __name__ VARCHAR,
    instance VARCHAR,
    job VARCHAR >,
    name VARCHAR,
    timestamp TIMESTAMPTZ,
    value VARCHAR
) WITH (
    connector = 'kafka',
    topic = 'prometheus',
    properties.bootstrap.server = 'message_queue:29092',
    scan.startup.mode = 'earliest'
) ROW FORMAT JSON;