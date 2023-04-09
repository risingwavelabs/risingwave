CREATE SOURCE twitter WITH (
    connector = 'kafka',
    topic = 'twitter',
    properties.bootstrap.server = 'message_queue:29092',
    scan.startup.mode = 'earliest'
) ROW FORMAT AVRO MESSAGE 'Event' ROW SCHEMA LOCATION 'http://file_server:8080/avro.json';