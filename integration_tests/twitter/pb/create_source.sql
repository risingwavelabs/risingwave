CREATE SOURCE twitter WITH (
    connector = 'kafka',
    topic = 'twitter',
    properties.bootstrap.server = 'message_queue:29092',
    scan.startup.mode = 'earliest'
) FORMAT PLAIN ENCODE PROTOBUF (message = 'twitter.schema.Event', schema.location = 'http://file_server:8080/schema');