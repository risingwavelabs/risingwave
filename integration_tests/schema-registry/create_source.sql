CREATE SOURCE student WITH (
    connector = 'kafka',
    topic = 'sr-test',
    properties.bootstrap.server = 'message_queue:29092',
    scan.startup.mode = 'earliest'
)
FORMAT PLAIN ENCODE AVRO (schema.registry = 'http://message_queue:8081');

CREATE SOURCE student_karapace WITH (
    connector = 'kafka',
    topic = 'sr-test',
    properties.bootstrap.server = 'message_queue:29092',
    scan.startup.mode = 'earliest'
)
FORMAT PLAIN ENCODE AVRO (schema.registry = 'http://karapace-registry:8081');
