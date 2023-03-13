CREATE SOURCE student WITH (
    connector = 'kafka',
    topic = 'sr-test',
    properties.bootstrap.server = 'message_queue:29092',
    scan.startup.mode = 'earliest'
)
ROW FORMAT avro message 'student'
row schema location confluent schema registry 'http://message_queue:8081';