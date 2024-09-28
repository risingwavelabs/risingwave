CREATE SOURCE twitter WITH (
    connector = 'pulsar',
    pulsar.topic = 'twitter',
    pulsar.service.url = 'pulsar://message_queue:6650',
    subscription.name.prefix = 'custom_prefix'
) ROW FORMAT PROTOBUF MESSAGE 'twitter.schema.Event' ROW SCHEMA LOCATION 'http://file_server:8080/schema';