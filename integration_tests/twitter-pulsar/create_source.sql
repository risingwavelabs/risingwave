--
-- The Pulsar source version
--
CREATE SOURCE twitter (data JSONB, author JSONB) WITH (
    connector = 'pulsar',
    pulsar.topic = 'twitter',
    pulsar.service.url = 'pulsar://message_queue:6650'
) ROW FORMAT JSON;