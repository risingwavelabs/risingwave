--
-- The Pulsar source version
--
CREATE SOURCE twitter (
    data STRUCT < created_at TIMESTAMPTZ,
    id VARCHAR,
    text VARCHAR,
    lang VARCHAR >,
    author STRUCT < created_at TIMESTAMPTZ,
    id VARCHAR,
    name VARCHAR,
    username VARCHAR,
    followers INT >
) WITH (
    connector = 'pulsar',
    pulsar.topic = 'twitter',
    pulsar.admin.url = 'http://message_queue:8080',
    pulsar.service.url = 'pulsar://message_queue:6650'
) ROW FORMAT JSON;
