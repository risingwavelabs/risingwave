CREATE SINK pubsub_sink
FROM
  personnel
WITH
(
    connector = 'google_pubsub',
    pubsub.endpoint = 'pubsub-emulator:8900',
    pubsub.emulator_host = 'pubsub-emulator:8900',
    pubsub.project_id = 'demo',
    pubsub.topic = 'test',
) FORMAT PLAIN ENCODE JSON (
    force_append_only='true',
);

