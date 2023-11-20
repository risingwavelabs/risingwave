CREATE SINK IF NOT EXISTS counts_sink
FROM counts
WITH (
  connector = 'kafka',
  properties.bootstrap.server='message_queue:29092',
  topic = 'counts',
  type = 'debezium',
  primary_key = 'id'
);
