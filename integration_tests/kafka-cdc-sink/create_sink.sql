CREATE SINK IF NOT EXISTS counts_sink
FROM counts
WITH (
  connector = 'kafka',
  properties.bootstrap.server='message_queue:29092',
  topic = 'counts',
  primary_key = 'id'
) FORMAT DEBEZIUM ENCODE JSON;
