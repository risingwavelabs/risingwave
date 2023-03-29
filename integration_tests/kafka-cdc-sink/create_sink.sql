CREATE SINK IF NOT EXISTS counts_sink
FROM counts
WITH (
connector = 'kafka',
properties.bootstrap.server='message_queue:9092',
topic = 'counts',
type = 'debezium',
use_transaction = 'false',
primary_key = 'id'
);