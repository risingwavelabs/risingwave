CREATE TABLE live_stream_metrics
WITH
  (
    connector = 'nats',
    server_url = 'nats-server:4222',
    subject = 'live_stream_metrics',
    stream = 'risingwave',
    allow_create_stream = 'true',
    connect_mode = 'plain',
    consumer.durable_name = 'consumer_pb'
  ) FORMAT PLAIN ENCODE PROTOBUF (
    message = 'livestream.schema.LiveStreamMetrics',
    schema.location = 'http://file_server:8080/schema'
  );

-- Second consumer on the same stream, mirroring the JSON test setup.
-- Required by data_check which validates both live_stream_metrics and live_stream_metrics_2.
CREATE TABLE live_stream_metrics_2
WITH
  (
    connector = 'nats',
    server_url = 'nats-server:4222',
    subject = 'live_stream_metrics',
    stream = 'risingwave',
    allow_create_stream = 'true',
    connect_mode = 'plain',
    consumer.durable_name = 'consumer_pb_2'
  ) FORMAT PLAIN ENCODE PROTOBUF (
    message = 'livestream.schema.LiveStreamMetrics',
    schema.location = 'http://file_server:8080/schema'
  );
