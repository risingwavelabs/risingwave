CREATE TABLE live_stream_metrics
WITH
  (
    connector = 'nats',
    server_url = 'nats-server:4222',
    subject = 'live_stream_metrics',
    stream = 'risingwave',
    allow_create_stream = 'true',
    connect_mode = 'plain'
  ) FORMAT PLAIN ENCODE PROTOBUF (
    message = 'livestream.schema.LiveStreamMetrics',
    schema.location = 'http://file_server:8080/schema'
  );
