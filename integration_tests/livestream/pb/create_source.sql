CREATE SOURCE live_stream_metrics_pb WITH (
    connector = 'kafka',
    topic = 'live_stream_metrics',
    properties.bootstrap.server = 'message_queue:29092',
    scan.startup.mode = 'earliest'
) FORMAT PLAIN ENCODE PROTOBUF  (message = 'livestream.schema.LiveStreamMetrics', schema.location = 'http://file_server:8080/schema');