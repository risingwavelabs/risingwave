CREATE SOURCE live_stream_metrics_pb WITH (
    connector = 'kafka',
    topic = 'live_stream_metrics',
    properties.bootstrap.server = 'message_queue:29092',
    scan.startup.mode = 'earliest'
) ROW FORMAT PROTOBUF MESSAGE 'livestream.schema.LiveStreamMetrics' ROW SCHEMA LOCATION 'http://file_server:8080/schema';
