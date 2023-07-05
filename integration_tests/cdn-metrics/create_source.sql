CREATE SOURCE nics_metrics (
    device_id VARCHAR,
    metric_name VARCHAR,
    aggregation VARCHAR,
    nic_name VARCHAR,
    report_time TIMESTAMPTZ,
    bandwidth DOUBLE PRECISION,
    metric_value DOUBLE PRECISION
) WITH (
    connector = 'kafka',
    topic = 'nics_metrics',
    properties.bootstrap.server = 'message_queue:29092',
    scan.startup.mode = 'earliest'
) ROW FORMAT JSON;

CREATE SOURCE tcp_metrics (
    device_id VARCHAR,
    metric_name VARCHAR,
    report_time TIMESTAMPTZ,
    metric_value DOUBLE PRECISION
) WITH (
    connector = 'kafka',
    topic = 'tcp_metrics',
    properties.bootstrap.server = 'message_queue:29092',
    scan.startup.mode = 'earliest'
) ROW FORMAT JSON;