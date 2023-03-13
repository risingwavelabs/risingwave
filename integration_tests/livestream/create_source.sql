CREATE SOURCE live_stream_metrics (
    client_ip VARCHAR,
    user_agent VARCHAR,
    user_id VARCHAR,
    -- The live room.
    room_id VARCHAR,
    -- Sent bits per second.
    video_bps BIGINT,
    -- Sent frames per second. Typically 30 fps.
    video_fps BIGINT,
    -- Round-trip time (in ms). 200ms is recommended.
    video_rtt BIGINT,
    -- Lost packets per second.
    video_lost_pps BIGINT,
    -- How long was the longest freeze (in ms).
    video_longest_freeze_duration BIGINT,
    -- Total freeze duration.
    video_total_freeze_duration BIGINT,
    report_timestamp TIMESTAMPTZ,
    country VARCHAR
) WITH (
    connector = 'kafka',
    topic = 'live_stream_metrics',
    properties.bootstrap.server = 'message_queue:29092',
    scan.startup.mode = 'earliest'
) ROW FORMAT JSON;