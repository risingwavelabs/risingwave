-- First source: consumes from 'live_stream_metrics' subject in 'risingwave' stream
CREATE TABLE live_stream_metrics (
  client_ip VARCHAR,
  user_agent VARCHAR,
  user_id VARCHAR,
  room_id VARCHAR,
  video_bps BIGINT,
  video_fps BIGINT,
  video_rtt BIGINT,
  video_lost_pps BIGINT,
  video_longest_freeze_duration BIGINT,
  video_total_freeze_duration BIGINT,
  report_timestamp TIMESTAMPTZ,
  country VARCHAR
)
INCLUDE partition
INCLUDE offset
INCLUDE payload
WITH
  (
    connector = 'nats',
    server_url = 'nats-server:4222',
    subject = 'live_stream_metrics',
    allow_create_stream = 'true',
    stream = 'risingwave',
    connect_mode = 'plain',
    consumer.durable_name = 'consumer1'
  ) FORMAT PLAIN ENCODE JSON;

-- Second source: consumes from the SAME subject but with a DIFFERENT consumer name
-- This tests that the shared NATS client can handle multiple concurrent consumers
-- on the same stream while maintaining data accuracy. Both sources connect to the
-- same NATS server and should reuse the same underlying client connection.
CREATE TABLE live_stream_metrics_2 (
  client_ip VARCHAR,
  user_agent VARCHAR,
  user_id VARCHAR,
  room_id VARCHAR,
  video_bps BIGINT,
  video_fps BIGINT,
  video_rtt BIGINT,
  video_lost_pps BIGINT,
  video_longest_freeze_duration BIGINT,
  video_total_freeze_duration BIGINT,
  report_timestamp TIMESTAMPTZ,
  country VARCHAR
)
INCLUDE partition
INCLUDE offset
INCLUDE payload
WITH
  (
    connector = 'nats',
    server_url = 'nats-server:4222',
    subject = 'live_stream_metrics',
    allow_create_stream = 'true',
    stream = 'risingwave',
    connect_mode = 'plain',
    consumer.durable_name = 'consumer2'
  ) FORMAT PLAIN ENCODE JSON;
