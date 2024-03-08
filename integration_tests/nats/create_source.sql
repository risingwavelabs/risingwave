CREATE TABLE
  personnel (id integer, name varchar);


CREATE TABLE
  nats_source_table (id integer, name varchar)
WITH
  (
    connector = 'nats',
    server_url = 'nats-server:4222',
    subject = 'subject1',
    stream = 'my_stream',
    connect_mode = 'plain'
  ) FORMAT PLAIN ENCODE JSON;


CREATE SINK nats_sink
FROM
  personnel
WITH
  (
    connector = 'nats',
    server_url = 'nats-server:4222',
    subject = 'subject1',
    type = 'append-only',
    force_append_only = 'true',
    connect_mode = 'plain'
  );


INSERT INTO
  personnel
VALUES
  (1, 'Alice'),
  (2, 'Bob'),
  (3, 'Tom'),
  (4, 'Jerry'),
  (5, 'Araminta'),
  (6, 'Clover'),
  (7, 'Posey'),
  (8, 'Waverly');


FLUSH;


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
WITH
  (
    connector = 'nats',
    server_url = 'nats-server:4222',
    subject = 'live_stream_metrics',
    stream = 'risingwave',
    connect_mode = 'plain'
  ) FORMAT PLAIN ENCODE JSON;