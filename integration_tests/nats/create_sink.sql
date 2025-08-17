set sink_decouple = false;

CREATE TABLE
  personnel (id integer, name varchar);


CREATE SINK nats_sink
FROM
  personnel
WITH
  (
    connector = 'nats',
    server_url = 'nats-server:4222',
    subject = 'subject1',
    type = 'append-only',
    allow_create_stream = 'true',
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

CREATE SINK nats_sink_all_types_sink
FROM
  nats_sink_all_types
WITH
  (
    connector = 'nats',
    server_url = 'nats-server:4222',
    subject = 'subject2',
    allow_create_stream = 'true',
    type = 'append-only',
    force_append_only = 'true',
    connect_mode = 'plain'
  );
