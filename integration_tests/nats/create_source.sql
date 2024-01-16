CREATE TABLE personnel (id integer, name varchar,);

CREATE TABLE nats_source_table
(
  id integer,
  name varchar,
)
WITH (
  connector='nats',
    server_url='nats-server:4222',
    subject='subject1',
    stream = 'my_stream',
    connect_mode='plain'
) FORMAT PLAIN ENCODE JSON;

CREATE SINK nats_sink
FROM
  personnel WITH (
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
  (2, 'Bob');

INSERT INTO
  personnel
VALUES
  (3, 'Tom'),
  (4, 'Jerry');

INSERT INTO
  personnel
VALUES
  (5, 'Araminta'),
  (6, 'Clover');

INSERT INTO
  personnel
VALUES
  (7, 'Posey'),
  (8, 'Waverly');

FLUSH;