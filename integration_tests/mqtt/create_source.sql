CREATE TABLE
  personnel (id integer, name varchar);

CREATE TABLE mqtt_source_table
(
  id integer,
  name varchar,
)
WITH (
    connector='mqtt',
    host='mqtt-server',
    topic= 'test'
) FORMAT PLAIN ENCODE JSON;


CREATE SINK mqtt_sink
FROM
  personnel
WITH
  (
    connector='mqtt',
    host='mqtt-server',
    topic= 'test',
    type = 'append-only',
    force_append_only='true',
    retain = 'true',
    qos = '1'
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