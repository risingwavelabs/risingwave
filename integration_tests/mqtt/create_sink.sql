set sink_decouple = false;

CREATE SINK mqtt_sink
FROM
  personnel
WITH
(
    connector='mqtt',
    url='tcp://mqtt-server',
    topic= 'test',
    type = 'append-only',
    retain = 'true',
    qos = 'at_least_once',
) FORMAT PLAIN ENCODE JSON (
    force_append_only='true',
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