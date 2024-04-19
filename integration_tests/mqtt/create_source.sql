CREATE TABLE
  personnel (id integer, name varchar);

CREATE TABLE mqtt_source_table
(
  id integer,
  name varchar,
)
WITH (
    connector='mqtt',
    url='tcp://mqtt-server',
    topic= 'test',
    qos = 'at_least_once',
) FORMAT PLAIN ENCODE JSON;
