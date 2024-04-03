CREATE TABLE
  personnel (id integer, name varchar);

CREATE TABLE hivemq_source_table
(
  id integer,
  name varchar,
)
WITH (
    connector='mqtt',
    url='tcp://hivemq-ce',
    topic= 'test',
    qos = 'at_least_once',
) FORMAT PLAIN ENCODE JSON;
