
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
