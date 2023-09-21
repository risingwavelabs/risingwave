CREATE TABLE test
(
  id integer,
  name varchar,
)
WITH (
  connector='nats',
    server_url='nats-server:4222',
    subject='subject2',
    connect_mode='plain'
) FORMAT PLAIN ENCODE JSON;
