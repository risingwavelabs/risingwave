CREATE TABLE items
(
  PRIMARY KEY(id)
)
WITH (
  connector='kafka',
  properties.bootstrap.server = 'message_queue:29092',
  topic='ua1'
) FORMAT UPSERT ENCODE AVRO (schema.registry = 'http://message_queue:8081');
