CREATE TABLE items
(
  PRIMARY KEY(id)
)
WITH (
  connector='kafka',
  properties.bootstrap.server = 'message_queue:29092',
  topic='ua1'
) ROW FORMAT UPSERT_AVRO (schema.registry = 'http://message_queue:8081');
