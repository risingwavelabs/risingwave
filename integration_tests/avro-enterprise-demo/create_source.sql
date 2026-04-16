SET sink_decouple = true;

DROP SINK IF EXISTS demo_change_file_sink;
DROP SINK IF EXISTS demo_profile_push_sink;
DROP SINK IF EXISTS demo_profile_latest_sink;

DROP MATERIALIZED VIEW IF EXISTS demo_core.customer_change_audit;
DROP MATERIALIZED VIEW IF EXISTS demo_marketing.customer_analytics_dev;
DROP MATERIALIZED VIEW IF EXISTS demo_core.customer_action_window;
DROP MATERIALIZED VIEW IF EXISTS demo_core.customer_actions_parsed;
DROP MATERIALIZED VIEW IF EXISTS demo_ops.customer_profile_complex;
DROP MATERIALIZED VIEW IF EXISTS demo_ops.customer_profile_current;

CREATE SCHEMA IF NOT EXISTS demo_core;
CREATE SCHEMA IF NOT EXISTS demo_ops;
CREATE SCHEMA IF NOT EXISTS demo_marketing;

SET SEARCH_PATH TO demo_core, public;

DROP TABLE IF EXISTS customer_profiles;
DROP TABLE IF EXISTS customer_actions_cdc;

CREATE TABLE customer_profiles (
  PRIMARY KEY (rw_key)
)
INCLUDE KEY AS rw_key
WITH (
  connector = 'kafka',
  properties.bootstrap.server = 'message_queue:29092',
  topic = 'demo.customer.profile.upsert.avro'
)
FORMAT UPSERT ENCODE AVRO (
  schema.registry = 'http://message_queue:8081'
);

CREATE TABLE customer_actions_cdc (
  action_id bigint,
  customer_key varchar,
  action_type varchar,
  amount numeric,
  action_ts timestamp,
  channel varchar,
  team varchar,
  PRIMARY KEY (action_id)
) WITH (
  connector = 'postgres-cdc',
  hostname = 'source-postgres',
  port = '5432',
  username = 'postgres',
  password = 'postgres',
  database.name = 'customerdb',
  schema.name = 'public',
  table.name = 'customer_actions',
  slot.name = 'demo_customer_actions'
);
