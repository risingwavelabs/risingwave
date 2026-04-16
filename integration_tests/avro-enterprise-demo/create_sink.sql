DROP SINK IF EXISTS demo_change_file_sink;
DROP SINK IF EXISTS demo_profile_latest_sink;

CREATE SINK demo_profile_latest_sink
FROM demo_ops.customer_profile_current
WITH (
  connector = 'kafka',
  properties.bootstrap.server = 'message_queue:29092',
  topic = 'demo.customer.profile.latest',
  primary_key = 'customer_key'
)
FORMAT UPSERT ENCODE JSON;

CREATE SINK demo_change_file_sink
FROM demo_core.customer_change_audit
WITH (
  connector = 's3',
  s3.region_name = 'custom',
  s3.bucket_name = 'hummock001',
  s3.credentials.access = 'hummockadmin',
  s3.credentials.secret = 'hummockadmin',
  s3.endpoint_url = 'http://minio-0:9301',
  s3.path = 'demo/customer_change_json/',
  type = 'append-only',
  force_append_only = 'true'
)
FORMAT PLAIN ENCODE JSON(force_append_only = 'true');
