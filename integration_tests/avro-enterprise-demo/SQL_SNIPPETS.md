# SQL Snippets — Avro Enterprise Demo

这份文件提供 step cards 引用的 SQL 片段。

> Presenter/reference note:
> - 这份文件用于讲解与引用，不展示 demo 凭据原值。
> - 所有 demo 凭据/secret 在这里都使用 redacted placeholder。

---

## SQL-B1-KAFKA-SOURCE-DDL
来源：`create_source.sql`

```sql
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
```

Proof query:

```sql
SHOW CREATE TABLE demo_core.customer_profiles;
```

---

## SQL-C1-CDC-SOURCE-DDL
来源：`create_source.sql`

```sql
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
  password = '<demo-password-redacted>',
  database.name = 'customerdb',
  schema.name = 'public',
  table.name = 'customer_actions',
  slot.name = 'demo_customer_actions'
);
```

---

## SQL-B2-MV-DDL
来源：`create_mv.sql`

```sql
CREATE FUNCTION normalize_email(input varchar)
RETURNS varchar
LANGUAGE SQL
AS $$ select lower(input) $$;
```

```sql
CREATE MATERIALIZED VIEW demo_ops.customer_profile_current AS
SELECT
  customer_id AS customer_key,
  full_name,
  normalize_email(email) AS email_normalized,
  status,
  balance,
  (profile).address.city AS city,
  (profile).address.postal_code AS postal_code,
  (profile).attributes['segment'] AS segment,
  array_length((profile).tags) AS tag_count,
  CASE
    WHEN (preferred_contact)."string" IS NOT NULL THEN (preferred_contact)."string"
    WHEN (preferred_contact)."int" IS NOT NULL THEN CAST((preferred_contact)."int" AS varchar)
    ELSE NULL
  END AS preferred_contact_text,
  account_uuid,
  event_ts
FROM demo_core.customer_profiles
WHERE status <> 'INACTIVE';
```

```sql
CREATE MATERIALIZED VIEW demo_marketing.customer_analytics_dev AS
SELECT
  windowed.customer_key,
  profile.full_name,
  profile.email_normalized,
  profile.segment,
  profile.city,
  windowed.window_start,
  windowed.window_end,
  windowed.action_count,
  windowed.gross_amount
FROM demo_core.customer_action_window AS windowed
JOIN demo_ops.customer_profile_current AS profile
  ON windowed.customer_key = profile.customer_key
WHERE windowed.action_count > 0;
```

---

## SQL-B3-LATEST-STATE-QUERIES
```sql
SELECT customer_key, full_name, status, city, segment, tag_count
FROM demo_ops.customer_profile_current
ORDER BY customer_key;
```

```sql
SELECT customer_key, status, attributes_map, tags
FROM demo_ops.customer_profile_complex
ORDER BY customer_key;
```

---

## SQL-B4-SUBSCRIBE-PROOF
主流程由 `scripts/demo_subscribe.py` 驱动，对应 SQL 逻辑是：

```sql
CREATE SUBSCRIPTION customer_change_audit_sub
FROM customer_change_audit WITH (retention = '1 hour');
```

```sql
DECLARE customer_change_audit_cur SUBSCRIPTION CURSOR
FOR customer_change_audit_sub SINCE NOW();
```

```sql
FETCH NEXT FROM customer_change_audit_cur;
```

---

## SQL-B5-SINK-DDL
来源：`create_sink.sql`

```sql
CREATE SINK demo_profile_latest_sink
FROM demo_ops.customer_profile_current
WITH (
  connector = 'kafka',
  properties.bootstrap.server = 'message_queue:29092',
  topic = 'demo.customer.profile.latest',
  primary_key = 'customer_key'
)
FORMAT UPSERT ENCODE JSON;
```

```sql
CREATE SINK demo_change_file_sink
FROM demo_core.customer_change_audit
WITH (
  connector = 's3',
  s3.region_name = 'custom',
  s3.bucket_name = 'hummock001',
  s3.credentials.access = '<demo-access-redacted>',
  s3.credentials.secret = '<demo-secret-redacted>',
  s3.endpoint_url = 'http://minio-0:9301',
  s3.path = 'demo/customer_change_json/',
  type = 'append-only',
  force_append_only = 'true'
)
FORMAT PLAIN ENCODE JSON(force_append_only = 'true');
```

---

## SQL-S1-REFRESH-SCHEMA
来源：`sql/refresh_add_optional_field.sql`

```sql
ALTER TABLE demo_core.customer_profiles REFRESH SCHEMA;
```

Proof query:

```sql
SELECT column_name
FROM information_schema.columns
WHERE table_schema = 'demo_core'
  AND table_name = 'customer_profiles'
ORDER BY ordinal_position;
```

---

## SQL-C1-CDC-PROOF
Proof query:

```sql
SELECT count(*)
FROM demo_core.customer_change_audit
WHERE channel = 'cdc-branch';
```

---

## SQL-R1-RBAC-CHECK
Allowed query:

```sql
SELECT count(*) FROM demo_marketing.customer_analytics_dev;
```

Denied query:

```sql
SELECT count(*) FROM demo_core.customer_profiles;
```

---

## SQL-F1-SOURCE-RECOVERY
Proof query:

```sql
SELECT count(*)
FROM demo_core.customer_change_audit
WHERE channel = 'ops-console';
```
