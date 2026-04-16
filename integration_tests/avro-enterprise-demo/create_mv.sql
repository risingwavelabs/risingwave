SET SEARCH_PATH TO public;

DROP FUNCTION IF EXISTS normalize_email(varchar);
CREATE FUNCTION normalize_email(input varchar)
RETURNS varchar
LANGUAGE SQL
AS $$ select lower(input) $$;

DROP MATERIALIZED VIEW IF EXISTS demo_core.customer_change_audit;
DROP MATERIALIZED VIEW IF EXISTS demo_marketing.customer_analytics_dev;
DROP MATERIALIZED VIEW IF EXISTS demo_core.customer_action_window;
DROP MATERIALIZED VIEW IF EXISTS demo_ops.customer_profile_complex;
DROP MATERIALIZED VIEW IF EXISTS demo_ops.customer_profile_current;

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

CREATE MATERIALIZED VIEW demo_ops.customer_profile_complex AS
SELECT
  customer_id AS customer_key,
  status,
  balance,
  (profile).attributes AS attributes_map,
  (profile).tags AS tags,
  account_uuid
FROM demo_core.customer_profiles;

CREATE MATERIALIZED VIEW demo_core.customer_action_window AS
SELECT
  customer_key,
  window_start,
  window_end,
  count(*) AS action_count,
  sum(amount) AS gross_amount
FROM tumble(demo_core.customer_actions_cdc, action_ts, INTERVAL '10 minute')
GROUP BY customer_key, window_start, window_end;

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

CREATE MATERIALIZED VIEW demo_core.customer_change_audit AS
SELECT
  action.customer_key,
  action.action_type,
  action.amount,
  action.action_ts,
  action.channel,
  profile.full_name,
  profile.status,
  profile.segment,
  profile.city
FROM demo_core.customer_actions_cdc AS action
JOIN demo_ops.customer_profile_current AS profile
  ON action.customer_key = profile.customer_key;

DROP USER IF EXISTS marketing_user;
DROP USER IF EXISTS ops_user;

CREATE USER marketing_user WITH PASSWORD 'marketing123';
CREATE USER ops_user WITH PASSWORD 'ops123';

GRANT USAGE ON SCHEMA demo_marketing TO marketing_user;
GRANT SELECT ON ALL MATERIALIZED VIEWS IN SCHEMA demo_marketing TO marketing_user;
GRANT USAGE ON SCHEMA demo_ops TO ops_user;
GRANT SELECT ON ALL MATERIALIZED VIEWS IN SCHEMA demo_ops TO ops_user;
