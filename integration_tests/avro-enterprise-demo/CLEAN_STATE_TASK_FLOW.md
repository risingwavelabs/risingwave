# Avro Enterprise Demo — Clean-State Task Flow

This document covers the full demo/test flow, using **direct bash commands and direct SQL execution** rather than wrapper scripts.

It assumes:
- the demo repository is available locally,
- Docker Compose is available,
- the demo services are exposed on local ports,
- **each task starts from a clean state**.

For each task below, you will see:
1. the customer-facing feature(s) covered,
2. the exact commands to run,
3. the expected result,
4. the evidence to inspect.

---

## Local Endpoints

- RisingWave SQL: `localhost:14566`
- Grafana: `http://localhost:3001`
- Prometheus: `http://localhost:9500`
- Redpanda broker: `localhost:39093`
- Schema Registry: `http://localhost:38081`
- MinIO: `http://localhost:9400`
- Source Postgres: `localhost:8433`
- Push receiver: `http://localhost:18080/endpoint`
- Redpanda metrics: `http://localhost:39644`

---

## Common Clean-State Bootstrap

Run this block **before every task**.

```bash
cd integration_tests/avro-enterprise-demo

docker compose down --remove-orphans

docker compose up -d --build --force-recreate

docker compose run --rm demo-tools "for i in \$(seq 1 60); do psql -h risingwave-standalone -p 4566 -U root -d dev -c 'SELECT 1' >/dev/null 2>&1 && exit 0; sleep 2; done; exit 1"

docker compose run --rm demo-tools "for i in \$(seq 1 30); do pg_isready -h source-postgres -p 5432 -U postgres -d customerdb && exit 0; sleep 2; done; exit 1"

docker compose run --rm demo-tools "PGPASSWORD=postgres psql -h source-postgres -p 5432 -U postgres -d customerdb -v ON_ERROR_STOP=1 -f /workspace/sql/source_postgres_init.sql -P pager=off"

docker compose run --rm demo-tools "python /workspace/scripts/produce_avro.py --step setup"
docker compose run --rm demo-tools "python /workspace/scripts/produce_avro.py --step stage1"
```

What this does:
- resets the demo stack,
- waits for RisingWave and source Postgres,
- initializes the source Postgres table/publication,
- prepares the Kafka topics and initial Avro dataset.

---

# Task 1 — Kafka Avro Source Creation

## Features Covered
- Kafka Avro source creation
- Topic with Avro key/value
- Auto schema decode from registry
- Avro key used as primary key

## Commands

Run the **Common Clean-State Bootstrap** first, then:

```bash
psql -h localhost -p 14566 -U root -d dev -f create_source.sql
```

Optional proof command:

```bash
psql -h localhost -p 14566 -U root -d dev -c "SHOW CREATE TABLE demo_core.customer_profiles;"
```

## Relevant SQL
Source definition from `create_source.sql`:

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

## Expected Result
You should see:
- the source table created successfully,
- Avro key/value source configuration in `SHOW CREATE TABLE`,
- decoded source columns available in the source schema,
- the primary key bound to the Avro key.

---

# Task 2 — Materialized View Creation in DEV

## Features Covered
- Materialized view creation in DEV
- Filters / projections / joins / windowing
- SQL UDFs

## Commands

Run the **Common Clean-State Bootstrap** first, then:

```bash
psql -h localhost -p 14566 -U root -d dev -f create_source.sql
psql -h localhost -p 14566 -U root -d dev -f create_mv.sql
```

Optional proof commands:

```bash
psql -h localhost -p 14566 -U root -d dev -c "SHOW CREATE MATERIALIZED VIEW demo_ops.customer_profile_current;"
psql -h localhost -p 14566 -U root -d dev -c "SHOW CREATE MATERIALIZED VIEW demo_marketing.customer_analytics_dev;"
psql -h localhost -p 14566 -U root -d dev -c "SELECT name FROM rw_catalog.rw_functions WHERE name = 'normalize_email';"
```

## Relevant SQL
Examples from `create_mv.sql`:

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

## Expected Result
You should see:
- the SQL UDF created,
- the latest-state MV created,
- the analytics MV created,
- a join/window/projection/UDF path ready for querying.

---

# Task 3 — Compacted Updates, Latest State, and Tombstones

## Features Covered
- Compacted topic with updates
- Multiple updates for the same key
- Correct latest-state materialization
- Delete / tombstone handling
- Downstream behavior in the materialized view

## Commands

Run the **Common Clean-State Bootstrap** first, then:

```bash
psql -h localhost -p 14566 -U root -d dev -f create_source.sql
psql -h localhost -p 14566 -U root -d dev -f create_mv.sql
```

Then query the latest-state view:

```bash
psql -h localhost -p 14566 -U root -d dev -c "SELECT customer_key, full_name, status, city, segment, tag_count FROM demo_ops.customer_profile_current ORDER BY customer_key;"
```

Optional delete/tombstone proof:

```bash
docker compose exec -T message_queue rpk topic consume demo.customer.profile.upsert.avro -n 10 -f 'offset=%o partition=%p key=%k value_size=%V\n'
```

## Relevant Input
The initial Avro sequence is loaded from:
- `scripts/produce_avro.py --step stage1`

That sequence includes:
- multiple updates for `c-1001`
- a tombstone for `c-1002`

## Expected Result
You should see:
- only the latest active state for each retained key,
- `c-1002` missing from the latest-state view after the tombstone,
- evidence that the source topic includes a tombstone record (`value_size=0`).

---

# Task 4 — Complex Avro Handling

## Features Covered
- Nested records
- Arrays
- Maps
- Unions
- Enums
- Logical types

## Commands

Run the **Common Clean-State Bootstrap** first, then:

```bash
psql -h localhost -p 14566 -U root -d dev -f create_source.sql
psql -h localhost -p 14566 -U root -d dev -f create_mv.sql
```

Run these proof queries:

```bash
psql -h localhost -p 14566 -U root -d dev -c "SELECT customer_key, status, attributes_map, tags, account_uuid FROM demo_ops.customer_profile_complex ORDER BY customer_key;"
```

```bash
psql -h localhost -p 14566 -U root -d dev -c "SELECT customer_key, city, postal_code, segment, tag_count, preferred_contact_text FROM demo_ops.customer_profile_current ORDER BY customer_key;"
```

## Relevant Schema Elements
The Avro payload in `scripts/produce_avro.py` includes:
- nested `profile.address`
- array `tags`
- map `attributes`
- union `preferred_contact`
- enum `status`
- logical types for timestamp / decimal / uuid

## Expected Result
You should see that complex Avro fields are decoded and still queryable through the materialized views.

---

# Task 5 — Streaming Join and Native SUBSCRIBE

## Features Covered
- Streaming joins
- Native incremental consumption via `SUBSCRIBE`
- UDF output in the joined path

## Commands

Run the **Common Clean-State Bootstrap** first, then:

```bash
psql -h localhost -p 14566 -U root -d dev -f create_source.sql
psql -h localhost -p 14566 -U root -d dev -f create_mv.sql
```

Insert the supporting CDC actions:

```bash
docker compose run --rm demo-tools "python /workspace/scripts/update_cdc_actions.py --scenario valid"
```

Flush RisingWave:

```bash
psql -h localhost -p 14566 -U root -d dev -c "FLUSH;"
```

Run the subscribe proof:

```bash
docker compose run --rm demo-tools "python /workspace/scripts/demo_subscribe.py"
```

Optional query:

```bash
psql -h localhost -p 14566 -U root -d dev -c "SELECT count(*) FROM demo_marketing.customer_analytics_dev;"
```

## Relevant SQL
The joined MV path is defined in `create_mv.sql` through:
- `demo_core.customer_action_window`
- `demo_marketing.customer_analytics_dev`
- `normalize_email(...)`

## Expected Result
You should see:
- joined rows produced in `customer_analytics_dev`,
- native `SUBSCRIBE` receiving an incremental row,
- the join path using the UDF-normalized email output.

Evidence file:

```bash
cat artifacts/subscription/native_subscribe.json
```

---

# Task 6 — JSON-Format Downstream Output

## Features Covered
- JSON-format downstream output
- Downstream behavior in the sink

## Commands

Run the **Common Clean-State Bootstrap** first, then:

```bash
psql -h localhost -p 14566 -U root -d dev -f create_source.sql
psql -h localhost -p 14566 -U root -d dev -f create_mv.sql
psql -h localhost -p 14566 -U root -d dev -f create_sink.sql
```

Wait for sink readiness:

```bash
docker compose run --rm demo-tools "python /workspace/scripts/produce_avro.py --step wait_sink"
psql -h localhost -p 14566 -U root -d dev -c "FLUSH;"
```

Capture downstream JSON state:

```bash
docker compose run --rm demo-tools "python /workspace/scripts/consume_downstream.py"
cat artifacts/downstream/latest_state.json
```

## Relevant SQL
From `create_sink.sql`:

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

## Expected Result
You should see processed output in JSON format at the downstream target, reflected in `artifacts/downstream/latest_state.json`.

---

# Task 7 — Schema Evolution: Add Optional Field

## Features Covered
- Add optional field
- Source continuity
- View continuity
- State preservation

## Commands

Run the **Common Clean-State Bootstrap** first, then:

```bash
psql -h localhost -p 14566 -U root -d dev -f create_source.sql
psql -h localhost -p 14566 -U root -d dev -f create_mv.sql
psql -h localhost -p 14566 -U root -d dev -f create_sink.sql
```

Publish schema v2 and refresh the source schema:

```bash
docker compose run --rm demo-tools "python /workspace/scripts/produce_avro.py --step schema_v2"
psql -h localhost -p 14566 -U root -d dev -f sql/refresh_add_optional_field.sql
```

Publish add-field data and flush:

```bash
docker compose run --rm demo-tools "python /workspace/scripts/produce_avro.py --step stage2"
psql -h localhost -p 14566 -U root -d dev -c "FLUSH;"
```

Check the schema:

```bash
psql -h localhost -p 14566 -U root -d dev -c "SELECT column_name FROM information_schema.columns WHERE table_schema='demo_core' AND table_name='customer_profiles' ORDER BY ordinal_position;"
```

Check latest-state output again:

```bash
docker compose run --rm demo-tools "python /workspace/scripts/consume_downstream.py"
cat artifacts/downstream/latest_state.json
```

## Expected Result
You should see:
- `vip_note` added to the source schema,
- source and views still queryable,
- latest-state output still readable after the add-field evolution.

---

# Task 8 — Alternative Source Ingestion via Postgres CDC

## Features Covered
- Alternative source ingestion

## Commands

Run the **Common Clean-State Bootstrap** first, then:

```bash
psql -h localhost -p 14566 -U root -d dev -f create_source.sql
psql -h localhost -p 14566 -U root -d dev -f create_mv.sql
```

Insert a dedicated CDC proof row directly into source Postgres:

```bash
docker compose exec -T source-postgres psql -U postgres -d customerdb -c "INSERT INTO public.customer_actions (action_id, customer_key, action_type, amount, action_ts, channel, team) VALUES (900001, 'c-1001', 'cdc_branch_demo', '1.23', '2026-04-07 09:25:00', 'cdc-branch', 'ops');"
```

Flush RisingWave and query the downstream audit view:

```bash
psql -h localhost -p 14566 -U root -d dev -c "FLUSH;"
psql -h localhost -p 14566 -U root -d dev -c "SELECT count(*) FROM demo_core.customer_change_audit WHERE channel='cdc-branch';"
```

## Expected Result
You should see at least one downstream row for channel `cdc-branch`, proving that the Postgres CDC source is functioning.

---

# Task 9 — Restricted Access by Team (RBAC)

## Features Covered
- Restricted access to sources/views by team

## Commands

Run the **Common Clean-State Bootstrap** first, then:

```bash
psql -h localhost -p 14566 -U root -d dev -f create_source.sql
psql -h localhost -p 14566 -U root -d dev -f create_mv.sql
```

Check allowed view access:

```bash
PGPASSWORD=marketing123 psql -h localhost -p 14566 -U marketing_user -d dev -c "SELECT count(*) FROM demo_marketing.customer_analytics_dev;"
```

Check denied raw-source access:

```bash
PGPASSWORD=marketing123 psql -h localhost -p 14566 -U marketing_user -d dev -c "SELECT count(*) FROM demo_core.customer_profiles;"
```

## Expected Result
You should see:
- the marketing user can read the marketing-facing view,
- the same user is denied access to the raw source table.

---

# Task 10 — Observability

## Features Covered
- Metrics
- Logs
- Offset visibility
- Failure diagnostics

## Commands

Run the **Common Clean-State Bootstrap** first, then:

```bash
psql -h localhost -p 14566 -U root -d dev -f create_source.sql
psql -h localhost -p 14566 -U root -d dev -f create_mv.sql
psql -h localhost -p 14566 -U root -d dev -f create_sink.sql
```

Run these checks:

```bash
curl --get --data-urlencode 'query=up{job="prometheus"}' 'http://localhost:9500/api/v1/query'
```

```bash
docker compose exec -T message_queue rpk topic list
```

```bash
docker compose logs risingwave-standalone --tail 50
```

## Expected Result
You should see:
- a successful Prometheus response,
- Kafka topics visible through `rpk`,
- current RisingWave logs available for diagnostics.

---

# Task 11 — Source Failure Handling

## Features Covered
- Source failure handling

## Commands

Run the **Common Clean-State Bootstrap** first, then:

```bash
psql -h localhost -p 14566 -U root -d dev -f create_source.sql
psql -h localhost -p 14566 -U root -d dev -f create_mv.sql
```

Stop and start the source Postgres service:

```bash
docker compose stop source-postgres
sleep 8
docker compose start source-postgres
sleep 8
```

Insert a recovery marker event:

```bash
docker compose run --rm demo-tools "python /workspace/scripts/update_cdc_actions.py --scenario source_recovery"
psql -h localhost -p 14566 -U root -d dev -c "FLUSH;"
```

Check the audit view:

```bash
psql -h localhost -p 14566 -U root -d dev -c "SELECT count(*) FROM demo_core.customer_change_audit WHERE channel='ops-console';"
```

## Expected Result
You should see at least one recovered row with channel `ops-console`, showing that ingestion resumed after the source restart.

---

# Task 12 — Downstream Delivery Failure Handling

## Features Covered
- Downstream delivery failure handling

## Commands

Run the **Common Clean-State Bootstrap** first, then:

```bash
psql -h localhost -p 14566 -U root -d dev -f create_source.sql
psql -h localhost -p 14566 -U root -d dev -f create_mv.sql
psql -h localhost -p 14566 -U root -d dev -f create_sink.sql
```

Trigger the downstream delivery failure scenario:

```bash
docker compose run --rm demo-tools "python /workspace/scripts/update_cdc_actions.py --scenario sink_failure"
sleep 8
psql -h localhost -p 14566 -U root -d dev -c "FLUSH;"
```

Inspect the downstream receiver evidence:

```bash
cat artifacts/http-receiver/error.log
cat artifacts/http-receiver/received.jsonl
```

## Expected Result
You should see:
- an initial forced failure recorded in `error.log`,
- the same customer key later appearing successfully in `received.jsonl`.

This proves downstream delivery failure surfaced first and then recovered.

---

# Task 13 — Poison Message Behavior

## Features Covered
- Poison-message behavior

## Commands

Run the **Common Clean-State Bootstrap** first, then:

```bash
psql -h localhost -p 14566 -U root -d dev -f create_source.sql
psql -h localhost -p 14566 -U root -d dev -f create_mv.sql
psql -h localhost -p 14566 -U root -d dev -f create_sink.sql
```

Capture baseline downstream state:

```bash
docker compose run --rm demo-tools "python /workspace/scripts/consume_downstream.py"
cp artifacts/downstream/latest_state.json artifacts/poison-kafka/latest_state_before.json
curl -sS http://localhost:21250/metrics | rg '^user_source_error_cnt' > artifacts/poison-kafka/metric_before.prom
```

Inject the poison payload and a valid follow-up record:

```bash
docker compose run --rm demo-tools "python /workspace/scripts/produce_poison_kafka.py"
```

Wait, then collect evidence:

```bash
sleep 15
curl -sS http://localhost:21250/metrics | rg '^user_source_error_cnt' > artifacts/poison-kafka/metric_after.prom
docker compose logs risingwave-standalone --tail 400 > artifacts/poison-kafka/risingwave.log
docker compose run --rm demo-tools "python /workspace/scripts/consume_downstream.py"
cp artifacts/downstream/latest_state.json artifacts/poison-kafka/latest_state_after.json
cat artifacts/poison-kafka/produce-result.json
```

## Expected Result
You should see:
- source-error telemetry/logging for the malformed payload,
- the malformed payload skipped,
- the valid follow-up record still materialized downstream.

Primary artifact to inspect:

```bash
cat artifacts/poison-kafka/poison-kafka-check.md
```

---

# Task 14 — Full Integrated Rehearsal

## Features Covered
- Full end-to-end retained demo flow

## Commands

Run once from clean state:

```bash
cd integration_tests/avro-enterprise-demo
docker compose down --remove-orphans
time ./scripts/run_demo.sh
```

## Expected Result
You should see a successful integrated run covering:
1. canonical backbone
2. schema evolution branch
3. CDC branch
4. RBAC branch
5. observability branch
6. failure branch
7. poison-message branch

Primary evidence:

```bash
cat artifacts/integrated-rehearsal-status.md
```

Expected content:
- `Result: PASS`
- the latest wall-clock time
- the list of covered segments

---

# Narrated-Only Items (No Live Commands)

These are not part of the live task flow:

## CI/CD Promotion
Say only:
- the MV definition is kept as code,
- it fits a definition-as-code / dbt-compatible automation model,
- no live higher-environment promotion is shown in this session.

## AD Group Mapping
Say only:
- AD group-to-RBAC mapping is not demonstrated live,
- it remains a workaround / future-path topic.

## Fixed-Width Output
Say only:
- fixed-width output was requested originally,
- it is intentionally deferred from the current demo scope.

---

# Final Quick Check

Before a real customer-facing run, verify these files exist and look correct:

```bash
cat artifacts/backbone-status.md
cat artifacts/schema-branch-status.md
cat artifacts/cdc-branch-status.md
cat artifacts/rbac-branch-status.md
cat artifacts/observability-branch-status.md
cat artifacts/failure-branch-status.md
cat artifacts/poison-kafka/poison-kafka-check.md
cat artifacts/integrated-rehearsal-status.md
```

If all of them look correct, the local demo package is ready for a scoped customer presentation.
