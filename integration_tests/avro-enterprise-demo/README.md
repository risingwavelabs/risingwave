# Avro Enterprise Compose Demo

> This is a local demo environment. Default credentials and published host ports are for demonstration convenience only and are not production deployment recommendations.

This demo packages a single-node RisingWave stack with Redpanda, Schema Registry, MinIO, Prometheus, Grafana, a small HTTP receiver, and a Postgres CDC source to demonstrate a scoped enterprise demo workflow.

## Scope frozen for this demo

### Included live scope
- Kafka Avro source creation with Avro key/value
- Automatic schema decode from Schema Registry
- Avro key used as primary key
- Compacted-topic updates and latest-state materialization
- Tombstone handling
- Complex Avro handling
- Streaming joins, windowing, and SQL UDFs
- Native SQL `SUBSCRIBE` for incremental consumption
- Add-optional-field schema evolution
- Materialized view creation in DEV via SQL console
- JSON-format downstream output
- Alternative source ingestion via Postgres CDC
- Restricted access to sources/views by team
- Observability
- Source failure handling
- Downstream delivery failure handling
- Poison-message behavior

### Out of scope
- Remove optional field
- AD group to RBAC mapping as a real integration
- Real higher-environment promotion during the live demo
- CSV output
- XML output
- Fixed-width output

### Required remark
Fixed-width output appeared in the original customer request but is intentionally deferred from the current demo scope.

## Recommended scripts

### Canonical backbone
```bash
cd integration_tests/avro-enterprise-demo
./scripts/run_backbone.sh
```

Writes:
- `artifacts/backbone-status.md`
- `artifacts/downstream/latest_state.json`
- `artifacts/subscription/native_subscribe.json`

Note:
- The backbone uses a minimal CDC fixture internally to support the join / `SUBSCRIBE` proof path.
- The formal customer-facing proof for **alternative source ingestion** is still the dedicated CDC branch below.

### Schema-evolution branch
```bash
./scripts/run_schema_branch.sh
```

Writes:
- `artifacts/schema-branch-status.md`

### CDC branch
```bash
./scripts/run_cdc_branch.sh
```

Writes:
- `artifacts/cdc-branch-status.md`

### RBAC branch
```bash
./scripts/run_rbac_branch.sh
```

Writes:
- `artifacts/rbac-branch-status.md`

### Observability branch
```bash
./scripts/run_observability_branch.sh
```

Writes:
- `artifacts/observability-branch-status.md`

### Failure branches
```bash
./scripts/run_failure_branches.sh
```

Writes:
- `artifacts/failure-branch-status.md`
- `artifacts/http-receiver/error.log`
- `artifacts/http-receiver/received.jsonl`

### Poison-message branch
```bash
./scripts/run_poison_kafka_case.sh
```

Writes:
- `artifacts/poison-kafka/poison-kafka-check.md`
- `artifacts/poison-kafka/risingwave.log`
- `artifacts/poison-kafka/metric_before.prom`
- `artifacts/poison-kafka/metric_after.prom`
- `artifacts/poison-kafka/latest_state_before.json`
- `artifacts/poison-kafka/latest_state_after.json`
- `artifacts/poison-kafka/produce-result.json`

### Full internal rehearsal
```bash
./scripts/run_demo.sh
```

This runs the backbone plus all retained isolated branches in sequence for internal verification.

## Core SQL objects

- `create_source.sql`
  - `demo_core.customer_profiles` is the Kafka upsert Avro table.
  - `demo_core.customer_actions_cdc` is the Postgres CDC table.
- `create_mv.sql`
  - `normalize_email` is the SQL UDF.
  - `demo_ops.customer_profile_current` is the latest-state materialized view.
  - `demo_marketing.customer_analytics_dev` demonstrates filters, projections, joins, and windowing.
- `create_sink.sql`
  - `demo_profile_latest_sink` is the downstream Kafka JSON sink.
  - `demo_change_file_sink` is the JSON object sink to MinIO.

## Customer-safe wording highlights

- Native `SUBSCRIBE` is the retained proof for incremental / push-style consumption in this demo.
- JSON proof means the presenter can visibly show processed output in JSON format at the downstream target.
- Poison-message behavior should be explained as the expected demo behavior and then verified live.
- CI/CD promotion is narrated only as a definition-as-code / dbt-compatible automation path.
- CSV/XML/fixed-width helpers remain out of current demo scope even if historical helper scripts still exist in the repo.

See also:
- `CLEAN_STATE_TASK_FLOW.md`
- `DEMO_FLOW.md`
- `DEMO_STEP_CARDS.md`
- `SQL_SNIPPETS.md`
- `SCHEMA_DIFF_CARDS.md`
- `RUNBOOK.md`
- `CUSTOMER_NOTES.md`
- `.omx/plans/presenter-wording-avro-enterprise-demo.md`

## Host-exposed demo endpoints

- RisingWave SQL: `localhost:14566`
- Grafana: `http://localhost:3001`
- Prometheus: `http://localhost:9500`
- Redpanda broker: `localhost:39093`
- Schema Registry: `http://localhost:38081`
- MinIO: `http://localhost:9400`
- Source Postgres: `localhost:8433`
- Push receiver: `http://localhost:18080/endpoint`
- Redpanda metrics: `http://localhost:39644`

Useful commands:

```bash
docker compose logs risingwave-standalone --tail 200
docker compose logs push-receiver --tail 200
docker compose exec message_queue rpk topic list
docker compose exec message_queue rpk topic consume demo.customer.profile.latest -n 20
curl "http://localhost:9500/api/v1/query?query=up{job='prometheus'}"
```
