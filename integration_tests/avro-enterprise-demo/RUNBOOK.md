# Demo Runbook — Avro Enterprise Demo

这份 runbook 面向 operator，告诉你：
- 按什么顺序跑
- 每一步对应哪个 `step_id`
- 证据去哪看

> 详细讲法、SQL、输入/输出、schema note 请看：
> - `DEMO_STEP_CARDS.md`
> - `SQL_SNIPPETS.md`
> - `SCHEMA_DIFF_CARDS.md`

---

## 1. Opening scope card

### In scope
- Kafka Avro source creation
- latest-state materialization and tombstones
- complex Avro
- joins / windowing / UDF
- native `SUBSCRIBE`
- JSON-format downstream output
- add-optional-field schema evolution
- Postgres CDC
- RBAC restrictions
- observability
- source failure handling
- downstream delivery failure handling
- poison-message behavior

### Out of scope
- remove optional field
- AD group mapping
- real higher-environment promotion
- CSV / XML / fixed-width outputs

### Deferral remark
Fixed-width output was in the original customer request and is intentionally deferred from this demo scope.

---

## 2. Canonical backbone

### Command
```bash
./scripts/run_backbone.sh
```

### Steps covered
- `B1-source-create`
- `B2-mv-create`
- `B3-updates-tombstones`
- `B4-join-subscribe`
- `B5-json-output`

### Evidence
- `artifacts/backbone-status.md`
- `artifacts/downstream/latest_state.json`
- `artifacts/subscription/native_subscribe.json`

---

## 3. Isolated branches

### Branch A — Schema evolution
- Step ID: `S1-add-optional-field`
- Command:
  ```bash
  ./scripts/run_schema_branch.sh
  ```
- Evidence:
  - `artifacts/schema-branch-status.md`

### Branch B — Postgres CDC
- Step ID: `C1-postgres-cdc`
- Command:
  ```bash
  ./scripts/run_cdc_branch.sh
  ```
- Evidence:
  - `artifacts/cdc-branch-status.md`

### Branch C — RBAC allow / deny
- Step ID: `R1-rbac-allow-deny`
- Command:
  ```bash
  ./scripts/run_rbac_branch.sh
  ```
- Evidence:
  - `artifacts/rbac-branch-status.md`

### Branch D — Observability
- Step ID: `O1-observability-signals`
- Command:
  ```bash
  ./scripts/run_observability_branch.sh
  ```
- Evidence:
  - `artifacts/observability-branch-status.md`

### Branch E — Source and downstream delivery failures
- Step IDs:
  - `F1-source-recovery`
  - `F2-downstream-delivery-retry`
- Command:
  ```bash
  ./scripts/run_failure_branches.sh
  ```
- Evidence:
  - `artifacts/failure-branch-status.md`
  - `artifacts/http-receiver/error.log`
  - `artifacts/http-receiver/received.jsonl`

### Branch F — Poison-message behavior
- Step ID: `P1-poison-message`
- Command:
  ```bash
  ./scripts/run_poison_kafka_case.sh
  ```
- Evidence:
  - `artifacts/poison-kafka/poison-kafka-check.md`

---

## 4. Narrated-only boundaries
- CI/CD promotion is definition-as-code / dbt-compatible automation path only.
- AD mapping is workaround / future path only if needed.

---

## 5. Full integrated rehearsal
```bash
./scripts/run_demo.sh
```

Evidence:
- `artifacts/integrated-rehearsal-status.md`
