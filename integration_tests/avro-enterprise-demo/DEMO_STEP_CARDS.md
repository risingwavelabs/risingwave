# Demo Step Cards — Avro Enterprise Demo

这份文件是当前 demo 的**单一 step registry**。  
每一个 retained demo action 都有唯一的 `step_id`，并绑定：

- feature checklist item
- expected proof
- command / SQL ref
- input ref
- output ref
- schema note / schema diff ref
- talking points
- caveat / scope boundary

> 规则：
> - 现场讲解时，优先按 `step_id` 组织叙述
> - 脚本 header、runbook、artifact 都应能回指到相同 `step_id`
> - 没有 SQL 的动作允许 `SQL_REF: N/A`
> - 没有 schema 变化的动作允许 `SCHEMA_NOTE: no schema change`

---

## Backbone

### B1-source-create
- **Feature checklist item**
  - Kafka Avro source creation
  - Topic with Avro key/value
  - Auto schema decode from registry
  - Avro key used as primary key
- **Expected proof**
  - `customer_profiles` source table created successfully
  - source schema contains decoded structured columns
  - primary key is bound to Avro key
- **Command / SQL ref**
  - Command: `./scripts/run_backbone.sh`
  - SQL ref: `SQL-B1-KAFKA-SOURCE-DDL`
- **Input ref**
  - `prepare.sh`
  - `scripts/produce_avro.py --step setup`
  - `scripts/produce_avro.py --step stage1`
- **Output ref**
  - `artifacts/backbone-status.md`
  - SQL console / `SHOW CREATE TABLE demo_core.customer_profiles`
- **Schema note / schema diff ref**
  - `SCHEMA-B1-CUSTOMER-PROFILES`
- **Talking points**
  - 这是 Kafka Avro source
  - key/value 都是 Avro
  - schema 通过 registry 自动解码
  - Avro key 被提升为主键语义
- **Caveat / scope boundary**
  - 这里证明的是 source 建立与 schema decode，不是完整 error-handling

### B2-mv-create
- **Feature checklist item**
  - Materialized view creation in DEV
  - Filters / projections / joins / windowing
  - UDFs
- **Expected proof**
  - MV 和 SQL UDF 成功创建
  - `customer_profile_current` / `customer_analytics_dev` 可查询
- **Command / SQL ref**
  - Command: `./scripts/run_backbone.sh`
  - SQL ref: `SQL-B2-MV-DDL`
- **Input ref**
  - none beyond stack + source readiness
- **Output ref**
  - `artifacts/backbone-status.md`
  - SQL console queries against `demo_ops.customer_profile_current` and `demo_marketing.customer_analytics_dev`
- **Schema note / schema diff ref**
  - `SCHEMA-B2-MV-OBJECTS`
- **Talking points**
  - 这里同时展示 SQL UDF、latest-state MV、join/window MV
  - 这一步不是单纯建表，而是建出后续可视化的主证明面
- **Caveat / scope boundary**
  - 这里只建立 DEV 里的对象，不做跨环境 promotion live demo

### B3-updates-tombstones
- **Feature checklist item**
  - Compacted topic with updates
  - Multiple updates for same key
  - Correct latest-state materialization
  - Delete / tombstone handling
  - Complex Avro handling
- **Expected proof**
  - 同一个 key 多次更新后只保留最新有效状态
  - tombstone 删除后 key 从 latest-state 中消失
  - complex Avro 字段仍可查询
- **Command / SQL ref**
  - Command: `./scripts/run_backbone.sh`
  - SQL ref: `SQL-B3-LATEST-STATE-QUERIES`
- **Input ref**
  - `scripts/produce_avro.py --step stage1`
- **Output ref**
  - `artifacts/backbone-status.md`
  - `artifacts/downstream/latest_state.json`
- **Schema note / schema diff ref**
  - `SCHEMA-NO-CHANGE`
- **Talking points**
  - compacted topic 的重点不是“保留所有历史”，而是 latest-state
  - tombstone 行为在 MV 与 downstream proof 上都应可见
  - complex Avro 字段没有因为 decode 而丢失可用性
- **Caveat / scope boundary**
  - 这一步不讲 schema evolution

### B4-join-subscribe
- **Feature checklist item**
  - Stream processing
  - Streaming joins
  - Incremental consumption via native `SUBSCRIBE`
- **Expected proof**
  - join 后的行被增量观察到
  - `SUBSCRIBE` 收到新产生的 joined row
- **Command / SQL ref**
  - Command: `./scripts/run_backbone.sh`
  - SQL ref: `SQL-B4-SUBSCRIBE-PROOF`
- **Input ref**
  - `scripts/update_cdc_actions.py --scenario valid`
  - `scripts/demo_subscribe.py`
- **Output ref**
  - `artifacts/subscription/native_subscribe.json`
  - `artifacts/backbone-status.md`
- **Schema note / schema diff ref**
  - `SCHEMA-NO-CHANGE`
- **Talking points**
  - 本次 demo 中，incremental / push-style expectation 由 native `SUBSCRIBE` 满足
  - 这里展示的是 join 后结果的增量可见性
- **Caveat / scope boundary**
  - 不把 wrapper push bridge 当成 primary customer-facing proof

### B5-json-output
- **Feature checklist item**
  - JSON-format downstream output
  - Downstream behavior in sink
- **Expected proof**
  - processed output 以 JSON format 出现在 downstream target
- **Command / SQL ref**
  - Command: `./scripts/run_backbone.sh`
  - SQL ref: `SQL-B5-SINK-DDL`
- **Input ref**
  - `scripts/consume_downstream.py`
- **Output ref**
  - `artifacts/downstream/latest_state.json`
  - `artifacts/backbone-status.md`
- **Schema note / schema diff ref**
  - `SCHEMA-B5-SINK-OBJECTS`
- **Talking points**
  - 当前 demo 的 acceptance bar 是“看到 downstream JSON output”
  - 不需要额外扩成更重的 sink marketing claim
- **Caveat / scope boundary**
  - CSV / XML / fixed-width 不在当前 live scope

---

## Schema Evolution Branch

### S1-add-optional-field
- **Feature checklist item**
  - Schema evolution: add optional field
  - Verify source/view continuity and state preservation
- **Expected proof**
  - `vip_note` 出现在 source schema 中
  - source / view / latest-state 都继续可读
- **Command / SQL ref**
  - Command: `./scripts/run_schema_branch.sh`
  - SQL ref: `SQL-S1-REFRESH-SCHEMA`
- **Input ref**
  - `scripts/produce_avro.py --step schema_v2`
  - `scripts/produce_avro.py --step stage2`
- **Output ref**
  - `artifacts/schema-branch-status.md`
  - `artifacts/downstream/latest_state.json`
- **Schema note / schema diff ref**
  - `SCHEMA-S1-VIP-NOTE`
- **Talking points**
  - 这里演示的是 add-field，不是 remove-field
  - 重点不是 DDL 本身，而是 continuity + preserved state
- **Caveat / scope boundary**
  - remove optional field 明确不在当前 demo scope

---

## CDC Branch

### C1-postgres-cdc
- **Feature checklist item**
  - Alternative source ingestion
- **Expected proof**
  - Postgres CDC 写入后，downstream audit view 出现匹配记录
- **Command / SQL ref**
  - Command: `./scripts/run_cdc_branch.sh`
  - SQL ref: `SQL-C1-CDC-SOURCE-DDL`, `SQL-C1-CDC-PROOF`
- **Input ref**
  - `source-postgres.public.customer_actions` insert
- **Output ref**
  - `artifacts/cdc-branch-status.md`
- **Schema note / schema diff ref**
  - `SCHEMA-C1-CDC-SOURCE`
- **Talking points**
  - 这是正式的 non-Kafka ingestion proof
  - backbone 内部虽用到最小 CDC 夹具，但对客户讲 alternative source 时正式证据是这一段
- **Caveat / scope boundary**
  - 本次固定为 Postgres CDC，不演示 webhook / 其他 source

---

## RBAC Branch

### R1-rbac-allow-deny
- **Feature checklist item**
  - Restricted access to sources/views by team
- **Expected proof**
  - marketing_user 可以访问允许的 view
  - marketing_user 访问 raw source table 被拒绝
- **Command / SQL ref**
  - Command: `./scripts/run_rbac_branch.sh`
  - SQL ref: `SQL-R1-RBAC-CHECK`
- **Input ref**
  - demo-local users/grants created by `create_mv.sql`
- **Output ref**
  - `artifacts/rbac-branch-status.md`
- **Schema note / schema diff ref**
  - `SCHEMA-R1-RBAC-PRINCIPALS`
- **Talking points**
  - 这一步证明的是 native RBAC / grants
  - 不是 AD group mapping
- **Caveat / scope boundary**
  - AD mapping 是 narrated-only boundary

---

## Observability Branch

### O1-observability-signals
- **Feature checklist item**
  - Observability: metrics / logs / offset visibility / failure diagnostics
- **Expected proof**
  - Prometheus query 成功
  - Kafka topic 可见
  - RisingWave logs 可用于诊断
- **Command / SQL ref**
  - Command: `./scripts/run_observability_branch.sh`
  - SQL ref: `N/A`
- **Input ref**
  - current running stack
- **Output ref**
  - `artifacts/observability-branch-status.md`
- **Schema note / schema diff ref**
  - `SCHEMA_NOTE: no schema change`
- **Talking points**
  - 这一步重点是 operational visibility，不是数据变更
- **Caveat / scope boundary**
  - 观测信号必须来自当前 run，不引用旧历史截图

---

## Failure Branches

### F1-source-recovery
- **Feature checklist item**
  - Source failure handling
- **Expected proof**
  - source 重启后恢复 ingestion
  - downstream audit view 中出现恢复后的新数据
- **Command / SQL ref**
  - Command: `./scripts/run_failure_branches.sh`
  - SQL ref: `SQL-F1-SOURCE-RECOVERY`
- **Input ref**
  - `scripts/update_cdc_actions.py --scenario source_recovery`
- **Output ref**
  - `artifacts/failure-branch-status.md`
- **Schema note / schema diff ref**
  - `SCHEMA_NOTE: no schema change`
- **Talking points**
  - 这里证明 source-side failure 可见且可恢复
- **Caveat / scope boundary**
  - 不把这一步讲成 generalized HA guarantee

### F2-downstream-delivery-retry
- **Feature checklist item**
  - Downstream delivery failure handling
- **Expected proof**
  - 对同一个 key，先看到失败日志，再看到成功投递
- **Command / SQL ref**
  - Command: `./scripts/run_failure_branches.sh`
  - SQL ref: `N/A`
- **Input ref**
  - `scripts/update_cdc_actions.py --scenario sink_failure`
  - `scripts/http_receiver.py` fail-once behavior
- **Output ref**
  - `artifacts/failure-branch-status.md`
  - `artifacts/http-receiver/error.log`
  - `artifacts/http-receiver/received.jsonl`
- **Schema note / schema diff ref**
  - `SCHEMA_NOTE: no schema change`
- **Talking points**
  - 当前 demo 证明的是 downstream delivery failure surfaced + retry recovery
- **Caveat / scope boundary**
  - 不把它扩展为所有 sink connector 的统一故障语义

---

## Poison Branch

### P1-poison-message
- **Feature checklist item**
  - Poison-message behavior
- **Expected proof**
  - source-error telemetry/logging 出现
  - bad payload 被跳过
  - follow-up valid message 继续被处理
- **Command / SQL ref**
  - Command: `./scripts/run_poison_kafka_case.sh`
  - SQL ref: `N/A`
- **Input ref**
  - `scripts/produce_poison_kafka.py`
- **Output ref**
  - `artifacts/poison-kafka/poison-kafka-check.md`
  - `artifacts/poison-kafka/risingwave.log`
  - `artifacts/poison-kafka/metric_before.prom`
  - `artifacts/poison-kafka/metric_after.prom`
  - `artifacts/poison-kafka/latest_state_before.json`
  - `artifacts/poison-kafka/latest_state_after.json`
- **Schema note / schema diff ref**
  - `SCHEMA_NOTE: no schema change`
- **Talking points**
  - 先读 expected behavior，再看 artifact
  - 只讲当前验证过的语义：telemetry/logging + skip + continue
- **Caveat / scope boundary**
  - 不讲 quarantine / DLQ / hard stop，除非未来证据明确支持
