# Full Feature Checklist (Run Verified)

> **OBSOLETE / PRE-FREEZE ARTIFACT**
>
> This checklist predates the current frozen customer-demo scope.
> Do **not** use it as the source of truth for the current customer demo.
> Use the current scope-aligned documents instead:
> - `../README.md`
> - `../RUNBOOK.md`
> - `../CUSTOMER_NOTES.md`
> - `../../.omx/plans/workstream0-capability-matrix-avro-enterprise-demo.md`

Run time: 2026-04-08
Runner: `./scripts/run_demo.sh` (completed successfully on rerun)
Note: first attempt failed at `create_source.sql` because leftover subscription `demo_marketing.customer_analytics_sub`; cleaned with `DROP SUBSCRIPTION ...` and reran successfully.

| 功能项 (原始要求) | 状态 | 结论 | 关键证据 |
| --- | --- | --- | --- |
| Kafka Avro source creation | [x] | 已创建 Kafka Avro source table。 | `create_source.sql` 中 `CREATE TABLE customer_profiles ... FORMAT UPSERT ENCODE AVRO`；`run_demo.sh` 第 3 步成功。 |
| Topic with Avro key/value | [x] | topic 使用 Avro key + Avro value。 | `scripts/produce_avro.py` 中 `KEY_SCHEMA='"string"'` + `AvroSerializer` key/value；`SHOW CREATE TABLE demo_core.customer_profiles` 显示 `FORMAT UPSERT ENCODE AVRO`。 |
| Auto schema decode from registry | [x] | 已从 Schema Registry 自动解码并映射列。 | `artifacts/demo-status.md` 第一项通过；`SHOW CREATE TABLE` 含结构化列（`profile`, `preferred_contact` 等）。 |
| Avro key used as primary key | [x] | Avro key 作为主键使用。 | `SHOW CREATE TABLE demo_core.customer_profiles` 显示 `PRIMARY KEY (rw_key) INCLUDE KEY AS rw_key`。 |
| Compacted topic with updates | [x] | source 与 downstream topic 都是 compacted。 | `rpk topic describe demo.customer.profile.upsert.avro` 和 `...latest` 显示 `cleanup.policy=compact`。 |
| Multiple updates for same key | [x] | 同 key 多次更新已验证。 | `artifacts/http-receiver/received.jsonl` 中 `c-1001` 多条更新；`scripts/produce_avro.py` 的 `STAGE1/STAGE2/STAGE3` 对同 key 连续更新。 |
| Correct latest-state materialization | [x] | latest-state 物化结果正确，仅保留最新活跃状态。 | `artifacts/downstream/latest_state.json` 仅有 `c-1001`/`c-1004`；`artifacts/demo-status.md` 对应项通过。 |
| Delete/tombstone handling | [x] | tombstone 删除行为生效。 | `artifacts/downstream/latest_state.json` 无 `c-1002`；`artifacts/demo-status.md` tombstone 检查通过。 |
| Source topic includes deletes | [x] | source topic 中存在 delete(tombstone) 记录。 | `rpk topic consume demo.customer.profile.upsert.avro -n 10 -f 'offset=%o ... value_size=%V'` 输出 `offset=4 ... value_size=0`。 |
| Downstream behaviour in materialized view and sink | [x] | MV 与 sink 行为一致，反映最新状态与删除效果。 | `artifacts/demo-status.md` 中 “Downstream Kafka upsert sink reflects latest state and deletes” 通过；`artifacts/downstream/latest_state.json`。 |
| Complex Avro handling | [x] | 已覆盖 nested records/arrays/maps/unions/enums/logical types。 | `scripts/produce_avro.py` `schema_v1()` 含 `Profile(Address)`, `tags(array)`, `attributes(map)`, union, enum, logicalType(`timestamp-micros`, `decimal`, `uuid`)。 |
| Stream processing | [x] | 流式聚合/转换已运行。 | `create_mv.sql` 中 `tumble(...)`, 聚合、JOIN、过滤；`artifacts/demo-status.md` 流式 join+window 通过。 |
| incremental subscription / push results without polling | [x] | 原生 SQL SUBSCRIBE 与 push bridge 都已验证。 | `artifacts/subscription/native_subscribe.json`（`initial_rows=[]` 且收到 `op=Insert`）；`artifacts/demo-status.md` 的 SUBSCRIBE 与 push bridge 两项通过。 |
| streaming joins | [x] | 流式 join 已验证。 | `create_mv.sql` 中 `demo_marketing.customer_analytics_dev` join 定义；`SELECT count(*) FROM demo_marketing.customer_analytics_dev` 为 3。 |
| UDFs | [x] | SQL UDF 已创建并参与 MV 计算。 | `create_mv.sql` 定义 `normalize_email`；`rw_catalog.rw_functions` 存在 `normalize_email`。 |
| Schema evolution add optional field | [x] | 新增 optional 字段已验证。 | `scripts/produce_avro.py` `schema_v2()` 新增 `vip_note`；`SHOW CREATE TABLE demo_core.customer_profiles` 可见 `vip_note`。 |
| Remove optional field | [ ] | 当前 run 未成功验证移除 optional 字段。 | blocker: refresh/remove 顺序下 registry latest 仍包含 `vip_note`；`curl /subjects/.../versions/latest` 显示 schema 仍有 `vip_note`，且 `information_schema.columns` 仍有 `vip_note`。 |
| Verify source/view continuity and state preservation | [x] | 在 source 重启与 schema 变更流程后，view 状态连续。 | `artifacts/demo-status.md` 中 source restart continuity 通过；`artifacts/downstream/latest_state.json` 状态连续可读。 |
| Materialized view creation in DEV | [x] | DEV 中 MV 创建成功。 | `SHOW CREATE MATERIALIZED VIEW demo_marketing.customer_analytics_dev` 返回定义；`run_demo.sh` 第 3 步成功。 |
| Promotion through CI/CD | [ ] | 自动化脚本存在，但本次未连到上层环境执行验证。 | `cicd/promote_mv.sh` 存在；blocker: 缺少 `RW_TARGET_HOST` 等真实高环境参数。 |
| File sink generation CSV/JSON/XML/fixed-width | [x] | 4 种文件已落地（经 JSON sink 导出）。 | `artifacts/exports/customer_change_export.{csv,json,xml,txt}` 存在；`artifacts/demo-status.md` 对应项通过。 |
| Alternative source ingestion | [x] | 非 Kafka 源已用 Postgres CDC 实现并跑通。 | `create_source.sql` 中 `customer_actions_cdc` 使用 `connector='postgres-cdc'`；`artifacts/demo-status.md` source recovery 通过。 |
| Enterprise access model: AD group to RBAC role mapping | [ ] | 本 demo 未实现外部 AD group 映射。 | `artifacts/demo-status.md` 明确未完成（native RBAC only）。 |
| Enterprise access model: Restricted access to sources/views by team | [x] | 团队视图可读、原始源受限。 | `PGPASSWORD=marketing123 ... SELECT count(*) FROM demo_marketing.customer_analytics_dev` 成功；同账号查 `demo_core.customer_profiles` 报权限错误。 |
| Observability (metrics/logs/offset/failure diagnostics) | [x] | 指标、日志、offset、故障诊断入口可用。 | `curl http://localhost:9500/api/v1/query?query=up{job='prometheus'}` 返回 `1`；`docker compose logs risingwave-standalone --tail 50` 有 committed offset 与错误日志；`rpk topic consume demo.customer.profile.latest` 显示 offsets。 |
| Error Handling: source failure handling | [x] | source 停启后恢复并继续处理。 | `run_demo.sh` 第 7 步执行 stop/start source-postgres；`artifacts/demo-status.md` source recovery 通过；`customer_change_audit` 中 `channel='ops-console'` 计数为 1。 |
| Error Handling: sink failure handling | [x] | sink 失败日志与恢复后成功投递已验证。 | `artifacts/http-receiver/error.log` 有 `c-1004 forced failure`；`artifacts/http-receiver/received.jsonl` 后续有 `c-1004` 成功记录。 |
| Error Handling: poison message | [ ] | 未能验证“poison message 被 source parser 拒绝”路径。 | blocker: webhook 路径先被判定为非 webhook source；`curl .../webhook/...` 返回 `403 {"error":"Table \`customer_actions_raw\` is not with webhook source"}`；`artifacts/webhook_poison.out` 同结果。 |

## Remaining Blockers (explicit)

1. `Remove optional field`：当前流程中无法证明 `vip_note` 被真正移除（registry latest 与 table schema 仍含该字段）。
2. `Promotion through CI/CD`：缺少真实上层环境连接参数，脚本仅存在未实跑。
3. `AD group to RBAC mapping`：该 demo 只做 native user/role。
4. `Poison message`：webhook source path 在当前运行态被 403 拦截，未进入预期 poison parsing/rejection 路径。
