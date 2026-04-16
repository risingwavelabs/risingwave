# Avro Enterprise Demo Flow

这份文档回答三个问题：

1. **演示流程是什么**：推荐顺序是什么
2. **每一步对应什么功能**：客户 checklist 的哪一项由哪一步证明
3. **详细内容去哪里看**：SQL / 输入 / 输出 / schema 变化 / talking points 在哪里

> 详细单步内容现在以 `DEMO_STEP_CARDS.md` 为准。  
> `DEMO_FLOW.md` 只负责给出演示顺序、step_id 与功能映射。

---

## 1. 入口文档关系

- `README.md`
  - scope / setup / entrypoint / links
- `RUNBOOK.md`
  - operator 怎么跑
- `DEMO_FLOW.md`
  - 演示顺序与 feature 映射
- `DEMO_STEP_CARDS.md`
  - 单步执行卡片（主 artifact）
- `SQL_SNIPPETS.md`
  - SQL 引用库
- `SCHEMA_DIFF_CARDS.md`
  - schema note / diff 引用库

---

## 2. 推荐演示顺序

### Phase A — Canonical backbone
1. `B1-source-create`
2. `B2-mv-create`
3. `B3-updates-tombstones`
4. `B4-join-subscribe`
5. `B5-json-output`

### Phase B — Isolated branches
6. `S1-add-optional-field`
7. `C1-postgres-cdc`
8. `R1-rbac-allow-deny`
9. `O1-observability-signals`
10. `F1-source-recovery`
11. `F2-downstream-delivery-retry`
12. `P1-poison-message`

### Phase C — Narrated-only boundaries
13. CI/CD promotion（definition-as-code / dbt-compatible automation path）
14. AD mapping（workaround / future path）
15. Fixed-width defer remark

---

## 3. Step-to-feature mapping

| Step ID | 演示动作 | 对应 feature |
| --- | --- | --- |
| `B1-source-create` | 建 Kafka Avro source | Kafka Avro source creation / key-value / registry decode / Avro key as PK |
| `B2-mv-create` | 建 UDF 与 MV | MV creation / filters / projections / joins / windowing / UDF |
| `B3-updates-tombstones` | 更新与删除 | compacted updates / multiple updates / latest-state / tombstone / complex Avro |
| `B4-join-subscribe` | 增量订阅 proof | streaming joins / native `SUBSCRIBE` |
| `B5-json-output` | 下游 JSON 输出 | JSON-format downstream output |
| `S1-add-optional-field` | 增字段 + continuity | schema evolution add optional field / continuity / state preservation |
| `C1-postgres-cdc` | Postgres CDC proof | alternative source ingestion |
| `R1-rbac-allow-deny` | 允许/拒绝访问 | restricted access to sources/views by team |
| `O1-observability-signals` | metrics / logs / offsets | observability |
| `F1-source-recovery` | source restart 后恢复 | source failure handling |
| `F2-downstream-delivery-retry` | 下游投递失败后恢复 | downstream delivery failure handling |
| `P1-poison-message` | poison payload 行为 | poison-message behavior |

---

## 4. 每一步应该展示什么

对每一个 retained step，现场都应该先说清楚：

1. **Feature checklist item**：这一步在证明哪个客户需求
2. **Expected proof**：你接下来应该看到什么结果才算通过
3. **SQL / input / output / schema note**：具体参考哪里

详细内容请直接跳到：
- `DEMO_STEP_CARDS.md`

---

## 5. 现场使用建议

### 如果你只讲主线
按这 5 步：
- `B1-source-create`
- `B2-mv-create`
- `B3-updates-tombstones`
- `B4-join-subscribe`
- `B5-json-output`

### 如果客户要看完整 enterprise story
按完整顺序走到：
- `S1-add-optional-field`
- `C1-postgres-cdc`
- `R1-rbac-allow-deny`
- `O1-observability-signals`
- `F1-source-recovery`
- `F2-downstream-delivery-retry`
- `P1-poison-message`

### Narrated-only 的内容
不要 live 展示，只说明：
- CI/CD promotion
- AD mapping
- fixed-width defer

---

## 6. 一次完整 rehearsal
```bash
cd integration_tests/avro-enterprise-demo
time ./scripts/run_demo.sh
```

当前 integrated rehearsal 的结果参考：
- `artifacts/integrated-rehearsal-status.md`
