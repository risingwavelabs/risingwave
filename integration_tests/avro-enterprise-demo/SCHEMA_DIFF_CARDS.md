# Schema Diff Cards — Avro Enterprise Demo

这份文件只记录与 step card 相关的 schema notes / schema diffs。

---

## SCHEMA-B1-CUSTOMER-PROFILES
Feature:
- Kafka Avro source creation

Source object created:
- `demo_core.customer_profiles`

Key points:
- source columns由 Avro schema 自动解码导入
- `rw_key` 通过 `INCLUDE KEY AS rw_key` 暴露
- primary key 语义绑定到 Avro key

Schema note:
- 这是 source schema 建立，不是 schema evolution

---

## SCHEMA-B2-MV-OBJECTS
Feature:
- MV creation / joins / windowing / UDFs

Objects created:
- `normalize_email`
- `demo_ops.customer_profile_current`
- `demo_ops.customer_profile_complex`
- `demo_core.customer_action_window`
- `demo_marketing.customer_analytics_dev`
- `demo_core.customer_change_audit`

Schema note:
- 这里新增的是函数和 materialized views，不是对 source schema 的变更

---

## SCHEMA-B5-SINK-OBJECTS
Feature:
- downstream JSON output

Objects created:
- `demo_profile_latest_sink`
- `demo_change_file_sink`

Schema note:
- 这里是 sink object creation，不是 source schema change

---

## SCHEMA-S1-VIP-NOTE
Feature:
- add optional field

Schema change:
- 在 schema v2 中新增可选字段：`vip_note`

Observed proof:
- `information_schema.columns` 中出现 `vip_note`
- source / MV / latest-state 在 refresh 后继续可用

Boundary:
- remove optional field 不在当前 demo scope

---

## SCHEMA-C1-CDC-SOURCE
Feature:
- alternative source ingestion

Schema note:
- 本步骤不改变 schema
- 它证明的是已有 `customer_actions_cdc` source 的数据接入能力

---

## SCHEMA-R1-RBAC-PRINCIPALS
Feature:
- team-restricted access

Objects / principals involved:
- `marketing_user`
- `ops_user`
- grants on `demo_marketing` / `demo_ops`

Schema note:
- 这是权限配置验证，不是 schema 变化

---

## SCHEMA-NO-CHANGE
Schema note:
- no schema change
