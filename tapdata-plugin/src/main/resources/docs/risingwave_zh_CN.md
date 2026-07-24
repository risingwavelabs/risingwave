## 连接配置帮助

### 概述

RisingWave 是一个兼容 PostgreSQL 协议的流式数据库。此连接器通过 PostgreSQL 线协议（JDBC 模式）或 RisingWave WebSocket ingest 端点（流式模式）将数据写入 RisingWave 表。

### 支持版本

- **WebSocket 流式模式**：需要 RisingWave **3.0.0+**（WebSocket ingest 端点）
- **JDBC 模式**：任意 RisingWave 版本均可（PostgreSQL 线协议）

### 写入模式

- **WebSocket 流式（默认且推荐）** - 通过 RisingWave WebSocket ingest 端点流式发送 DML 批次，支持异步 ACK，面向高吞吐和低延迟场景。需要 RisingWave 3.0.0+。
- **WebSocket JSONB 仅追加** - 创建单个 `data JSONB` 列，把每条插入的源记录保存为一个
  JSON 文档。该模式允许无主键模型，但会拒绝更新和删除事件；由于没有行标识，重试可能
  追加重复记录。为避免静默精度损失，任意精度的十进制和整数值会保存为 JSON 字符串。
- **JDBC（兼容回退）** - 通过 PostgreSQL JDBC 执行标准 SQL `INSERT`/`UPDATE`/`DELETE`。配置简单，兼容所有 RisingWave 版本。

WebSocket 流式模式要求每个源模型都有主键。插入和主键不变的更新使用 upsert。
更新改变主键值时，连接器会在同一个 WebSocket batch 中发送 `delete(before)` 和
`upsert(after)`；删除使用 before image。对于无主键表，如果源端只产生插入事件，可使用
WebSocket JSONB 仅追加模式；需要支持更新或删除事件时，请使用 JDBC 模式。

### 前置条件

#### 1. 目标表创建（自动）

流式模式下，连接器在目标表不存在时会**自动创建**，并加上 `WITH (connector = 'webhook')`。**无需**手动建表。

如果配置了 **Webhook 密钥**，connector 会引用 RisingWave catalog Secret，避免密钥值通过
`SHOW CREATE TABLE` 暴露。connector 会根据 **Webhook 密钥**创建和轮换该 Secret。
**RisingWave Secret 名称**留空时，会自动生成每张表的名称；填写后可指定一个稳定名称，
多个目标表可以共用。该功能需要 Secret Management 和 `CREATE SECRET` 权限。

```sql
-- 自动生成的 DDL（示例）：
CREATE TABLE "public"."orders" (
    "id" integer,
    "customer_name" varchar,
    "amount" numeric,
    PRIMARY KEY ("id")
) WITH (connector = 'webhook') VALIDATE SECRET "tapdata_webhook_..." AS secure_compare(
    headers->>'x-rw-signature',
    'sha256=' || encode(hmac("tapdata_webhook_...", payload, 'sha256'), 'hex')
);
```

如果未配置 Webhook 密钥，表会创建为**不带** `VALIDATE`，意味着任何能访问 ingest 端点的人都可以写入。

> **注意：** 已有的普通表无法自动转换为 webhook 表。如果表已存在且不是 webhook 表，需先删表。

#### 2. WebSocket ingest 端点

WebSocket ingest 端点与 SQL 端口不同：

| 部署方式 | SQL 端口 | WebSocket ingest 端口 |
|---------|---------|----------------------|
| 本地（`risedev`） | 4566 | 4560 |
| RisingWave Cloud | 4566 | 443（通过 `wss://`） |

**Ingest Endpoint** 应填写基础 URL，不带路径：
- 本地：`ws://host:4560`
- 云端：`wss://<Cloud SQL 主机>`（无需填写端口，Cloud 使用 443）

连接器会自动拼接 `/ingest/{database}/{schema}/{table}`。

### 连接预检查

连接测试会验证 SQL 登录、RisingWave 版本和配置的 Schema。JDBC 模式会创建临时表，
并验证 SQL 插入、更新、删除和删表。WebSocket 流式模式会创建临时 webhook 表，连接
配置的 WebSocket 地址，在配置密钥时发送签名 init 消息，写入一个 DML 批次，等待
RisingWave ACK，最后删除临时表。这样可以同时验证 endpoint、ingest 路径、签名配置
以及目标表所需的 DDL 权限。

#### 3. SSL

- **本地部署**：不需要 SSL。连接器默认使用 `sslmode=prefer`。
- **RisingWave Cloud**：需要 SSL，云代理通过 TLS SNI 路由连接。使用 `sslmode=require` 或 `prefer`。
- TLS 使用 Java 运行时的受信任 CA 库。目前不支持上传自定义 CA 或双向 TLS 客户端证书；
  RisingWave Cloud 使用公共受信任证书，不需要上传这些文件。

### 连接字段

| 字段 | 必填 | 默认值 | 描述 |
|------|------|--------|------|
| 主机 | 是 | - | RisingWave 服务器主机名或 IP |
| 端口 | 是 | 4566 | RisingWave SQL 端口 |
| 数据库 | 是 | dev | RisingWave 数据库名 |
| Schema | 否 | public | 目标 Schema |
| 用户名 | 是 | root | 数据库用户名 |
| 密码 | 否 | - | 数据库密码 |
| 写入模式 | 否 | streaming | `streaming`（推荐）、`streaming_jsonb`（仅追加）或 `jdbc`（兼容回退） |
| Ingest 地址 | 否 | 留空 | 留空时自动使用 `ws://<主机>:4560`；TLS 或独立 ingest 主机时需显式填写 |
| Webhook 密钥 | 否 | - | 用于签名 WebSocket 认证消息的 HMAC 密钥（仅流式模式） |
| RisingWave Secret 名称 | 否 | 自动生成 | 可选的稳定名称；connector 会根据 Webhook 密钥创建和轮换该受保护 Secret，留空时生成每表名称 |

### 限制

1. **Webhook 表不支持 `NOT NULL`**：流式模式下，建表时不生成 `NOT NULL` 约束。
2. **无法原地升级**：已有的普通表无法自动转换为 webhook 表。需先删表或使用新表名。
3. **不支持 DDL**：不会自动传播表结构变更（增删改列）。
4. **`varchar(N)` 和 `numeric(p,s)` 被简化**：建表时为兼容 RisingWave 会去掉长度/精度。
5. **仅作为目标**：此连接器只能作为目标（sink）使用，不能作为源。
6. **JSONB 精确数值使用字符串**：JSONB 仅追加模式会把任意精度十进制和整数保存为
   JSON 字符串，因为 RisingWave JSON number 可能发生舍入。
7. **typed 更新需要完整行**：connector 只做一次规范化：用事件中可用的 `before`、`after`
   和顶层 `removedFields` 形成完整 post-image，再把同一行交给 WebSocket 或 JDBC。无法安全
   补齐全部目标列时会明确失败。replace event 把 `after` 当作最终结果，省略的已知
   列写为 SQL `NULL`。唯一主键为 `_id` 且没有 `before` 的 MongoDB event 使用相同的权威
   文档规则。MongoDB source 必须保持启用 TapData 的**更新字段补全**
   (`enableFillingModifiedData=true`)；TapData 默认启用该功能。若关闭，target 无法区分 partial
   patch 与完整的稀疏文档，省略列可能被写成 `NULL`。删除
   `profile.name` 等嵌套字段时必须提供 `profile` 的完整
   post-image。MySQL 必须使用 `binlog_row_image=FULL`；TapData 会在事件到达 connector 前拒绝
   `MINIMAL`。若 PostgreSQL 的 TOAST 值在可用 image 中无法还原，更新会明确失败。
8. **大记录存在 frame 限制**：connector 会把 batch 拆成小于 8 MiB 的有序 WebSocket
   payload。单条 source record 本身超过该大小时无法安全拆分，会返回明确错误。
9. **JDBC 可见性屏障**：RisingWave 的 JDBC insert 是异步可见的。connector 只会在后续
   update/delete 到来且当前 batch 仍有未刷新的带主键 insert 时保守执行 `FLUSH`，并在 batch
   结束时再次执行。没有 insert 等待变为 query-visible 时，update/delete 可以保留在同一 batch。
10. **JSONB 模式中的二进制值**：`byte[]` 会以 PostgreSQL bytea hex 文本（`\\x<hex>`）存为
    JSON string。JSONB 没有原生 binary scalar；需要原始字节的消费者应按此表示解码。
11. **CDC identity 与 schema 安全性**：带主键 update/delete 必须提供旧主键 identity。
    只有唯一主键为 `_id` 时允许 after-only update，因为 MongoDB `_id` 不可变；其他缺失旧
    identity 的事件会明确失败。自动 schema evolution 未支持时，未知的关系型字段也会明确
    失败，不会被静默丢弃。
12. **Source 验证状态**：精确的 `1.0.0` 发布产物已通过 PostgreSQL、使用
    `binlog_row_image=FULL` 的 MySQL 8.4、显式启用 `enableFillingModifiedData=true` 的
    MongoDB 7，以及通过 JSONB 仅追加模式写入的 Kafka 3.9.1 验证。SQL Server 尚未完成验证。
