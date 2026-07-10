## 连接配置帮助

### 概述

RisingWave 是一个兼容 PostgreSQL 协议的流式数据库。此连接器通过 PostgreSQL 线协议（JDBC 模式）或 RisingWave WebSocket ingest 端点（流式模式）将数据写入 RisingWave 表。

### 支持版本

- **JDBC 模式**：任意 RisingWave 版本均可（PostgreSQL 线协议）
- **WebSocket 流式模式**：需要 RisingWave **3.0.0+**（WebSocket ingest 端点，PR #25444）

### 写入模式

- **JDBC（兼容）** - 通过 PostgreSQL JDBC 执行标准 SQL `INSERT`/`UPDATE`/`DELETE`。简单易用，兼容所有 RisingWave 版本。
- **WebSocket 流式** - 通过 RisingWave WebSocket ingest 端点流式发送 DML 批次，支持异步 ACK。高吞吐、低延迟。需要 RisingWave 3.0.0+。

### 前置条件

#### 1. 目标表创建（自动）

流式模式下，连接器在目标表不存在时会**自动创建**，并加上 `WITH (connector = 'webhook')`。**无需**手动建表。

如果在 Tapdata 连接中配置了 **Webhook 密钥**，自动创建的表会包含 `VALIDATE` 子句，用于 HMAC-SHA256 签名验证：

```sql
-- 自动生成的 DDL（示例）：
CREATE TABLE IF NOT EXISTS "public"."orders" (
    "id" integer,
    "customer_name" varchar,
    "amount" numeric,
    PRIMARY KEY ("id")
) WITH (connector = 'webhook') VALIDATE AS secure_compare(
    headers->>'x-rw-signature',
    'sha256=' || encode(hmac('your-secret', payload, 'sha256'), 'hex')
);
```

如果未配置 Webhook 密钥，表会创建为**不带** `VALIDATE`，意味着任何能访问 ingest 端点的人都可以写入。

> **注意：** 已有的普通表无法通过 `CREATE TABLE IF NOT EXISTS` 转换为 webhook 表。如果表已存在且不是 webhook 表，需先删表。

#### 2. WebSocket ingest 端点

WebSocket ingest 端点与 SQL 端口不同：

| 部署方式 | SQL 端口 | WebSocket ingest 端口 |
|---------|---------|----------------------|
| 本地（`risedev`） | 4566 | 4560 |
| RisingWave Cloud | 4566 | 443（通过 `wss://`） |

**Ingest Endpoint** 应填写基础 URL，不带路径：
- 本地：`ws://host:4560`
- 云端：`wss://your-cluster.risingwave.cloud`

连接器会自动拼接 `/ingest/{database}/{schema}/{table}`。

#### 3. SSL

- **本地部署**：不需要 SSL。连接器默认使用 `sslmode=prefer`。
- **RisingWave Cloud**：需要 SSL，云代理通过 TLS SNI 路由连接。使用 `sslmode=require` 或 `prefer`。

### 连接字段

| 字段 | 必填 | 默认值 | 描述 |
|------|------|--------|------|
| 主机 | 是 | - | RisingWave 服务器主机名或 IP |
| 端口 | 是 | 4566 | RisingWave SQL 端口 |
| 数据库 | 是 | dev | RisingWave 数据库名 |
| Schema | 否 | public | 目标 Schema |
| 用户名 | 是 | root | 数据库用户名 |
| 密码 | 否 | - | 数据库密码 |
| 写入模式 | 否 | jdbc | `jdbc` 或 `streaming` |
| Ingest 地址 | 否 | ws://host.docker.internal:4560 | WebSocket ingest 基础 URL（仅流式模式） |
| Webhook 密钥 | 否 | - | 用于签名 WebSocket 认证消息的 HMAC 密钥（仅流式模式） |

### 限制

1. **Webhook 表不支持 `NOT NULL`**：流式模式下，建表时不生成 `NOT NULL` 约束。
2. **无法原地升级**：已有的普通表无法通过 `CREATE TABLE IF NOT EXISTS` 转换为 webhook 表。需先删表或使用新表名。
3. **不支持 DDL**：不会自动传播表结构变更（增删改列）。
4. **`varchar(N)` 和 `numeric(p,s)` 被简化**：建表时为兼容 RisingWave 会去掉长度/精度。
5. **仅作为目标**：此连接器只能作为目标（sink）使用，不能作为源。
