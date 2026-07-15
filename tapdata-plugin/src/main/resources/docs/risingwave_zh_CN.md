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

如果在 Tapdata 连接中配置了 **Webhook 密钥**，自动创建的表会包含 `VALIDATE` 子句，用于 HMAC-SHA256 签名验证：

```sql
-- 自动生成的 DDL（示例）：
CREATE TABLE "public"."orders" (
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

### 限制

1. **Webhook 表不支持 `NOT NULL`**：流式模式下，建表时不生成 `NOT NULL` 约束。
2. **无法原地升级**：已有的普通表无法自动转换为 webhook 表。需先删表或使用新表名。
3. **不支持 DDL**：不会自动传播表结构变更（增删改列）。
4. **`varchar(N)` 和 `numeric(p,s)` 被简化**：建表时为兼容 RisingWave 会去掉长度/精度。
5. **仅作为目标**：此连接器只能作为目标（sink）使用，不能作为源。
6. **JSONB 精确数值使用字符串**：JSONB 仅追加模式会把任意精度十进制和整数保存为
   JSON 字符串，因为 RisingWave JSON number 可能发生舍入。
