## RisingWave 连接

这是仅支持目标端的 connector，用于把 TapData 的全量和 CDC 事件写入 RisingWave。

### 选择写入模式

| 模式 | 适用场景 | 要求 |
|---|---|---|
| **WebSocket streaming** | 带主键的插入、更新和删除。默认推荐。 | RisingWave 3.0+，并且表有主键 |
| **WebSocket JSONB append-only** | 无主键的仅插入数据流 | RisingWave 3.0+；不支持更新和删除 |
| **JDBC** | 兼容回退 | 可访问 PostgreSQL 兼容 SQL 端点 |

WebSocket streaming 通常有更好的吞吐和延迟。所有模式仍使用 JDBC 检查版本、schema、
metadata、DDL 和权限。

### 本地连接示例

TapData 在 Docker 中、RisingWave 在宿主机上运行时：

```text
主机: host.docker.internal
端口: 4566
数据库: dev
Schema: public
用户: root
写入模式: WebSocket streaming
Ingest 地址: <留空>
SSL 模式: prefer 或 disable
```

不要使用 `localhost` 访问 Docker 宿主机。**Ingest 地址**留空时自动使用
`ws://<主机>:4560`。

### RisingWave Cloud 示例

```text
主机: <Cloud SQL hostname>
端口: 4566
SSL 模式: require
Ingest 地址: wss://<Cloud SQL hostname>
```

Cloud WebSocket ingest 使用 443 端口，WSS 使用 Java trust store。JDBC `sslmode=require`
会加密 SQL 连接，但不提供 `verify-full` hostname 验证。

### 连接测试

连接测试会检查：

- SQL 登录和 RisingWave 版本；
- 目标 schema；
- 建表、删表和写入权限；
- WebSocket 模式还会检查 endpoint、可选的签名初始化、一次 DML 写入及 RisingWave ACK。

WebSocket 模式要求 RisingWave 3.0 或更高版本。旧版兼容 RisingWave 仍可使用 JDBC。

### 可选 Webhook Secret

**Webhook Secret** 用于签名 WebSocket 初始化消息。填写后，connector 会创建或复用
**RisingWave Secret Name**，并在建表 DDL 中引用该 Secret。`SHOW CREATE TABLE` 不会包含
secret 明文。

数据库用户需要 `CREATE SECRET` 权限。Secret Name 留空时，connector 会为每张表生成名称。

### 任务要求

- 生产全量和 CDC 任务使用 **数据复制**。
- WebSocket streaming 必须使用真实的源表主键作为更新条件。
- MySQL 要求 `binlog_row_image=FULL`。
- MongoDB 要求启用 TapData 更新字段补全：
  `enableFillingModifiedData=true`。
- SQL Server 要求启用 Change Tracking。
- Oracle 要求使用 LogMiner、`autoLog=false` 和主键 supplemental logging。

### 重要限制

- WebSocket streaming 写入完整行 upsert。更新或删除缺少旧主键时会失败。
- 不支持关系型表的自动 schema evolution。出现未知字段时会失败，不会静默丢弃。
- JSONB append-only 创建一个 `data JSONB` 列，只接受插入。
- JSONB 是 at-least-once。ACK 结果不明确时重试可能产生重复记录。
- 单条序列化 WebSocket 记录超过 8 MiB 时会明确失败。
- JDBC 是兼容模式，吞吐通常低于 WebSocket streaming。

`1.0.0` 验证矩阵包括 PostgreSQL、MySQL 8.4、MongoDB 7、通过 JSONB append-only 写入的
Kafka 3.9.1、SQL Server 2022 和 Oracle 26ai。SQL Server 和 Oracle 均已通过 WebSocket
streaming 与 JDBC 测试。
