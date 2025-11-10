# MySQL CDC 配置界面生成 Prompt

## 任务说明
生成一个用户友好的 MySQL CDC 配置界面，用于替代手工编写 CREATE SOURCE 语句。界面需要区分必填和可选参数，并为特定参数提供下拉选项。

## 配置参数规范

### 必填参数 (Required Parameters)

1. **connector** 
   - 类型: 固定值
   - 值: `mysql-cdc`
   - 说明: 连接器类型，固定为 mysql-cdc
   - UI: 隐藏字段（自动填充）

2. **hostname**
   - 类型: 文本输入
   - 说明: MySQL 数据库的主机名或 IP 地址
   - 示例: `localhost`, `192.168.1.100`, `db.example.com`
   - 验证: 非空

3. **port**
   - 类型: 数字输入
   - 说明: MySQL 数据库端口
   - 默认值: `3306`
   - 验证: 1-65535 范围内的整数

4. **username**
   - 类型: 文本输入
   - 说明: 数据库用户名（需要有 REPLICATION CLIENT、REPLICATION SLAVE 等权限）
   - 验证: 非空

5. **password**
   - 类型: 密码输入
   - 说明: 数据库密码
   - 验证: 非空
   - UI: 密码框（隐藏输入）

6. **database.name**
   - 类型: 文本输入
   - 说明: 要同步的数据库名称
   - 验证: 非空
   - 提示: 不能是 MySQL 内置数据库（如 mysql, sys 等）

7. **table.name**
   - 类型: 文本输入
   - 说明: 要同步的表名
   - 验证: 非空
   - 提示: 指定要从中摄取数据的表名

### 可选参数 (Optional Parameters)

1. **server.id**
   - 类型: 数字输入
   - 说明: 数据库客户端的唯一数字 ID，在 MySQL 集群中必须唯一
   - 默认值: 自动生成随机 ID
   - 验证: 正整数
   - 提示: 创建共享源时必填。如不指定，RisingWave 会生成一个随机 ID
   - 注意: 在整个 MySQL 集群中运行的所有数据库进程中必须唯一

2. **ssl.mode**
   - 类型: 下拉选择
   - 选项:
     - `disabled` - 不使用 SSL（默认）
     - `preferred` - 优先使用 SSL
     - `required` - 必须使用 SSL
   - 默认值: `disabled`
   - 说明: SSL 连接模式，决定与 MySQL 安全通信的加密级别

3. **auto.schema.change**
   - 类型: 下拉选择
   - 选项:
     - `true` - 启用自动 schema 变更
     - `false` - 禁用自动 schema 变更（默认）
   - 默认值: `false`
   - 说明: 是否启用复制 MySQL 表的 schema 变更

4. **transactional**
   - 类型: 下拉选择
   - 选项:
     - `true` - 启用事务（共享源默认为 true）
     - `false` - 禁用事务（非共享源默认为 false）
   - 默认值: 共享源为 `true`，其他为 `false`
   - 说明: 是否为即将创建的 CDC 表启用事务
   - 注意: 涉及超过 4096 行变更的事务无法保证

### Per-Table 参数 (在 CREATE TABLE ... FROM SOURCE 时使用)

1. **snapshot**
   - 类型: 下拉选择
   - 选项:
     - `true` - 执行初始快照（默认）
     - `false` - 禁用 CDC 回填，仅消费表创建后的上游事件
   - 默认值: `true`
   - 说明: 是否在创建表时对现有数据执行快照
   - 使用场景: 仅适用于从共享源创建的表

2. **snapshot.interval**
   - 类型: 数字输入
   - 说明: 用于缓冲上游事件的屏障间隔
   - 默认值: `1`
   - 验证: 正整数

3. **snapshot.batch_size**
   - 类型: 数字输入
   - 说明: 从上游表读取快照查询的批次大小
   - 默认值: `1000`
   - 验证: 正整数

### 高级参数 (Advanced Parameters) - 可折叠区域

这些参数通常不需要修改，可以放在"高级设置"折叠区域中：

## UI 布局建议

```
┌─────────────────────────────────────────┐
│  MySQL CDC 源配置                        │
├─────────────────────────────────────────┤
│                                         │
│  基本连接信息                              │
│  ┌───────────────────────────────────┐  │
│  │ 主机名 *     [________________]    │  │
│  │ 端口 *       [3306_____________]   │  │
│  │ 用户名 *     [________________]    │  │
│  │ 密码 *       [****************]    │  │
│  │ 数据库名 *   [________________]    │  │
│  │              提示：不能使用内置库    │  │
│  │ 表名 *       [________________]    │  │
│  └───────────────────────────────────┘  │
│                                         │
│  其他选项（可选）                         │
│  ┌───────────────────────────────────┐  │
│  │ Server ID    [自动生成__________]  │  │
│  │              提示：共享源时必填      │  │
│  │ SSL 模式     [▼ disabled]          │  │
│  │ 自动 Schema 变更 [▼ 否(false)]     │  │
│  │ 事务模式     [▼ 自动______________] │  │
│  │              (共享源:true,其他:false)│  │
│  └───────────────────────────────────┘  │
│                                         │
│  ▼ 高级设置（点击展开）                   │
│  ┌───────────────────────────────────┐  │
│  │ 其他 Debezium 参数                │  │
│  │  [+ 添加参数]                      │  │
│  └───────────────────────────────────┘  │
│                                         │
│  [测试连接]  [取消]  [创建源]             │
└─────────────────────────────────────────┘
```

## 生成的 SQL 示例

根据用户填写的配置，生成的 CREATE SOURCE 语句示例：

### 示例 1: 基本配置（使用默认值）

```sql
CREATE SOURCE my_mysql_source WITH (
  connector = 'mysql-cdc',
  hostname = 'localhost',
  port = '3306',
  username = 'root',
  password = 'password',
  database.name = 'mydb',
  table.name = 'orders'
);
```

### 示例 2: 完整配置（包含可选参数）

```sql
CREATE SOURCE my_mysql_source WITH (
  connector = 'mysql-cdc',
  hostname = 'localhost',
  port = '3306',
  username = 'root',
  password = 'password',
  database.name = 'mydb',
  table.name = 'orders',
  server.id = '5701',
  ssl.mode = 'required',
  auto.schema.change = 'true',
  transactional = 'true'
);
```

### 示例 3: 从源创建表

```sql
-- 创建共享源
CREATE SOURCE mysql_source WITH (
  connector = 'mysql-cdc',
  hostname = 'localhost',
  port = '3306',
  username = 'root',
  password = 'password',
  database.name = 'mydb',
  server.id = '5701'
);

-- 从源创建表（使用自动 schema 映射）
CREATE TABLE orders (*)
FROM mysql_source TABLE 'mydb.orders';

-- 从源创建表（不执行初始快照，仅同步增量）
CREATE TABLE users (
  id INT PRIMARY KEY,
  name VARCHAR,
  email VARCHAR
)
WITH (snapshot = 'false')
FROM mysql_source TABLE 'mydb.users';

-- 从源创建表（自定义快照参数）
CREATE TABLE products (
  id INT PRIMARY KEY,
  name VARCHAR,
  price DECIMAL
)
WITH (
  snapshot = 'true',
  snapshot.interval = '1',
  snapshot.batch_size = '2000'
)
FROM mysql_source TABLE 'mydb.products';
```

## 验证规则

1. **连接验证**: 点击"测试连接"按钮时，应该（此为 cloud 版本需要做的事，当前网页中可以删掉）：
   - 验证数据库连接是否成功
   - 检查用户是否有足够的权限（REPLICATION CLIENT, REPLICATION SLAVE 等）
   - 验证 binlog 是否已启用
   - 检查 binlog 格式是否为 ROW

2. **字段验证**:
   - hostname/port/username/password/database.name/table.name 不能为空
   - port 必须是 1-65535 的整数
   - server.id 必须是正整数（如果填写）
   - database.name 不能是 MySQL 内置数据库（mysql, sys, information_schema, performance_schema）

3. **依赖关系**:
   - 当创建共享源时，建议填写 server.id

## 额外说明

### UI 实现建议

- 带 * 的字段为必填项
- 所有下拉选项都应该显示中文描述和实际值（括号内）
- 建议提供"测试连接"功能，在创建前验证配置的正确性
- 建议提供"导入/导出配置"功能，方便配置复用
- 支持从环境变量读取敏感信息（如密码），使用 SECRET 对象

### 参数说明特点

1. **自动生成参数**: 
   - `server.id` 如果留空，RisingWave 会自动生成一个随机的唯一 ID
   
2. **条件显示**:
   - `transactional` 的默认值根据是否为共享源自动确定
   
3. **智能提示**:
   - 提示用户 `database.name` 不能使用 MySQL 内置数据库
   - 创建共享源时提示需要填写 `server.id`

4. **权限要求提示**:
   - 用户需要有 REPLICATION CLIENT 权限
   - 用户需要有 REPLICATION SLAVE 权限
   - 用户需要有 SELECT 权限
   - 需要在数据库中启用 binlog，并设置 binlog_format = 'ROW'
   - binlog_row_image 需要设置为 'FULL'

### 支持的 MySQL 版本

- MySQL 5.7
- MySQL 8.0.x
- AWS RDS MySQL
- AWS Aurora MySQL
- 其他 MySQL 兼容数据库

### 与 PostgreSQL CDC 的主要区别

- MySQL CDC 不需要配置 replication slot
- MySQL CDC 不需要配置 publication
- MySQL CDC 需要配置 server.id（特别是共享源模式）
- MySQL CDC 需要确保 binlog 已启用且格式为 ROW

### 官方文档参考

完整的参数说明和使用示例请参考：
https://docs.risingwave.com/ingestion/sources/mysql/mysql-cdc#connector-parameters

