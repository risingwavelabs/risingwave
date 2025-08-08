# RisingWave 刷新容错机制实施计划

## 项目概述

实现 RisingWave 可刷新物化视图（Refreshable Materialized Views）的容错机制，解决现有刷新过程中的故障恢复和状态管理问题。

## 问题分析

### 当前存在的问题

1. **刷新过程缺乏容错能力**
   - 处理大数据量时易失败（在 `load finish` 阶段执行 sort-merge join 时容易超时）
   - 无断点续传机制（失败后需要从头开始）
   - 整个刷新是 "all-or-nothing" 操作

2. **系统状态管理缺失**
   - Meta 层不知道表是否正在刷新，存在竞态条件
   - 用户可能重复执行 `REFRESH TABLE` 命令导致数据不一致
   - 系统崩溃后无法自动恢复刷新任务

3. **执行过程阻塞性**
   - `on_load_finish` 是大的阻塞操作，无法响应 barrier
   - 在刷新过程中系统失去响应性

## 解决方案设计

### 核心理念

借鉴 Backfill 机制，将 `refresh table` 重构为可持久化、可跟踪状态的后台任务，使用 `select_with_strategy` 实现非阻塞执行。

### 1. Meta 层持久化状态管理

#### TableCatalog 扩展
- 添加 `refresh_state` 字段：`IDLE` | `REFRESHING` | `FINISHING`
- 添加 `refresh_progress` 字段用于跟踪恢复信息

#### 并发控制机制
- 刷新前检查表状态，拒绝并发请求
- 实现状态转换的原子性操作
- 系统启动时扫描和恢复中断的刷新任务

### 2. Progress State Table 机制

#### 设计思路
参考 StreamScanNode 的模式，为 MaterializeNode 添加专门的 progress state table。

#### Progress Table Schema
```sql
CREATE TABLE refresh_progress (
    vnode SMALLINT,                    -- partition key
    refresh_stage VARCHAR,             -- 'refreshing' | 'merging' | 'cleanup'
    main_iter_position BYTEA,         -- 主表迭代器序列化位置
    staging_iter_position BYTEA,      -- staging表迭代器位置
    processed_rows BIGINT,            -- 已处理行数
    last_checkpoint_epoch BIGINT,     -- 检查点epoch
    PRIMARY KEY (vnode)
);
```

### 3. MaterializeExecutor 分阶段状态机

#### 状态定义
```rust
#[derive(Debug, Clone)]
pub enum RefreshStage {
    Normal,                           // 正常执行状态
    Refreshing,                       // 正在刷新数据到 staging table
    Merging {                         // 正在执行 sort-merge join
        current_vnode: Option<VirtualNode>,
        completed_vnodes: HashSet<VirtualNode>,
    },
    Cleanup,                          // 清理 staging table
}
```

#### 执行流程
1. **Normal → Refreshing**: 收到 `RefreshStart` barrier，开始写 staging table
2. **Refreshing → Merging**: 收到 `LoadFinish` barrier，开始分块 sort-merge
3. **Merging → Cleanup**: 完成所有 vnode 的处理，清理 staging table
4. **Cleanup → Normal**: 完成清理，重置状态

### 4. select_with_strategy 分流执行

#### 核心思路
参考 CDC backfill 的实现，使用双流处理：
- **左流（高优先级）**: 上游消息流（Chunk, Barrier, Watermark）
- **右流（后台处理）**: refresh 任务流（分块 sort-merge join）

#### 实现方式
```rust
let upstream_stream = input.execute();
let merge_task_stream = self.create_merge_task_stream();

let combined_stream = select_with_strategy(
    upstream_stream,
    merge_task_stream,
    |_: &mut ()| PollNext::Left  // 优先处理 barrier
);
```

### 5. 分块处理机制

#### 处理粒度
- 按 vnode 分块：每个 vnode 独立处理，支持并行恢复
- 批次处理：每次处理固定数量的行（如 1000 行）
- 定期检查点：每个批次完成后更新 progress table

#### 断点续传
- 从 progress table 恢复迭代器位置
- 继续未完成的 vnode 处理
- 支持跨系统重启的恢复

### 6. Barrier 协调机制

#### 新增 Barrier 类型
- `RefreshProgress`: 定期报告刷新进度
- `RefreshComplete`: 通知刷新完成

#### Barrier 处理逻辑
- 在 Merging 阶段接收到 Barrier 时暂停 merge 任务
- 提交当前进度到 progress table
- 处理 Barrier（可能包含 Pause/Resume/Throttle）
- 发送 Barrier 到下游后恢复 merge 任务

## 实施阶段

### Phase 1: 基础架构搭建 ✅ **已完成**
1. **扩展 Proto 定义** ✅ **完成**
   - `proto/catalog.proto`: 添加 RefreshState, RefreshProgress
   - `proto/stream_plan.proto`: MaterializeNode 添加 refresh_progress_table

2. **Meta 层状态管理** ⚠️ **框架完成，实现待补充**
   - 添加了状态管理方法的签名和文档
   - 并发控制和恢复逻辑标记为 TODO，需要数据库迁移后实现

3. **增强 Refresh Handler** ⚠️ **部分完成**
   - 增强了文档和错误处理
   - 原子性状态操作待数据库迁移后实现

### Phase 2: 核心执行引擎 🔄 **进行中**
1. **扩展 RefreshableMaterializeArgs** ✅ **完成**
   - 添加 progress_table_catalog、progress_table、current_stage、merge_progress 字段
   - 实现丰富的状态管理方法（transition_to_stage, is_refreshing 等）
   - 用 RefreshStage 枚举替代简单的 bool 标志

2. **重构 MaterializeExecutor** 🔄 **进行中**
   - ✅ 定义了 RefreshStage 和 MergeProgress 类型
   - ✅ 添加了 handle_message/handle_chunk_message/handle_barrier_message 辅助方法
   - ❌ select_with_strategy 分流逻辑待实现

3. **分块 Sort-Merge Join** ❌ **待实现**
   - 按 vnode 分块处理逻辑待实现
   - 断点续传机制待实现
   - 进度追踪和持久化待实现

### Phase 3: 协调与恢复 ❌ **待实现**
1. **Barrier 协调增强** ❌ **待实现**
   - 添加新的 barrier 类型（RefreshProgress, RefreshComplete）
   - 实现 barrier 处理逻辑

2. **恢复机制完善** ❌ **待实现**
   - 启动时恢复中断任务
   - 错误处理和重试逻辑

### Phase 4: 集成与测试 ❌ **待实现**
1. **前端集成** ⚠️ **准备中**
   - StreamMaterialize 节点已添加 refresh_progress_table 占位符
   - 具体的 progress table catalog 生成逻辑待实现

2. **测试验证** ❌ **待实现**
   - 容错性测试
   - 大数据量处理测试
   - 故障恢复测试

## 当前实施重点

### 🔥 立即需要完成的任务
1. **数据库迁移文件** - 为 table 添加 refresh_state 和 refresh_progress 字段
2. **select_with_strategy 实现** - MaterializeExecutor 的核心非阻塞逻辑
3. **Progress Table Schema 设计** - 具体的进度表结构和操作

### 📋 实际实现与计划差异
- **进度超预期**: RefreshableMaterializeArgs 的实现比计划更完善，包含了丰富的状态管理 API
- **实现细节调整**: 使用 RefreshStage 枚举替代简单状态，提供更精确的状态跟踪
- **架构优化**: 通过 MaterializeNode.refresh_progress_table 字段集成 progress table

## 关键文件清单

### Proto 定义
- `proto/catalog.proto` - 表状态管理扩展
- `proto/stream_plan.proto` - Progress table 定义

### Meta 层
- `src/meta/src/barrier/command.rs` - 状态协调和新 barrier 类型
- `src/frontend/src/handler/refresh.rs` - 并发控制增强

### Stream 执行层
- `src/stream/src/executor/mview/materialize.rs` - 核心执行逻辑重构
- `src/frontend/src/optimizer/plan_node/stream_materialize.rs` - Progress table 生成

### 支持组件
- `src/stream/src/task/barrier_manager/progress.rs` - 进度报告集成
- `src/meta/src/barrier/context/context_impl.rs` - 恢复逻辑

## 预期效果

### 容错性
- 大数据量刷新支持断点续传
- 系统重启后自动恢复中断的刷新任务
- 细粒度错误处理和恢复

### 系统稳定性
- 消除并发刷新导致的竞态条件
- 防止重复刷新请求
- 保证数据一致性

### 可观测性
- 实时刷新进度报告
- 详细的错误日志和监控指标
- 性能分析和优化数据

### 性能
- 非阻塞执行，保持系统响应性
- 分块处理提高大数据量处理效率
- 资源使用优化

## 风险评估

### 技术风险
- **复杂性增加**: 状态机和分流逻辑增加了系统复杂性
- **性能影响**: Progress table 的频繁更新可能影响性能
- **兼容性**: 需要确保与现有刷新逻辑的兼容性

### 缓解措施
- 充分的单元测试和集成测试
- 性能基准测试和优化
- 渐进式部署和回滚机制

## 总结

本计划通过引入进度跟踪机制、Meta 层状态管理和分阶段执行，彻底解决了 RisingWave 刷新机制的容错性问题。实施后将显著提高系统的可靠性、可观测性和用户体验。
