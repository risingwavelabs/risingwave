// Copyright 2025 RisingWave Labs
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::collections::HashMap;
use std::marker::PhantomData;
use std::sync::Arc;

use anyhow::Context;
use itertools::Itertools;
use risingwave_common::bitmap::{Bitmap, BitmapBuilder};
use risingwave_common::catalog::{ColumnDesc, Field, Schema};
use risingwave_common::hash::table_distribution::TableDistribution;
use risingwave_common::hash::{
    ExpandedWorkerSlotMapping, HashKey, HashKeyDispatcher, VirtualNode, VnodeCountCompat,
    WorkerSlotId,
};
use risingwave_common::memory::MemoryContext;
use risingwave_common::types::{DataType, Datum};
use risingwave_common::util::chunk_coalesce::DataChunkBuilder;
use risingwave_common::util::iter_util::ZipEqFast;
use risingwave_common::util::scan_range::ScanRange;
use risingwave_common::util::tracing::TracingContext;
use risingwave_expr::expr::{BoxedExpression, build_from_prost};
use risingwave_pb::batch_plan::exchange_info::DistributionMode;
use risingwave_pb::batch_plan::exchange_source::LocalExecutePlan::Plan;
use risingwave_pb::batch_plan::plan_node::NodeBody;
use risingwave_pb::batch_plan::{
    ExchangeInfo, ExchangeNode, LocalExecutePlan, PbExchangeSource, PbTaskId, PlanFragment,
    PlanNode, RowSeqScanNode, TaskOutputId,
};
use risingwave_pb::common::{BatchQueryEpoch, WorkerNode};
use risingwave_pb::plan_common::StorageTableDesc;

use super::AsOfDesc;
use crate::error::Result;
use crate::executor::{
    AsOf, BoxedDataChunkStream, BoxedExecutor, BoxedExecutorBuilder, DummyExecutor, Executor,
    ExecutorBuilder, JoinType, LookupJoinBase, unix_timestamp_sec_to_epoch,
};
use crate::task::{BatchTaskContext, ShutdownToken, TaskId};

/// Inner side executor builder for the `LocalLookupJoinExecutor`
struct InnerSideExecutorBuilder {
    table_desc: StorageTableDesc,
    table_distribution: TableDistribution,
    vnode_mapping: ExpandedWorkerSlotMapping,
    outer_side_key_types: Vec<DataType>,
    inner_side_schema: Schema,
    inner_side_column_ids: Vec<i32>,
    inner_side_key_types: Vec<DataType>,
    lookup_prefix_len: usize,
    context: Arc<dyn BatchTaskContext>,
    task_id: TaskId,
    epoch: BatchQueryEpoch,
    worker_slot_mapping: HashMap<WorkerSlotId, WorkerNode>,
    worker_slot_to_scan_range_mapping: HashMap<WorkerSlotId, Vec<(ScanRange, VirtualNode)>>,
    #[expect(dead_code)]
    chunk_size: usize,
    shutdown_rx: ShutdownToken,
    next_stage_id: usize,
    as_of: Option<AsOf>,
}

/// Used to build the executor for the inner side
#[async_trait::async_trait]
pub trait LookupExecutorBuilder: Send {
    fn reset(&mut self);

    async fn add_scan_range(&mut self, key_datums: Vec<Datum>) -> Result<()>;

    async fn build_executor(&mut self) -> Result<BoxedExecutor>;
}

pub type BoxedLookupExecutorBuilder = Box<dyn LookupExecutorBuilder>;

impl InnerSideExecutorBuilder {
    /// Gets the virtual node based on the given `scan_range`
    fn get_virtual_node(&self, scan_range: &ScanRange) -> Result<VirtualNode> {
        let virtual_node = scan_range
            .try_compute_vnode(&self.table_distribution)
            .context("Could not compute vnode for lookup join")?;
        Ok(virtual_node)
    }

    /// Creates the `RowSeqScanNode` that will be used for scanning the inner side table
    /// based on the passed `scan_range` and virtual node.
    fn create_row_seq_scan_node(&self, id: &WorkerSlotId) -> Result<NodeBody> {
        let list = self.worker_slot_to_scan_range_mapping.get(id).unwrap();
        let mut scan_ranges = vec![];
        let mut vnode_bitmap = BitmapBuilder::zeroed(self.vnode_mapping.len());

        list.iter().for_each(|(scan_range, vnode)| {
            scan_ranges.push(scan_range.to_protobuf());
            vnode_bitmap.set(vnode.to_index(), true);
        });

        let row_seq_scan_node = NodeBody::RowSeqScan(RowSeqScanNode {
            table_desc: Some(self.table_desc.clone()),
            column_ids: self.inner_side_column_ids.clone(),
            scan_ranges,
            ordered: false,
            vnode_bitmap: Some(vnode_bitmap.finish().to_protobuf()),
            limit: None,
            as_of: self.as_of.as_ref().map(Into::into),
        });

        Ok(row_seq_scan_node)
    }

    /// Creates the `PbExchangeSource` using the given `id`.
    fn build_prost_exchange_source(&self, id: &WorkerSlotId) -> Result<PbExchangeSource> {
        let worker = self
            .worker_slot_mapping
            .get(id)
            .context("No worker node found for the given worker slot id.")?;

        let local_execute_plan = LocalExecutePlan {
            plan: Some(PlanFragment {
                root: Some(PlanNode {
                    children: vec![],
                    identity: "SeqScan".to_owned(),
                    node_body: Some(self.create_row_seq_scan_node(id)?),
                }),
                exchange_info: Some(ExchangeInfo {
                    mode: DistributionMode::Single as i32,
                    ..Default::default()
                }),
            }),
            epoch: Some(self.epoch),
            tracing_context: TracingContext::from_current_span().to_protobuf(),
        };

        let prost_exchange_source = PbExchangeSource {
            task_output_id: Some(TaskOutputId {
                task_id: Some(PbTaskId {
                    // FIXME: We should replace this random generated uuid to current query_id for
                    // better dashboard. However, due to the lack of info of
                    // stage_id and task_id, we can not do it now. Now just make sure it will not
                    // conflict.
                    query_id: self.task_id.query_id.clone(),
                    stage_id: self.task_id.stage_id + 10000 + self.next_stage_id as u32,
                    task_id: (*id).into(),
                }),
                output_id: 0,
            }),
            host: Some(worker.host.as_ref().unwrap().clone()),
            local_execute_plan: Some(Plan(local_execute_plan)),
        };

        Ok(prost_exchange_source)
    }
}

#[async_trait::async_trait]
impl LookupExecutorBuilder for InnerSideExecutorBuilder {
    fn reset(&mut self) {
        self.worker_slot_to_scan_range_mapping = HashMap::new();
    }

    /// Adds the scan range made from the given `kwy_scalar_impls` into the worker slot id
    /// hash map, along with the scan range's virtual node.
    async fn add_scan_range(&mut self, key_datums: Vec<Datum>) -> Result<()> {
        let mut scan_range = ScanRange::full_table_scan();

        for ((datum, outer_type), inner_type) in key_datums
            .into_iter()
            .zip_eq_fast(
                self.outer_side_key_types
                    .iter()
                    .take(self.lookup_prefix_len),
            )
            .zip_eq_fast(
                self.inner_side_key_types
                    .iter()
                    .take(self.lookup_prefix_len),
            )
        {
            let datum = if inner_type == outer_type {
                datum
            } else {
                bail!("Join key types are not aligned: LHS: {outer_type:?}, RHS: {inner_type:?}");
            };

            scan_range.eq_conds.push(datum);
        }

        let vnode = self.get_virtual_node(&scan_range)?;
        let worker_slot_id = self.vnode_mapping[vnode.to_index()];

        let list = self
            .worker_slot_to_scan_range_mapping
            .entry(worker_slot_id)
            .or_default();
        list.push((scan_range, vnode));

        Ok(())
    }

    /// Builds and returns the `ExchangeExecutor` used for the inner side of the
    /// `LocalLookupJoinExecutor`.
    async fn build_executor(&mut self) -> Result<BoxedExecutor> {
        self.next_stage_id += 1;
        let mut sources = vec![];
        for id in self.worker_slot_to_scan_range_mapping.keys() {
            sources.push(self.build_prost_exchange_source(id)?);
        }

        if sources.is_empty() {
            return Ok(Box::new(DummyExecutor {
                schema: Schema::default(),
            }));
        }

        let exchange_node = NodeBody::Exchange(ExchangeNode {
            sources,
            sequential: true,
            input_schema: self.inner_side_schema.to_prost(),
        });

        let plan_node = PlanNode {
            children: vec![],
            identity: "LocalLookupJoinExchangeExecutor".to_owned(),
            node_body: Some(exchange_node),
        };

        let task_id = self.task_id.clone();

        let executor_builder = ExecutorBuilder::new(
            &plan_node,
            &task_id,
            self.context.clone(),
            self.epoch,
            self.shutdown_rx.clone(),
        );

        executor_builder.build().await
    }
}

/// Local Lookup Join Executor.
/// High level Execution flow:
/// Repeat 1-3:
///   1. Read N rows from outer side input and send keys to inner side builder after deduplication.
///   2. Inner side input lookups inner side table with keys and builds hash map.
///   3. Outer side rows join each inner side rows by probing the hash map.
///
/// Furthermore, we also want to minimize the number of RPC requests we send through the
/// `ExchangeExecutors`. This is done by grouping rows with the same key datums together, and also
/// by grouping together scan ranges that point to the same partition (and can thus be easily
/// scanned by the same worker node).
pub struct LocalLookupJoinExecutor<K> {
    base: LookupJoinBase<K>,
    _phantom: PhantomData<K>,
}

impl<K: HashKey> Executor for LocalLookupJoinExecutor<K> {
    fn schema(&self) -> &Schema {
        &self.base.schema
    }

    fn identity(&self) -> &str {
        &self.base.identity
    }

    fn execute(self: Box<Self>) -> BoxedDataChunkStream {
        Box::new(self.base).do_execute()
    }
}

impl<K> LocalLookupJoinExecutor<K> {
    pub fn new(base: LookupJoinBase<K>) -> Self {
        Self {
            base,
            _phantom: PhantomData,
        }
    }
}

pub struct LocalLookupJoinExecutorBuilder {}

impl BoxedExecutorBuilder for LocalLookupJoinExecutorBuilder {
    async fn new_boxed_executor(
        source: &ExecutorBuilder<'_>,
        inputs: Vec<BoxedExecutor>,
    ) -> Result<BoxedExecutor> {
        let [outer_side_input]: [_; 1] = inputs.try_into().unwrap();

        let lookup_join_node = try_match_expand!(
            source.plan_node().get_node_body().unwrap(),
            NodeBody::LocalLookupJoin
        )?;
        // as_of takes precedence
        let as_of = lookup_join_node
            .as_of
            .as_ref()
            .map(AsOf::try_from)
            .transpose()?;
        let query_epoch = as_of
            .as_ref()
            .map(|a| {
                let epoch = unix_timestamp_sec_to_epoch(a.timestamp).0;
                tracing::debug!(epoch, "time travel");
                risingwave_pb::common::BatchQueryEpoch {
                    epoch: Some(risingwave_pb::common::batch_query_epoch::Epoch::TimeTravel(
                        epoch,
                    )),
                }
            })
            .unwrap_or_else(|| source.epoch());

        let join_type = JoinType::from_prost(lookup_join_node.get_join_type()?);
        let condition = match lookup_join_node.get_condition() {
            Ok(cond_prost) => Some(build_from_prost(cond_prost)?),
            Err(_) => None,
        };

        let output_indices: Vec<usize> = lookup_join_node
            .get_output_indices()
            .iter()
            .map(|&x| x as usize)
            .collect();

        let outer_side_data_types = outer_side_input.schema().data_types();

        let table_desc = lookup_join_node.get_inner_side_table_desc()?;
        let inner_side_column_ids = lookup_join_node.get_inner_side_column_ids().to_vec();

        let inner_side_schema = Schema {
            fields: inner_side_column_ids
                .iter()
                .map(|&id| {
                    let column = table_desc
                        .columns
                        .iter()
                        .find(|c| c.column_id == id)
                        .unwrap();
                    Field::from(&ColumnDesc::from(column))
                })
                .collect_vec(),
        };

        let fields = if join_type == JoinType::LeftSemi || join_type == JoinType::LeftAnti {
            outer_side_input.schema().fields.clone()
        } else {
            [
                outer_side_input.schema().fields.clone(),
                inner_side_schema.fields.clone(),
            ]
            .concat()
        };

        let original_schema = Schema { fields };
        let actual_schema = output_indices
            .iter()
            .map(|&idx| original_schema[idx].clone())
            .collect();

        let mut outer_side_key_idxs = vec![];
        for outer_side_key in lookup_join_node.get_outer_side_key() {
            outer_side_key_idxs.push(*outer_side_key as usize)
        }

        let outer_side_key_types: Vec<DataType> = outer_side_key_idxs
            .iter()
            .map(|&i| outer_side_data_types[i].clone())
            .collect_vec();

        let lookup_prefix_len: usize = lookup_join_node.get_lookup_prefix_len() as usize;

        let mut inner_side_key_idxs = vec![];
        for inner_side_key in lookup_join_node.get_inner_side_key() {
            inner_side_key_idxs.push(*inner_side_key as usize)
        }

        let inner_side_key_types = inner_side_key_idxs
            .iter()
            .map(|&i| inner_side_schema.fields[i].data_type.clone())
            .collect_vec();

        let null_safe = lookup_join_node.get_null_safe().to_vec();

        let vnode_mapping = lookup_join_node
            .get_inner_side_vnode_mapping()
            .iter()
            .copied()
            .map(WorkerSlotId::from)
            .collect_vec();

        assert!(!vnode_mapping.is_empty());

        let chunk_size = source.context().get_config().developer.chunk_size;

        let asof_desc = lookup_join_node
            .asof_desc
            .map(|desc| AsOfDesc::from_protobuf(&desc))
            .transpose()?;

        let worker_nodes = lookup_join_node.get_worker_nodes();
        let worker_slot_mapping: HashMap<WorkerSlotId, WorkerNode> = worker_nodes
            .iter()
            .flat_map(|worker| {
                (0..(worker.compute_node_parallelism()))
                    .map(|i| (WorkerSlotId::new(worker.id, i), worker.clone()))
            })
            .collect();

        let vnodes = Some(Bitmap::ones(table_desc.vnode_count()).into());

        let inner_side_builder = InnerSideExecutorBuilder {
            table_desc: table_desc.clone(),
            table_distribution: TableDistribution::new_from_storage_table_desc(vnodes, table_desc),
            vnode_mapping,
            outer_side_key_types,
            inner_side_schema,
            inner_side_column_ids,
            inner_side_key_types: inner_side_key_types.clone(),
            lookup_prefix_len,
            context: source.context().clone(),
            task_id: source.task_id.clone(),
            epoch: query_epoch,
            worker_slot_to_scan_range_mapping: HashMap::new(),
            chunk_size,
            shutdown_rx: source.shutdown_rx().clone(),
            next_stage_id: 0,
            worker_slot_mapping,
            as_of,
        };

        let identity = source.plan_node().get_identity().clone();
        Ok(LocalLookupJoinExecutorArgs {
            join_type,
            condition,
            outer_side_input,
            outer_side_data_types,
            outer_side_key_idxs,
            inner_side_builder: Box::new(inner_side_builder),
            inner_side_key_types,
            inner_side_key_idxs,
            null_safe,
            lookup_prefix_len,
            chunk_builder: DataChunkBuilder::new(original_schema.data_types(), chunk_size),
            schema: actual_schema,
            output_indices,
            chunk_size,
            asof_desc,
            identity: identity.clone(),
            shutdown_rx: source.shutdown_rx().clone(),
            mem_ctx: source.context().create_executor_mem_context(&identity),
        }
        .dispatch())
    }
}

struct LocalLookupJoinExecutorArgs {
    join_type: JoinType,
    condition: Option<BoxedExpression>,
    outer_side_input: BoxedExecutor,
    outer_side_data_types: Vec<DataType>,
    outer_side_key_idxs: Vec<usize>,
    inner_side_builder: Box<dyn LookupExecutorBuilder>,
    inner_side_key_types: Vec<DataType>,
    inner_side_key_idxs: Vec<usize>,
    null_safe: Vec<bool>,
    lookup_prefix_len: usize,
    chunk_builder: DataChunkBuilder,
    schema: Schema,
    output_indices: Vec<usize>,
    chunk_size: usize,
    asof_desc: Option<AsOfDesc>,
    identity: String,
    shutdown_rx: ShutdownToken,
    mem_ctx: MemoryContext,
}

impl HashKeyDispatcher for LocalLookupJoinExecutorArgs {
    type Output = BoxedExecutor;

    fn dispatch_impl<K: HashKey>(self) -> Self::Output {
        Box::new(LocalLookupJoinExecutor::<K>::new(LookupJoinBase::<K> {
            join_type: self.join_type,
            condition: self.condition,
            outer_side_input: self.outer_side_input,
            outer_side_data_types: self.outer_side_data_types,
            outer_side_key_idxs: self.outer_side_key_idxs,
            inner_side_builder: self.inner_side_builder,
            inner_side_key_types: self.inner_side_key_types,
            inner_side_key_idxs: self.inner_side_key_idxs,
            null_safe: self.null_safe,
            lookup_prefix_len: self.lookup_prefix_len,
            chunk_builder: self.chunk_builder,
            schema: self.schema,
            output_indices: self.output_indices,
            chunk_size: self.chunk_size,
            asof_desc: self.asof_desc,
            identity: self.identity,
            shutdown_rx: self.shutdown_rx,
            mem_ctx: self.mem_ctx,
            _phantom: PhantomData,
        }))
    }

    fn data_types(&self) -> &[DataType] {
        &self.inner_side_key_types
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use risingwave_common::array::{DataChunk, DataChunkTestExt};
    use risingwave_common::catalog::{Field, Schema};
    use risingwave_common::hash::HashKeyDispatcher;
    use risingwave_common::memory::MemoryContext;
    use risingwave_common::types::DataType;
    use risingwave_common::util::chunk_coalesce::DataChunkBuilder;
    use risingwave_common::util::sort_util::{ColumnOrder, OrderType};
    use risingwave_expr::expr::{BoxedExpression, build_from_pretty};

    use super::LocalLookupJoinExecutorArgs;
    use crate::executor::join::JoinType;
    use crate::executor::test_utils::{
        FakeInnerSideExecutorBuilder, MockExecutor, diff_executor_output,
    };
    use crate::executor::{BoxedExecutor, SortExecutor};
    use crate::monitor::BatchSpillMetrics;
    use crate::task::ShutdownToken;

    const CHUNK_SIZE: usize = 1024;

    fn create_outer_side_input() -> BoxedExecutor {
        let schema = Schema {
            fields: vec![
                Field::unnamed(DataType::Int32),
                Field::unnamed(DataType::Float32),
            ],
        };
        let mut executor = MockExecutor::new(schema);

        executor.add(DataChunk::from_pretty(
            "i f
             1 6.1
             2 8.4
             3 3.9",
        ));

        executor.add(DataChunk::from_pretty(
            "i f
             2 5.5
             5 4.1
             5 9.1
             . .",
        ));

        Box::new(executor)
    }

    fn create_lookup_join_executor(
        join_type: JoinType,
        condition: Option<BoxedExpression>,
        null_safe: bool,
    ) -> BoxedExecutor {
        let outer_side_input = create_outer_side_input();

        let fields = if join_type == JoinType::LeftSemi || join_type == JoinType::LeftAnti {
            outer_side_input.schema().fields.clone()
        } else {
            [
                outer_side_input.schema().fields.clone(),
                outer_side_input.schema().fields.clone(),
            ]
            .concat()
        };
        let original_schema = Schema { fields };

        let inner_side_schema = Schema {
            fields: outer_side_input.schema().fields.clone(),
        };

        let inner_side_data_types = inner_side_schema.data_types();
        let outer_side_data_types = outer_side_input.schema().data_types();

        LocalLookupJoinExecutorArgs {
            join_type,
            condition,
            outer_side_input,
            outer_side_data_types,
            outer_side_key_idxs: vec![0],
            inner_side_builder: Box::new(FakeInnerSideExecutorBuilder::new(inner_side_schema)),
            inner_side_key_types: vec![inner_side_data_types[0].clone()],
            inner_side_key_idxs: vec![0],
            null_safe: vec![null_safe],
            lookup_prefix_len: 1,
            chunk_builder: DataChunkBuilder::new(original_schema.data_types(), CHUNK_SIZE),
            schema: original_schema.clone(),
            output_indices: (0..original_schema.len()).collect(),
            chunk_size: CHUNK_SIZE,
            asof_desc: None,
            identity: "TestLookupJoinExecutor".to_owned(),
            shutdown_rx: ShutdownToken::empty(),
            mem_ctx: MemoryContext::none(),
        }
        .dispatch()
    }

    fn create_order_by_executor(child: BoxedExecutor) -> BoxedExecutor {
        let column_orders = vec![
            ColumnOrder {
                column_index: 0,
                order_type: OrderType::ascending(),
            },
            ColumnOrder {
                column_index: 1,
                order_type: OrderType::ascending(),
            },
        ];

        Box::new(SortExecutor::new(
            child,
            Arc::new(column_orders),
            "SortExecutor".into(),
            CHUNK_SIZE,
            MemoryContext::none(),
            None,
            BatchSpillMetrics::for_test(),
        ))
    }

    async fn do_test(
        join_type: JoinType,
        condition: Option<BoxedExpression>,
        null_safe: bool,
        expected: DataChunk,
    ) {
        let lookup_join_executor = create_lookup_join_executor(join_type, condition, null_safe);
        let order_by_executor = create_order_by_executor(lookup_join_executor);
        let mut expected_mock_exec = MockExecutor::new(order_by_executor.schema().clone());
        expected_mock_exec.add(expected);
        diff_executor_output(order_by_executor, Box::new(expected_mock_exec)).await;
    }

    #[tokio::test]
    async fn test_inner_join() {
        let expected = DataChunk::from_pretty(
            "i f   i f
             1 6.1 1 9.2
             2 5.5 2 5.5
             2 5.5 2 4.4
             2 8.4 2 5.5
             2 8.4 2 4.4
             5 4.1 5 2.3
             5 4.1 5 3.7
             5 9.1 5 2.3
             5 9.1 5 3.7",
        );

        do_test(JoinType::Inner, None, false, expected).await;
    }

    #[tokio::test]
    async fn test_null_safe_inner_join() {
        let expected = DataChunk::from_pretty(
            "i f   i f
             1 6.1 1 9.2
             2 5.5 2 5.5
             2 5.5 2 4.4
             2 8.4 2 5.5
             2 8.4 2 4.4
             5 4.1 5 2.3
             5 4.1 5 3.7
             5 9.1 5 2.3
             5 9.1 5 3.7
             .  .  .  .",
        );

        do_test(JoinType::Inner, None, true, expected).await;
    }

    #[tokio::test]
    async fn test_left_outer_join() {
        let expected = DataChunk::from_pretty(
            "i f   i f
             1 6.1 1 9.2
             2 5.5 2 5.5
             2 5.5 2 4.4
             2 8.4 2 5.5
             2 8.4 2 4.4
             3 3.9 . .
             5 4.1 5 2.3
             5 4.1 5 3.7
             5 9.1 5 2.3
             5 9.1 5 3.7
             . .   . .",
        );

        do_test(JoinType::LeftOuter, None, false, expected).await;
    }

    #[tokio::test]
    async fn test_left_semi_join() {
        let expected = DataChunk::from_pretty(
            "i f
             1 6.1
             2 5.5
             2 8.4
             5 4.1
             5 9.1",
        );

        do_test(JoinType::LeftSemi, None, false, expected).await;
    }

    #[tokio::test]
    async fn test_left_anti_join() {
        let expected = DataChunk::from_pretty(
            "i f
             3 3.9
             . .",
        );

        do_test(JoinType::LeftAnti, None, false, expected).await;
    }

    #[tokio::test]
    async fn test_inner_join_with_condition() {
        let expected = DataChunk::from_pretty(
            "i f   i f
             1 6.1 1 9.2
             2 5.5 2 5.5
             2 8.4 2 5.5",
        );
        let condition = build_from_pretty("(less_than:boolean 5:float4 $3:float4)");

        do_test(JoinType::Inner, Some(condition), false, expected).await;
    }

    #[tokio::test]
    async fn test_left_outer_join_with_condition() {
        let expected = DataChunk::from_pretty(
            "i f   i f
             1 6.1 1 9.2
             2 5.5 2 5.5
             2 8.4 2 5.5
             3 3.9 . .
             5 4.1 . .
             5 9.1 . .
             . .   . .",
        );
        let condition = build_from_pretty("(less_than:boolean 5:float4 $3:float4)");

        do_test(JoinType::LeftOuter, Some(condition), false, expected).await;
    }

    #[tokio::test]
    async fn test_left_semi_join_with_condition() {
        let expected = DataChunk::from_pretty(
            "i f
             1 6.1
             2 5.5
             2 8.4",
        );
        let condition = build_from_pretty("(less_than:boolean 5:float4 $3:float4)");

        do_test(JoinType::LeftSemi, Some(condition), false, expected).await;
    }

    #[tokio::test]
    async fn test_left_anti_join_with_condition() {
        let expected = DataChunk::from_pretty(
            "i f
            3 3.9
            5 4.1
            5 9.1
            . .",
        );
        let condition = build_from_pretty("(less_than:boolean 5:float4 $3:float4)");

        do_test(JoinType::LeftAnti, Some(condition), false, expected).await;
    }
}
