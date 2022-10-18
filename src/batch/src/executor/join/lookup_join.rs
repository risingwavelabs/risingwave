// Copyright 2022 Singularity Data
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::collections::HashMap;
use std::marker::PhantomData;

use fixedbitset::FixedBitSet;
use futures::StreamExt;
use futures_async_stream::try_stream;
use itertools::Itertools;
use risingwave_common::array::{DataChunk, Row};
use risingwave_common::buffer::BitmapBuilder;
use risingwave_common::catalog::{ColumnDesc, Field, Schema};
use risingwave_common::error::{internal_error, Result, RwError};
use risingwave_common::hash::{HashKey, HashKeyDispatcher, PrecomputedBuildHasher};
use risingwave_common::types::{
    DataType, Datum, ParallelUnitId, ToOwnedDatum, VirtualNode, VnodeMapping,
};
use risingwave_common::util::chunk_coalesce::DataChunkBuilder;
use risingwave_common::util::scan_range::ScanRange;
use risingwave_common::util::worker_util::get_pu_to_worker_mapping;
use risingwave_expr::expr::expr_unary::new_unary_expr;
use risingwave_expr::expr::{build_from_prost, BoxedExpression, LiteralExpression};
use risingwave_pb::batch_plan::exchange_info::DistributionMode;
use risingwave_pb::batch_plan::exchange_source::LocalExecutePlan::Plan;
use risingwave_pb::batch_plan::plan_node::NodeBody;
use risingwave_pb::batch_plan::{
    ExchangeInfo, ExchangeNode, ExchangeSource as ProstExchangeSource, LocalExecutePlan,
    PlanFragment, PlanNode, RowSeqScanNode, TaskId as ProstTaskId, TaskOutputId,
};
use risingwave_pb::common::{BatchQueryEpoch, WorkerNode};
use risingwave_pb::expr::expr_node::Type;
use risingwave_pb::plan_common::StorageTableDesc;
use uuid::Uuid;

use crate::executor::join::chunked_data::ChunkedData;
use crate::executor::join::JoinType;
use crate::executor::{
    utils, BoxedDataChunkListStream, BoxedDataChunkStream, BoxedExecutor, BoxedExecutorBuilder,
    BufferChunkExecutor, EquiJoinParams, Executor, ExecutorBuilder, HashJoinExecutor, JoinHashMap,
    RowId,
};
use crate::task::{BatchTaskContext, TaskId};

struct DummyExecutor {
    schema: Schema,
}

impl Executor for DummyExecutor {
    fn schema(&self) -> &Schema {
        &self.schema
    }

    fn identity(&self) -> &str {
        "dummy"
    }

    fn execute(self: Box<Self>) -> BoxedDataChunkStream {
        DummyExecutor::do_nothing()
    }
}

impl DummyExecutor {
    #[try_stream(boxed, ok = DataChunk, error = RwError)]
    async fn do_nothing() {}
}

/// Inner side executor builder for the `LookupJoinExecutor`
pub struct InnerSideExecutorBuilder<C> {
    table_desc: StorageTableDesc,
    vnode_mapping: VnodeMapping,
    outer_side_key_types: Vec<DataType>,
    inner_side_schema: Schema,
    inner_side_column_ids: Vec<i32>,
    inner_side_key_types: Vec<DataType>,
    context: C,
    task_id: TaskId,
    epoch: BatchQueryEpoch,
    pu_to_worker_mapping: HashMap<ParallelUnitId, WorkerNode>,
    pu_to_scan_range_mapping: HashMap<ParallelUnitId, Vec<(ScanRange, VirtualNode)>>,
    chunk_size: usize,
}

/// Used to build the executor for the inner side
#[async_trait::async_trait]
pub trait LookupExecutorBuilder: Send {
    fn reset(&mut self);

    fn add_scan_range(&mut self, key_datums: &[Datum]) -> Result<()>;

    async fn build_executor(&self) -> Result<BoxedExecutor>;
}

type BoxedLookupExecutorBuilder = Box<dyn LookupExecutorBuilder>;

impl<C: BatchTaskContext> InnerSideExecutorBuilder<C> {
    /// Gets the virtual node based on the given `scan_range`
    fn get_virtual_node(&self, scan_range: &ScanRange) -> Result<VirtualNode> {
        let dist_keys = self
            .table_desc
            .dist_key_indices
            .iter()
            .map(|&k| k as usize)
            .collect_vec();
        let pk_indices = self
            .table_desc
            .pk
            .iter()
            .map(|col| col.index as _)
            .collect_vec();

        let virtual_node = scan_range.try_compute_vnode(&dist_keys, &pk_indices);
        virtual_node.ok_or_else(|| internal_error("Could not compute vnode for lookup join"))
    }

    /// Creates the `RowSeqScanNode` that will be used for scanning the inner side table
    /// based on the passed `scan_range` and virtual node.
    fn create_row_seq_scan_node(&self, id: &ParallelUnitId) -> Result<NodeBody> {
        let list = self.pu_to_scan_range_mapping.get(id).unwrap();
        let mut scan_ranges = vec![];
        let mut vnode_bitmap = BitmapBuilder::zeroed(self.vnode_mapping.len());

        list.iter().for_each(|(scan_range, vnode)| {
            scan_ranges.push(scan_range.to_protobuf());
            vnode_bitmap.set(*vnode as usize, true);
        });

        let row_seq_scan_node = NodeBody::RowSeqScan(RowSeqScanNode {
            table_desc: Some(self.table_desc.clone()),
            column_ids: self.inner_side_column_ids.clone(),
            scan_ranges,
            vnode_bitmap: Some(vnode_bitmap.finish().to_protobuf()),
        });

        Ok(row_seq_scan_node)
    }

    /// Creates the `ProstExchangeSource` using the given `id`.
    fn build_prost_exchange_source(&self, id: &ParallelUnitId) -> Result<ProstExchangeSource> {
        let worker = self.pu_to_worker_mapping.get(id).ok_or_else(|| {
            internal_error("No worker node found for the given parallel unit id.")
        })?;

        let local_execute_plan = LocalExecutePlan {
            plan: Some(PlanFragment {
                root: Some(PlanNode {
                    children: vec![],
                    identity: Uuid::new_v4().to_string(),
                    node_body: Some(self.create_row_seq_scan_node(id)?),
                }),
                exchange_info: Some(ExchangeInfo {
                    mode: DistributionMode::Single as i32,
                    ..Default::default()
                }),
            }),
            epoch: Some(self.epoch.clone()),
        };

        let prost_exchange_source = ProstExchangeSource {
            task_output_id: Some(TaskOutputId {
                task_id: Some(ProstTaskId {
                    // FIXME: We should replace this random generated uuid to current query_id for
                    // better dashboard. However, due to the lack of info of
                    // stage_id and task_id, we can not do it now. Now just make sure it will not
                    // conflict.
                    query_id: Uuid::new_v4().to_string(),
                    ..Default::default()
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
impl<C: BatchTaskContext> LookupExecutorBuilder for InnerSideExecutorBuilder<C> {
    fn reset(&mut self) {
        self.pu_to_scan_range_mapping = HashMap::new();
    }

    /// Adds the scan range made from the given `kwy_scalar_impls` into the parallel unit id
    /// hash map, along with the scan range's virtual node.
    fn add_scan_range(&mut self, key_datums: &[Datum]) -> Result<()> {
        let mut scan_range = ScanRange::full_table_scan();

        for ((datum, outer_type), inner_type) in key_datums
            .iter()
            .zip_eq(self.outer_side_key_types.iter())
            .zip_eq(self.inner_side_key_types.iter())
        {
            let datum = if inner_type == outer_type {
                datum.clone()
            } else {
                let cast_expr = new_unary_expr(
                    Type::Cast,
                    inner_type.clone(),
                    Box::new(LiteralExpression::new(outer_type.clone(), datum.clone())),
                )?;

                cast_expr.eval_row(Row::empty())?
            };

            scan_range.eq_conds.push(datum);
        }

        let vnode = self.get_virtual_node(&scan_range)?;
        let parallel_unit_id = self.vnode_mapping[vnode as usize];

        let list = self
            .pu_to_scan_range_mapping
            .entry(parallel_unit_id)
            .or_default();
        list.push((scan_range, vnode));

        Ok(())
    }

    /// Builds and returns the `ExchangeExecutor` used for the inner side of the
    /// `LookupJoinExecutor`.
    async fn build_executor(&self) -> Result<BoxedExecutor> {
        let mut sources = vec![];
        for id in self.pu_to_scan_range_mapping.keys() {
            sources.push(self.build_prost_exchange_source(id)?);
        }

        if sources.is_empty() {
            return Ok(Box::new(DummyExecutor {
                schema: Schema::default(),
            }));
        }

        let exchange_node = NodeBody::Exchange(ExchangeNode {
            sources,
            input_schema: self.inner_side_schema.to_prost(),
        });

        let plan_node = PlanNode {
            children: vec![],
            identity: "LookupJoinExchangeExecutor".to_string(),
            node_body: Some(exchange_node),
        };

        let task_id = self.task_id.clone();

        let executor_builder = ExecutorBuilder::new(
            &plan_node,
            &task_id,
            self.context.clone(),
            self.epoch.clone(),
        );

        executor_builder.build().await
    }
}

/// Lookup Join Executor.
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
pub struct LookupJoinExecutor<K> {
    join_type: JoinType,
    condition: Option<BoxedExpression>,
    outer_side_input: BoxedExecutor,
    outer_side_data_types: Vec<DataType>, // Data types of all columns of outer side table
    outer_side_key_idxs: Vec<usize>,
    inner_side_builder: BoxedLookupExecutorBuilder,
    inner_side_key_types: Vec<DataType>, // Data types only of key columns of inner side table
    inner_side_key_idxs: Vec<usize>,
    null_safe: Vec<bool>,
    chunk_builder: DataChunkBuilder,
    schema: Schema,
    output_indices: Vec<usize>,
    identity: String,
    chunk_size: usize,
    _phantom: PhantomData<K>,
}

const AT_LEAST_OUTER_SIDE_ROWS: usize = 512;

impl<K: HashKey> Executor for LookupJoinExecutor<K> {
    fn schema(&self) -> &Schema {
        &self.schema
    }

    fn identity(&self) -> &str {
        &self.identity
    }

    fn execute(self: Box<Self>) -> BoxedDataChunkStream {
        self.do_execute()
    }
}

impl<K> LookupJoinExecutor<K> {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        join_type: JoinType,
        condition: Option<BoxedExpression>,
        outer_side_input: BoxedExecutor,
        outer_side_data_types: Vec<DataType>,
        outer_side_key_idxs: Vec<usize>,
        inner_side_builder: Box<dyn LookupExecutorBuilder>,
        inner_side_key_types: Vec<DataType>,
        inner_side_key_idxs: Vec<usize>,
        null_safe: Vec<bool>,
        chunk_builder: DataChunkBuilder,
        schema: Schema,
        output_indices: Vec<usize>,
        identity: String,
        chunk_size: usize,
    ) -> Self {
        Self {
            join_type,
            condition,
            outer_side_input,
            outer_side_data_types,
            outer_side_key_idxs,
            inner_side_builder,
            inner_side_key_types,
            inner_side_key_idxs,
            null_safe,
            chunk_builder,
            schema,
            output_indices,
            identity,
            chunk_size,
            _phantom: PhantomData,
        }
    }
}

impl<K: HashKey> LookupJoinExecutor<K> {
    #[try_stream(boxed, ok = DataChunk, error = RwError)]
    async fn do_execute(mut self: Box<Self>) {
        let outer_side_schema = self.outer_side_input.schema().clone();

        let null_matched = {
            let mut null_matched = FixedBitSet::with_capacity(self.null_safe.len());
            for (idx, col_null_matched) in self.null_safe.iter().copied().enumerate() {
                null_matched.set(idx, col_null_matched);
            }
            null_matched
        };

        let mut outer_side_batch_read_stream: BoxedDataChunkListStream =
            utils::batch_read(self.outer_side_input.execute(), AT_LEAST_OUTER_SIDE_ROWS);

        while let Some(chunk_list) = outer_side_batch_read_stream.next().await {
            let chunk_list = chunk_list?;

            // Group rows with the same key datums together
            let groups = chunk_list
                .iter()
                .flat_map(|chunk| {
                    chunk.rows().map(|row| {
                        self.outer_side_key_idxs
                            .iter()
                            .map(|&idx| row.value_at(idx).to_owned_datum())
                            .collect_vec()
                    })
                })
                .sorted()
                .dedup()
                .collect_vec();

            self.inner_side_builder.reset();
            for row_key in &groups {
                self.inner_side_builder.add_scan_range(row_key)?;
            }
            let inner_side_input = self.inner_side_builder.build_executor().await?;

            // Lookup join outer side will become the probe side of hash join,
            // while its inner side will become the build side of hash join.
            let hash_join_probe_side_input = Box::new(BufferChunkExecutor::new(
                outer_side_schema.clone(),
                chunk_list,
            ));
            let hash_join_build_side_input = inner_side_input;
            let hash_join_probe_data_types = self.outer_side_data_types.clone();
            let hash_join_build_data_types = hash_join_build_side_input.schema().data_types();
            let hash_join_probe_side_key_idxs = self.outer_side_key_idxs.clone();
            let hash_join_build_side_key_idxs = self.inner_side_key_idxs.clone();

            let full_data_types = [
                hash_join_probe_data_types.clone(),
                hash_join_build_data_types.clone(),
            ]
            .concat();

            let mut build_side = Vec::new();
            let mut build_row_count = 0;
            #[for_await]
            for build_chunk in hash_join_build_side_input.execute() {
                let build_chunk = build_chunk?;
                if build_chunk.cardinality() > 0 {
                    build_row_count += build_chunk.cardinality();
                    build_side.push(build_chunk.compact())
                }
            }
            let mut hash_map =
                JoinHashMap::with_capacity_and_hasher(build_row_count, PrecomputedBuildHasher);
            let mut next_build_row_with_same_key =
                ChunkedData::with_chunk_sizes(build_side.iter().map(|c| c.capacity()))?;

            // Build hash map
            for (build_chunk_id, build_chunk) in build_side.iter().enumerate() {
                let build_keys = K::build(&hash_join_build_side_key_idxs, build_chunk)?;

                for (build_row_id, build_key) in build_keys.into_iter().enumerate() {
                    // Only insert key to hash map if it is consistent with the null safe
                    // restriction.
                    if build_key.null_bitmap().is_subset(&null_matched) {
                        let row_id = RowId::new(build_chunk_id, build_row_id);
                        next_build_row_with_same_key[row_id] = hash_map.insert(build_key, row_id);
                    }
                }
            }

            let params = EquiJoinParams::new(
                hash_join_probe_side_input,
                hash_join_probe_data_types,
                hash_join_probe_side_key_idxs,
                build_side,
                hash_join_build_data_types,
                full_data_types,
                hash_map,
                next_build_row_with_same_key,
                self.chunk_size,
            );

            if let Some(cond) = self.condition.as_ref() {
                let stream = match self.join_type {
                    JoinType::Inner => {
                        HashJoinExecutor::do_inner_join_with_non_equi_condition(params, cond)
                    }
                    JoinType::LeftOuter => {
                        HashJoinExecutor::do_left_outer_join_with_non_equi_condition(params, cond)
                    }
                    JoinType::LeftSemi => {
                        HashJoinExecutor::do_left_semi_join_with_non_equi_condition(params, cond)
                    }
                    JoinType::LeftAnti => {
                        HashJoinExecutor::do_left_anti_join_with_non_equi_condition(params, cond)
                    }
                    JoinType::RightOuter
                    | JoinType::RightSemi
                    | JoinType::RightAnti
                    | JoinType::FullOuter => unimplemented!(),
                };
                // For non-equi join, we need an output chunk builder to align the output chunks.
                let mut output_chunk_builder =
                    DataChunkBuilder::new(self.schema.data_types(), self.chunk_size);
                #[for_await]
                for chunk in stream {
                    #[for_await]
                    for output_chunk in output_chunk_builder
                        .trunc_data_chunk(chunk?.reorder_columns(&self.output_indices))
                    {
                        yield output_chunk
                    }
                }
                if let Some(output_chunk) = output_chunk_builder.consume_all() {
                    yield output_chunk
                }
            } else {
                let stream = match self.join_type {
                    JoinType::Inner => HashJoinExecutor::do_inner_join(params),
                    JoinType::LeftOuter => HashJoinExecutor::do_left_outer_join(params),
                    JoinType::LeftSemi => HashJoinExecutor::do_left_semi_anti_join::<false>(params),
                    JoinType::LeftAnti => HashJoinExecutor::do_left_semi_anti_join::<true>(params),
                    JoinType::RightOuter
                    | JoinType::RightSemi
                    | JoinType::RightAnti
                    | JoinType::FullOuter => unimplemented!(),
                };
                #[for_await]
                for chunk in stream {
                    yield chunk?.reorder_columns(&self.output_indices)
                }
            }
        }
    }
}

pub struct LookupJoinExecutorBuilder {}

#[async_trait::async_trait]
impl BoxedExecutorBuilder for LookupJoinExecutorBuilder {
    async fn new_boxed_executor<C: BatchTaskContext>(
        source: &ExecutorBuilder<'_, C>,
        inputs: Vec<BoxedExecutor>,
    ) -> Result<BoxedExecutor> {
        let [outer_side_input]: [_; 1] = inputs.try_into().unwrap();

        let lookup_join_node = try_match_expand!(
            source.plan_node().get_node_body().unwrap(),
            NodeBody::LookupJoin
        )?;

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

        let mut inner_side_key_idxs = vec![];
        for pk in &table_desc.pk {
            let key_idx = inner_side_column_ids
                .iter()
                .position(|&i| table_desc.columns[pk.index as usize].column_id == i)
                .ok_or_else(|| {
                    internal_error("Inner side key is not part of its output columns")
                })?;
            inner_side_key_idxs.push(key_idx);
        }

        let inner_side_key_types = inner_side_key_idxs
            .iter()
            .map(|&i| inner_side_schema.fields[i].data_type.clone())
            .collect_vec();

        let null_safe = lookup_join_node.get_null_safe().to_vec();

        let vnode_mapping = lookup_join_node.get_inner_side_vnode_mapping().to_vec();
        assert!(!vnode_mapping.is_empty());

        let chunk_size = source.context.get_config().developer.batch_chunk_size;

        let inner_side_builder = InnerSideExecutorBuilder {
            table_desc: table_desc.clone(),
            vnode_mapping,
            outer_side_key_types,
            inner_side_schema,
            inner_side_column_ids,
            inner_side_key_types: inner_side_key_types.clone(),
            context: source.context().clone(),
            task_id: source.task_id.clone(),
            epoch: source.epoch(),
            pu_to_worker_mapping: get_pu_to_worker_mapping(lookup_join_node.get_worker_nodes()),
            pu_to_scan_range_mapping: HashMap::new(),
            chunk_size,
        };

        Ok(LookupJoinExecutorArgs {
            join_type,
            condition,
            outer_side_input,
            outer_side_data_types,
            outer_side_key_idxs,
            inner_side_builder: Box::new(inner_side_builder),
            inner_side_key_types,
            inner_side_key_idxs,
            null_safe,
            chunk_builder: DataChunkBuilder::new(original_schema.data_types(), chunk_size),
            schema: actual_schema,
            output_indices,
            chunk_size,
            identity: source.plan_node().get_identity().clone(),
        }
        .dispatch())
    }
}

struct LookupJoinExecutorArgs {
    join_type: JoinType,
    condition: Option<BoxedExpression>,
    outer_side_input: BoxedExecutor,
    outer_side_data_types: Vec<DataType>,
    outer_side_key_idxs: Vec<usize>,
    inner_side_builder: Box<dyn LookupExecutorBuilder>,
    inner_side_key_types: Vec<DataType>,
    inner_side_key_idxs: Vec<usize>,
    null_safe: Vec<bool>,
    chunk_builder: DataChunkBuilder,
    schema: Schema,
    output_indices: Vec<usize>,
    chunk_size: usize,
    identity: String,
}

impl HashKeyDispatcher for LookupJoinExecutorArgs {
    type Output = BoxedExecutor;

    fn dispatch_impl<K: HashKey>(self) -> Self::Output {
        Box::new(LookupJoinExecutor::<K>::new(
            self.join_type,
            self.condition,
            self.outer_side_input,
            self.outer_side_data_types,
            self.outer_side_key_idxs,
            self.inner_side_builder,
            self.inner_side_key_types,
            self.inner_side_key_idxs,
            self.null_safe,
            self.chunk_builder,
            self.schema,
            self.output_indices,
            self.identity,
            self.chunk_size,
        ))
    }

    fn data_types(&self) -> &[DataType] {
        &self.inner_side_key_types
    }
}

#[cfg(test)]
mod tests {
    use risingwave_common::array::{DataChunk, DataChunkTestExt};
    use risingwave_common::catalog::{Field, Schema};
    use risingwave_common::hash::HashKeyDispatcher;
    use risingwave_common::types::{DataType, ScalarImpl};
    use risingwave_common::util::chunk_coalesce::DataChunkBuilder;
    use risingwave_common::util::sort_util::{OrderPair, OrderType};
    use risingwave_expr::expr::expr_binary_nonnull::new_binary_expr;
    use risingwave_expr::expr::{BoxedExpression, InputRefExpression, LiteralExpression};
    use risingwave_pb::expr::expr_node::Type;

    use super::LookupJoinExecutorArgs;
    use crate::executor::join::JoinType;
    use crate::executor::test_utils::{
        diff_executor_output, FakeInnerSideExecutorBuilder, MockExecutor,
    };
    use crate::executor::{BoxedExecutor, OrderByExecutor};

    const CHUNK_SIZE: usize = 1024;

    pub struct MockGatherExecutor {
        chunks: Vec<DataChunk>,
    }

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

        LookupJoinExecutorArgs {
            join_type,
            condition,
            outer_side_input,
            outer_side_data_types,
            outer_side_key_idxs: vec![0],
            inner_side_builder: Box::new(FakeInnerSideExecutorBuilder::new(inner_side_schema)),
            inner_side_key_types: vec![inner_side_data_types[0].clone()],
            inner_side_key_idxs: vec![0],
            null_safe: vec![null_safe],
            chunk_builder: DataChunkBuilder::new(original_schema.data_types(), CHUNK_SIZE),
            schema: original_schema.clone(),
            output_indices: (0..original_schema.len()).into_iter().collect(),
            chunk_size: CHUNK_SIZE,
            identity: "TestLookupJoinExecutor".to_string(),
        }
        .dispatch()
    }

    fn create_order_by_executor(child: BoxedExecutor) -> BoxedExecutor {
        let order_pairs = vec![
            OrderPair {
                column_idx: 0,
                order_type: OrderType::Ascending,
            },
            OrderPair {
                column_idx: 1,
                order_type: OrderType::Ascending,
            },
        ];

        Box::new(OrderByExecutor::new(
            child,
            order_pairs,
            "OrderByExecutor".into(),
            CHUNK_SIZE,
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

        let condition = Some(
            new_binary_expr(
                Type::LessThan,
                DataType::Boolean,
                Box::new(LiteralExpression::new(
                    DataType::Int32,
                    Some(ScalarImpl::Int32(5)),
                )),
                Box::new(InputRefExpression::new(DataType::Float32, 3)),
            )
            .unwrap(),
        );

        do_test(JoinType::Inner, condition, false, expected).await;
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

        let condition = Some(
            new_binary_expr(
                Type::LessThan,
                DataType::Boolean,
                Box::new(LiteralExpression::new(
                    DataType::Int32,
                    Some(ScalarImpl::Int32(5)),
                )),
                Box::new(InputRefExpression::new(DataType::Float32, 3)),
            )
            .unwrap(),
        );

        do_test(JoinType::LeftOuter, condition, false, expected).await;
    }

    #[tokio::test]
    async fn test_left_semi_join_with_condition() {
        let expected = DataChunk::from_pretty(
            "i f
             1 6.1
             2 5.5
             2 8.4",
        );

        let condition = Some(
            new_binary_expr(
                Type::LessThan,
                DataType::Boolean,
                Box::new(LiteralExpression::new(
                    DataType::Int32,
                    Some(ScalarImpl::Int32(5)),
                )),
                Box::new(InputRefExpression::new(DataType::Float32, 3)),
            )
            .unwrap(),
        );

        do_test(JoinType::LeftSemi, condition, false, expected).await;
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

        let condition = Some(
            new_binary_expr(
                Type::LessThan,
                DataType::Boolean,
                Box::new(LiteralExpression::new(
                    DataType::Int32,
                    Some(ScalarImpl::Int32(5)),
                )),
                Box::new(InputRefExpression::new(DataType::Float32, 3)),
            )
            .unwrap(),
        );

        do_test(JoinType::LeftAnti, condition, false, expected).await;
    }
}
