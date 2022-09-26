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
use risingwave_common::hash::{
    calc_hash_key_kind, HashKey, HashKeyDispatcher, PrecomputedBuildHasher,
};
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
use risingwave_pb::common::WorkerNode;
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

// Build side = "Left side", where we go through its rows one by one
// Probe side = "Right side", where we find matches for each row from the build side

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

/// Probe side source for the `LookupJoinExecutor`
pub struct ProbeSideSource<C> {
    table_desc: StorageTableDesc,
    vnode_mapping: VnodeMapping,
    build_side_key_types: Vec<DataType>,
    probe_side_schema: Schema,
    probe_side_column_ids: Vec<i32>,
    probe_side_key_types: Vec<DataType>,
    context: C,
    task_id: TaskId,
    epoch: u64,
    pu_to_worker_mapping: HashMap<ParallelUnitId, WorkerNode>,
    pu_to_scan_range_mapping: HashMap<ParallelUnitId, Vec<(ScanRange, VirtualNode)>>,
}

/// Used to build the executor for the probe side
#[async_trait::async_trait]
pub trait ProbeSideSourceBuilder: Send {
    fn reset(&mut self);

    fn add_scan_range(&mut self, key_datums: &[Datum]) -> Result<()>;

    async fn build_source(&self) -> Result<BoxedExecutor>;
}

impl<C: BatchTaskContext> ProbeSideSource<C> {
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

    /// Creates the `RowSeqScanNode` that will be used for scanning the probe side table
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
            column_ids: self.probe_side_column_ids.clone(),
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
            epoch: self.epoch,
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
impl<C: BatchTaskContext> ProbeSideSourceBuilder for ProbeSideSource<C> {
    fn reset(&mut self) {
        self.pu_to_scan_range_mapping = HashMap::new();
    }

    /// Adds the scan range made from the given `kwy_scalar_impls` into the parallel unit id
    /// hash map, along with the scan range's virtual node.
    fn add_scan_range(&mut self, key_datums: &[Datum]) -> Result<()> {
        let mut scan_range = ScanRange::full_table_scan();

        for ((datum, build_type), probe_type) in key_datums
            .iter()
            .zip_eq(self.build_side_key_types.iter())
            .zip_eq(self.probe_side_key_types.iter())
        {
            let datum = if probe_type == build_type {
                datum.clone()
            } else {
                let cast_expr = new_unary_expr(
                    Type::Cast,
                    probe_type.clone(),
                    Box::new(LiteralExpression::new(build_type.clone(), datum.clone())),
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
            .or_insert(vec![]);
        list.push((scan_range, vnode));

        Ok(())
    }

    /// Builds and returns the `ExchangeExecutor` used for the probe side of the
    /// `LookupJoinExecutor`.
    async fn build_source(&self) -> Result<BoxedExecutor> {
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
            input_schema: self.probe_side_schema.to_prost(),
        });

        let plan_node = PlanNode {
            children: vec![],
            identity: "LookupJoinExchangeExecutor".to_string(),
            node_body: Some(exchange_node),
        };

        let task_id = self.task_id.clone();

        let executor_builder =
            ExecutorBuilder::new(&plan_node, &task_id, self.context.clone(), self.epoch);

        executor_builder.build().await
    }
}

/// Lookup Join Executor.
/// High-level idea:
/// 1) Group together build side rows with the same datums on the key columns
/// 2) Create all the `ExchangeExecutors` for scanning the rows on the probe side table
/// 3) Execute the `ExchangeExecutors` and get the probe side chunks
/// 4) For each group of rows R1, determine the rows R2 in the probe side chunks that have the
///    same key datums as the rows in R1.
/// 5) Join R1 and R2 together based on the join type and condition.
/// 6) Repeat 4-5) for every row on the build side.
///
/// The actual implementation of this high level idea is much more complicated as we receive
/// rows on the build side and probe side chunk-by-chunk instead of all of once.
///
/// Furthermore, we also want to minimize the number of RPC requests we send through the
/// `ExchangeExecutors`. This is done by grouping rows with the same key datums together, and also
/// by grouping together scan ranges that point to the same partition (and can thus be easily
/// scanned by the same worker node).
pub struct LookupJoinExecutor<K> {
    join_type: JoinType,
    condition: Option<BoxedExpression>,
    build_child: Option<BoxedExecutor>,
    build_side_data_types: Vec<DataType>, // Data types of all columns of build side table
    build_side_key_idxs: Vec<usize>,
    probe_side_source: Box<dyn ProbeSideSourceBuilder>,
    probe_side_key_types: Vec<DataType>, // Data types only of key columns of probe side table
    probe_side_key_idxs: Vec<usize>,
    null_safe: Vec<bool>,
    chunk_builder: DataChunkBuilder,
    schema: Schema,
    output_indices: Vec<usize>,
    identity: String,
    _phantom: PhantomData<K>,
}

const AT_LEAST_BUILD_SIDE_ROWS: usize = 512;

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
        build_child: Option<BoxedExecutor>,
        build_side_data_types: Vec<DataType>,
        build_side_key_idxs: Vec<usize>,
        probe_side_source: Box<dyn ProbeSideSourceBuilder>,
        probe_side_key_types: Vec<DataType>,
        probe_side_key_idxs: Vec<usize>,
        null_safe: Vec<bool>,
        chunk_builder: DataChunkBuilder,
        schema: Schema,
        output_indices: Vec<usize>,
        identity: String,
    ) -> Self {
        Self {
            join_type,
            condition,
            build_child,
            build_side_data_types,
            build_side_key_idxs,
            probe_side_source,
            probe_side_key_types,
            probe_side_key_idxs,
            null_safe,
            chunk_builder,
            schema,
            output_indices,
            identity,
            _phantom: PhantomData,
        }
    }
}

impl<K: HashKey> LookupJoinExecutor<K> {
    #[try_stream(boxed, ok = DataChunk, error = RwError)]
    async fn do_execute(mut self: Box<Self>) {
        let lookup_join_build_side_schema = self.build_child.as_ref().unwrap().schema().clone();
        let mut lookup_join_build_side_batch_read_stream: BoxedDataChunkListStream =
            utils::batch_read(
                self.build_child.take().unwrap().execute(),
                AT_LEAST_BUILD_SIDE_ROWS,
            );

        while let Some(chunk_list) = lookup_join_build_side_batch_read_stream.next().await {
            let chunk_list = chunk_list?;

            // Group rows with the same key datums together
            let groups = chunk_list
                .iter()
                .flat_map(|chunk| {
                    chunk.rows().map(|row| {
                        self.build_side_key_idxs
                            .iter()
                            .map(|&idx| row.value_at(idx).to_owned_datum())
                            .collect_vec()
                    })
                })
                .sorted()
                .dedup()
                .collect_vec();

            self.probe_side_source.reset();
            for row_key in &groups {
                self.probe_side_source.add_scan_range(row_key)?;
            }
            let lookup_join_probe_child = self.probe_side_source.build_source().await?;

            // NOTICE!!!
            // Lookup join build side will become the probe side of hash join,
            // while its probe side will become the build side of hash join.
            let hash_join_probe_child = Box::new(BufferChunkExecutor::new(
                lookup_join_build_side_schema.clone(),
                chunk_list,
            ));
            let hash_join_build_child = lookup_join_probe_child;
            let hash_join_probe_data_types = self.build_side_data_types.clone();
            let hash_join_build_data_types = hash_join_build_child.schema().data_types();
            let hash_join_probe_side_key_idxs = self.build_side_key_idxs.clone();
            let hash_join_build_side_key_idxs = self.probe_side_key_idxs.clone();

            let full_data_types = [
                hash_join_probe_data_types.clone(),
                hash_join_build_data_types.clone(),
            ]
            .concat();

            let mut build_side = Vec::new();
            let mut build_row_count = 0;
            #[for_await]
            for build_chunk in hash_join_build_child.execute() {
                // for build_chunk in chunk_list {
                let build_chunk = build_chunk?;
                if build_chunk.cardinality() > 0 {
                    build_row_count += build_chunk.cardinality();
                    build_side.push(build_chunk.compact()?)
                }
            }
            let mut hash_map =
                JoinHashMap::with_capacity_and_hasher(build_row_count, PrecomputedBuildHasher);
            let mut next_build_row_with_same_key =
                ChunkedData::with_chunk_sizes(build_side.iter().map(|c| c.capacity()))?;
            let null_matched = {
                let mut null_matched = FixedBitSet::with_capacity(self.null_safe.len());
                for (idx, col_null_matched) in self.null_safe.iter().copied().enumerate() {
                    null_matched.set(idx, col_null_matched);
                }
                null_matched
            };

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
                hash_join_probe_child,
                hash_join_probe_data_types,
                hash_join_probe_side_key_idxs,
                build_side,
                hash_join_build_data_types,
                full_data_types,
                hash_map,
                next_build_row_with_same_key,
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
                    JoinType::RightOuter => {
                        HashJoinExecutor::do_right_outer_join_with_non_equi_condition(params, cond)
                    }
                    JoinType::RightSemi => {
                        HashJoinExecutor::do_right_semi_anti_join_with_non_equi_condition::<false>(
                            params, cond,
                        )
                    }
                    JoinType::RightAnti => {
                        HashJoinExecutor::do_right_semi_anti_join_with_non_equi_condition::<true>(
                            params, cond,
                        )
                    }
                    JoinType::FullOuter => {
                        HashJoinExecutor::do_full_outer_join_with_non_equi_condition(params, cond)
                    }
                };
                // For non-equi join, we need an output chunk builder to align the output chunks.
                let mut output_chunk_builder =
                    DataChunkBuilder::with_default_size(self.schema.data_types());
                #[for_await]
                for chunk in stream {
                    #[for_await]
                    for output_chunk in output_chunk_builder
                        .trunc_data_chunk(chunk?.reorder_columns(&self.output_indices))
                    {
                        yield output_chunk?
                    }
                }
                if let Some(output_chunk) = output_chunk_builder.consume_all()? {
                    yield output_chunk
                }
            } else {
                let stream = match self.join_type {
                    JoinType::Inner => HashJoinExecutor::do_inner_join(params),
                    JoinType::LeftOuter => HashJoinExecutor::do_left_outer_join(params),
                    JoinType::LeftSemi => HashJoinExecutor::do_left_semi_anti_join::<false>(params),
                    JoinType::LeftAnti => HashJoinExecutor::do_left_semi_anti_join::<true>(params),
                    JoinType::RightOuter => HashJoinExecutor::do_right_outer_join(params),
                    JoinType::RightSemi => {
                        HashJoinExecutor::do_right_semi_anti_join::<false>(params)
                    }
                    JoinType::RightAnti => {
                        HashJoinExecutor::do_right_semi_anti_join::<true>(params)
                    }
                    JoinType::FullOuter => HashJoinExecutor::do_full_outer_join(params),
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
        let [build_child]: [_; 1] = inputs.try_into().unwrap();

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

        let build_side_data_types = build_child.schema().data_types();

        let table_desc = lookup_join_node.get_probe_side_table_desc()?;
        let probe_side_column_ids = lookup_join_node.get_probe_side_column_ids().to_vec();

        let probe_side_schema = Schema {
            fields: probe_side_column_ids
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
            build_child.schema().fields.clone()
        } else {
            [
                build_child.schema().fields.clone(),
                probe_side_schema.fields.clone(),
            ]
            .concat()
        };

        let original_schema = Schema { fields };
        let actual_schema = output_indices
            .iter()
            .map(|&idx| original_schema[idx].clone())
            .collect();

        let mut build_side_key_idxs = vec![];
        for build_side_key in lookup_join_node.get_build_side_key() {
            build_side_key_idxs.push(*build_side_key as usize)
        }

        let build_side_key_types: Vec<DataType> = build_side_key_idxs
            .iter()
            .map(|&i| build_side_data_types[i].clone())
            .collect_vec();

        let mut probe_side_key_idxs = vec![];
        for pk in &table_desc.pk {
            let key_idx = probe_side_column_ids
                .iter()
                .position(|&i| table_desc.columns[pk.index as usize].column_id == i)
                .ok_or_else(|| {
                    internal_error("Probe side key is not part of its output columns")
                })?;
            probe_side_key_idxs.push(key_idx);
        }

        let probe_side_key_types = probe_side_key_idxs
            .iter()
            .map(|&i| probe_side_schema.fields[i as usize].data_type.clone())
            .collect_vec();

        let null_safe = lookup_join_node.get_null_safe().to_vec();

        let vnode_mapping = lookup_join_node.get_probe_side_vnode_mapping().to_vec();
        assert!(!vnode_mapping.is_empty());

        let hash_key_kind = calc_hash_key_kind(&probe_side_key_types);

        let probe_side_source = ProbeSideSource {
            table_desc: table_desc.clone(),
            vnode_mapping,
            build_side_key_types,
            probe_side_schema,
            probe_side_column_ids,
            probe_side_key_types: probe_side_key_types.clone(),
            context: source.context().clone(),
            task_id: source.task_id.clone(),
            epoch: source.epoch(),
            pu_to_worker_mapping: get_pu_to_worker_mapping(lookup_join_node.get_worker_nodes()),
            pu_to_scan_range_mapping: HashMap::new(),
        };

        Ok(LookupJoinExecutor::dispatch_by_kind(
            hash_key_kind,
            LookupJoinExecutor::new(
                join_type,
                condition,
                Some(build_child),
                build_side_data_types,
                build_side_key_idxs,
                Box::new(probe_side_source),
                probe_side_key_types,
                probe_side_key_idxs,
                null_safe,
                DataChunkBuilder::with_default_size(original_schema.data_types()),
                actual_schema,
                output_indices,
                source.plan_node().get_identity().clone(),
            ),
        ))
    }
}

impl HashKeyDispatcher for LookupJoinExecutor<()> {
    type Input = Self;
    type Output = BoxedExecutor;

    fn dispatch<K: HashKey>(input: Self::Input) -> Self::Output {
        Box::new(LookupJoinExecutor::<K>::new(
            input.join_type,
            input.condition,
            input.build_child,
            input.build_side_data_types,
            input.build_side_key_idxs,
            input.probe_side_source,
            input.probe_side_key_types,
            input.probe_side_key_idxs,
            input.null_safe,
            input.chunk_builder,
            input.schema,
            input.output_indices,
            input.identity,
        ))
    }
}

#[cfg(test)]
mod tests {
    use risingwave_common::array::{DataChunk, DataChunkTestExt};
    use risingwave_common::catalog::{Field, Schema};
    use risingwave_common::hash::{calc_hash_key_kind, HashKeyDispatcher};
    use risingwave_common::types::{DataType, ScalarImpl};
    use risingwave_common::util::chunk_coalesce::DataChunkBuilder;
    use risingwave_common::util::sort_util::{OrderPair, OrderType};
    use risingwave_expr::expr::expr_binary_nonnull::new_binary_expr;
    use risingwave_expr::expr::{BoxedExpression, InputRefExpression, LiteralExpression};
    use risingwave_pb::expr::expr_node::Type;

    use crate::executor::join::JoinType;
    use crate::executor::test_utils::{
        diff_executor_output, FakeProbeSideSourceBuilder, MockExecutor,
    };
    use crate::executor::{BoxedExecutor, LookupJoinExecutor, OrderByExecutor};

    pub struct MockGatherExecutor {
        chunks: Vec<DataChunk>,
    }

    fn create_build_child() -> BoxedExecutor {
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
        let build_child = create_build_child();

        let fields = if join_type == JoinType::LeftSemi || join_type == JoinType::LeftAnti {
            build_child.schema().fields.clone()
        } else {
            [
                build_child.schema().fields.clone(),
                build_child.schema().fields.clone(),
            ]
            .concat()
        };
        let original_schema = Schema { fields };

        let probe_side_schema = Schema {
            fields: build_child.schema().fields.clone(),
        };

        let probe_side_data_types = probe_side_schema.data_types();
        let build_side_data_types = build_child.schema().data_types();

        let hash_key_kind = calc_hash_key_kind(&probe_side_data_types);

        LookupJoinExecutor::dispatch_by_kind(
            hash_key_kind,
            LookupJoinExecutor::new(
                join_type,
                condition,
                Some(build_child),
                build_side_data_types,
                vec![0],
                Box::new(FakeProbeSideSourceBuilder::new(probe_side_schema)),
                vec![probe_side_data_types[0].clone()],
                vec![0],
                vec![null_safe],
                DataChunkBuilder::with_default_size(original_schema.data_types()),
                original_schema.clone(),
                (0..original_schema.len()).into_iter().collect(),
                "TestLookupJoinExecutor".to_string(),
            ),
        )
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

        let condition = Some(new_binary_expr(
            Type::LessThan,
            DataType::Boolean,
            Box::new(LiteralExpression::new(
                DataType::Int32,
                Some(ScalarImpl::Int32(5)),
            )),
            Box::new(InputRefExpression::new(DataType::Float32, 3)),
        ));

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

        let condition = Some(new_binary_expr(
            Type::LessThan,
            DataType::Boolean,
            Box::new(LiteralExpression::new(
                DataType::Int32,
                Some(ScalarImpl::Int32(5)),
            )),
            Box::new(InputRefExpression::new(DataType::Float32, 3)),
        ));

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

        let condition = Some(new_binary_expr(
            Type::LessThan,
            DataType::Boolean,
            Box::new(LiteralExpression::new(
                DataType::Int32,
                Some(ScalarImpl::Int32(5)),
            )),
            Box::new(InputRefExpression::new(DataType::Float32, 3)),
        ));

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

        let condition = Some(new_binary_expr(
            Type::LessThan,
            DataType::Boolean,
            Box::new(LiteralExpression::new(
                DataType::Int32,
                Some(ScalarImpl::Int32(5)),
            )),
            Box::new(InputRefExpression::new(DataType::Float32, 3)),
        ));

        do_test(JoinType::LeftAnti, condition, false, expected).await;
    }
}
