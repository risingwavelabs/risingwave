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
    DataType, Datum, ToOwnedDatum, VirtualNode, VIRTUAL_NODE_COUNT,
};
use risingwave_common::util::chunk_coalesce::DataChunkBuilder;
use risingwave_common::util::scan_range::ScanRange;
use risingwave_expr::expr::expr_unary::new_unary_expr;
use risingwave_expr::expr::{build_from_prost, BoxedExpression, LiteralExpression};
use risingwave_pb::batch_plan::plan_node::NodeBody;
use risingwave_pb::batch_plan::{PlanNode, RowSeqScanNode};
use risingwave_pb::expr::expr_node::Type;
use risingwave_pb::plan_common::StorageTableDesc;
use uuid::Uuid;

use crate::executor::join::chunked_data::ChunkedData;
use crate::executor::join::JoinType;
use crate::executor::{utils, BoxedDataChunkListStream, BoxedDataChunkStream, BoxedExecutor, BoxedExecutorBuilder, BoxedLookupExecutorBuilder, BufferChunkExecutor, DummyExecutor, EquiJoinParams, Executor, ExecutorBuilder, HashJoinExecutor, JoinHashMap, LookupExecutorBuilder, RowId, LookupJoinBase};
use crate::task::{BatchTaskContext, TaskId};

/// Inner side executor builder for the `DistributedLookupJoinExecutor`
/// All scan range must belong to same parallel unit.
pub struct InnerSideExecutorBuilder<C> {
    table_desc: StorageTableDesc,
    outer_side_key_types: Vec<DataType>,
    inner_side_column_ids: Vec<i32>,
    inner_side_key_types: Vec<DataType>,
    context: C,
    task_id: TaskId,
    epoch: u64,
    scan_range_vnode_list: Vec<(ScanRange, VirtualNode)>,
}

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
    fn create_row_seq_scan_node(&self) -> Result<NodeBody> {
        let mut scan_ranges = vec![];
        let mut vnode_bitmap = BitmapBuilder::zeroed(VIRTUAL_NODE_COUNT);

        self.scan_range_vnode_list
            .iter()
            .for_each(|(scan_range, vnode)| {
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
}

#[async_trait::async_trait]
impl<C: BatchTaskContext> LookupExecutorBuilder for InnerSideExecutorBuilder<C> {
    fn reset(&mut self) {
        self.scan_range_vnode_list.clear();
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
        self.scan_range_vnode_list.push((scan_range, vnode));

        Ok(())
    }

    /// Builds and returns the `ExchangeExecutor` used for the inner side of the
    /// `DistributedLookupJoinExecutor`.
    async fn build_executor(&self) -> Result<BoxedExecutor> {
        if self.scan_range_vnode_list.is_empty() {
            return Ok(Box::new(DummyExecutor {
                schema: Schema::default(),
            }));
        }

        let plan_node = PlanNode {
            children: vec![],
            identity: Uuid::new_v4().to_string(),
            node_body: Some(self.create_row_seq_scan_node()?),
        };

        let task_id = self.task_id.clone();

        let executor_builder =
            ExecutorBuilder::new(&plan_node, &task_id, self.context.clone(), self.epoch);

        executor_builder.build().await
    }
}

/// Distributed Lookup Join Executor.
/// High level Execution flow:
/// Repeat 1-3:
///   1. Read N rows from outer side input and send keys to inner side builder after deduplication.
///   2. Inner side input lookups inner side table with keys and builds hash map.
///   3. Outer side rows join each inner side rows by probing the hash map.
///
/// Distributed lookup join already scheduled to its inner side corresponding compute node, so that
/// it can just lookup the compute node locally without sending RPCs to other compute nodes.
pub struct DistributedLookupJoinExecutor<K> {
    base: LookupJoinBase<K>,
    _phantom: PhantomData<K>,
}

const AT_LEAST_OUTER_SIDE_ROWS: usize = 512;

impl<K: HashKey> Executor for DistributedLookupJoinExecutor<K> {
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

impl<K> DistributedLookupJoinExecutor<K> {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        base: LookupJoinBase<K>
    ) -> Self {
        Self {
            base,
            _phantom: PhantomData,
        }
    }
}

pub struct DistributedLookupJoinExecutorBuilder {}

#[async_trait::async_trait]
impl BoxedExecutorBuilder for DistributedLookupJoinExecutorBuilder {
    async fn new_boxed_executor<C: BatchTaskContext>(
        source: &ExecutorBuilder<'_, C>,
        inputs: Vec<BoxedExecutor>,
    ) -> Result<BoxedExecutor> {
        let [outer_side_input]: [_; 1] = inputs.try_into().unwrap();

        let distributed_lookup_join_node = try_match_expand!(
            source.plan_node().get_node_body().unwrap(),
            NodeBody::DistributedLookupJoin
        )?;

        let join_type = JoinType::from_prost(distributed_lookup_join_node.get_join_type()?);
        let condition = match distributed_lookup_join_node.get_condition() {
            Ok(cond_prost) => Some(build_from_prost(cond_prost)?),
            Err(_) => None,
        };

        let output_indices: Vec<usize> = distributed_lookup_join_node
            .get_output_indices()
            .iter()
            .map(|&x| x as usize)
            .collect();

        let outer_side_data_types = outer_side_input.schema().data_types();

        let table_desc = distributed_lookup_join_node.get_inner_side_table_desc()?;
        let inner_side_column_ids = distributed_lookup_join_node
            .get_inner_side_column_ids()
            .to_vec();

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
        for outer_side_key in distributed_lookup_join_node.get_outer_side_key() {
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

        let null_safe = distributed_lookup_join_node.get_null_safe().to_vec();

        let chunk_size = source.context.get_config().developer.batch_chunk_size;

        let inner_side_builder = InnerSideExecutorBuilder {
            table_desc: table_desc.clone(),
            outer_side_key_types,
            inner_side_column_ids,
            inner_side_key_types: inner_side_key_types.clone(),
            context: source.context().clone(),
            task_id: source.task_id.clone(),
            epoch: source.epoch(),
            scan_range_vnode_list: vec![],
        };

        Ok(DistributedLookupJoinExecutorArgs {
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

struct DistributedLookupJoinExecutorArgs {
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

impl HashKeyDispatcher for DistributedLookupJoinExecutorArgs {
    type Output = BoxedExecutor;

    fn dispatch_impl<K: HashKey>(self) -> Self::Output {
        Box::new(DistributedLookupJoinExecutor::<K>::new(
            LookupJoinBase::<K> {
                join_type: self.join_type,
                condition: self.condition,
                outer_side_input: self.outer_side_input,
                outer_side_data_types: self.outer_side_data_types,
                outer_side_key_idxs: self.outer_side_key_idxs,
                inner_side_builder: self.inner_side_builder,
                inner_side_key_types: self.inner_side_key_types,
                inner_side_key_idxs: self.inner_side_key_idxs,
                null_safe: self.null_safe,
                chunk_builder: self.chunk_builder,
                schema: self.schema,
                output_indices: self.output_indices,
                chunk_size: self.chunk_size,
                identity: self.identity,
                _phantom: PhantomData,
            }
        ))
    }

    fn data_types(&self) -> &[DataType] {
        &self.inner_side_key_types
    }
}
