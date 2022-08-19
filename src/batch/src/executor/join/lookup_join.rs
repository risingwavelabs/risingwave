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

use futures::StreamExt;
use futures_async_stream::try_stream;
use itertools::Itertools;
use risingwave_common::array::{Array, DataChunk, Row, RowRef};
use risingwave_common::buffer::BitmapBuilder;
use risingwave_common::catalog::{ColumnDesc, Field, Schema};
use risingwave_common::error::{internal_error, ErrorCode, Result, RwError};
use risingwave_common::types::{
    DataType, Datum, ParallelUnitId, ToOwnedDatum, VirtualNode, VnodeMapping,
};
use risingwave_common::util::chunk_coalesce::{DataChunkBuilder, SlicedDataChunk};
use risingwave_common::util::scan_range::ScanRange;
use risingwave_common::util::worker_util::get_pu_to_worker_mapping;
use risingwave_expr::expr::expr_binary_nonnull::new_binary_expr;
use risingwave_expr::expr::expr_binary_nullable::{
    new_not_distinct_from_expr, new_nullable_binary_expr,
};
use risingwave_expr::expr::expr_unary::new_unary_expr;
use risingwave_expr::expr::{
    build_from_prost, BoxedExpression, InputRefExpression, LiteralExpression,
};
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

use crate::executor::join::{
    concatenate, convert_datum_refs_to_chunk, convert_row_to_chunk, JoinType,
};
use crate::executor::{
    BoxedDataChunkStream, BoxedExecutor, BoxedExecutorBuilder, Executor, ExecutorBuilder,
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
            .order_key
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

        // let prost_task_id = self.task_id.clone();
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
        println!("Enter build source");
        let mut sources = vec![];
        for id in self.pu_to_scan_range_mapping.keys() {
            sources.push(self.build_prost_exchange_source(id)?);
        }
        println!("exchange sources for lookup join: {:?}", sources);

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
pub struct LookupJoinExecutor<P> {
    join_type: JoinType,
    condition: Option<BoxedExpression>,
    build_child: Option<BoxedExecutor>,
    build_side_data_types: Vec<DataType>, // Data types of all columns of build side table
    build_side_key_idxs: Vec<usize>,
    probe_side_source: P,
    probe_side_key_types: Vec<DataType>, // Data types only of key columns of probe side table
    probe_side_key_idxs: Vec<usize>,
    null_safe: Vec<bool>,
    chunk_builder: DataChunkBuilder,
    schema: Schema,
    output_indices: Vec<usize>,
    last_chunk: Option<SlicedDataChunk>,
    identity: String,
}

impl<P: 'static + ProbeSideSourceBuilder> Executor for LookupJoinExecutor<P> {
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

impl<P: 'static + ProbeSideSourceBuilder> LookupJoinExecutor<P> {
    #[try_stream(boxed, ok = DataChunk, error = RwError)]
    async fn do_execute(mut self: Box<Self>) {
        let mut build_side_stream = self.build_child.take().unwrap().execute();

        let invalid_join_error = RwError::from(ErrorCode::NotImplemented(
            format!(
                "Lookup Join does not support join type {:?}",
                self.join_type
            ),
            None.into(),
        ));

        while let Some(build_chunk) = build_side_stream.next().await {
            println!("Start to execute probe side stream1");
            // println!("we get build chunk");
            let build_chunk = build_chunk?.compact()?;

            // Group rows with the same key datums together
            let groups = build_chunk.rows().into_group_map_by(|row| {
                self.build_side_key_idxs
                    .iter()
                    .map(|&idx| row.value_at(idx).to_owned_datum())
                    .collect_vec()
            });

            let mut row_keys = vec![];
            let mut all_row_refs = vec![];

            groups.into_iter().for_each(|(row_key, row_refs)| {
                row_keys.push(row_key);
                all_row_refs.push(row_refs);
            });

            assert_eq!(row_keys.len(), all_row_refs.len());

            self.probe_side_source.reset();
            for row_key in &row_keys {
                self.probe_side_source.add_scan_range(row_key)?;
            }

            let probe_child = self.probe_side_source.build_source().await?;
            println!("Start to execute probe side stream2");
            let mut probe_side_stream = probe_child.execute();

            let mut probe_side_chunk_exists = true;

            // for each row in the build side chunk, has_match tells us if it has a match
            // on the probe side table
            let mut has_match = all_row_refs
                .iter()
                .map(|rows| vec![false; rows.len()])
                .collect_vec();

            // Keep looping until there are no more probe side chunks to get, then do 1 more loop
            while probe_side_chunk_exists {
                let probe_side_chunk = probe_side_stream.next().await;
                probe_side_chunk_exists = probe_side_chunk.is_some();

                let probe_side_chunk = if let Some(chunk) = probe_side_chunk {
                    println!("some for probe side chunk");
                    Some(chunk?.compact()?)
                } else {
                    println!("None for probe side chunk");
                    None
                };

                for (i, row_refs) in all_row_refs.iter().enumerate() {
                    // We filter the probe side chunk to only have the rows whose key datums
                    // matches the key datums of the row_refs we're currently looking at
                    let expr = self.create_expression(&row_keys[i]);
                    let chunk = if let Some(chunk) = probe_side_chunk.as_ref() {
                        let vis = expr.eval(chunk)?;
                        let chunk = chunk.with_visibility(vis.as_bool().iter().collect());
                        Some(chunk.compact()?)
                    } else {
                        None
                    };

                    // The handling of the different join types and which operation they should
                    // perform under each condition is very complex. For each
                    // row, the gist of it is:
                    //
                    // Inner Join: Join the cur_row and the probe chunk until the latter is None.
                    //
                    // Left Outer Join: Same as inner join but if the probe chunk is None and the
                    //      row has no match yet, null-pad the current row and append it
                    //
                    // Left Semi Join: Check for matches until probe chunk is None or a match is
                    //      found. If there were any matches by the end, append the row.
                    //
                    // Left Anti Join: Check for matches until probe chunk is None or a match is
                    //      found. If there were no matches by the end, append the row.
                    // TODO: Simplify the logic of handling the different join types
                    for (j, cur_row) in row_refs.iter().enumerate() {
                        let join_result = if probe_side_chunk_exists {
                            let chunk = chunk.as_ref().unwrap().clone();

                            match self.join_type {
                                JoinType::LeftSemi | JoinType::LeftAnti if has_match[i][j] => {
                                    continue;
                                }
                                JoinType::Inner
                                | JoinType::LeftOuter
                                | JoinType::LeftSemi
                                | JoinType::LeftAnti => self.do_inner_join(cur_row, chunk),
                                _ => Err(invalid_join_error.clone()),
                            }
                        } else {
                            match self.join_type {
                                JoinType::Inner => break,
                                JoinType::LeftOuter if has_match[i][j] => continue,
                                JoinType::LeftOuter if !has_match[i][j] => {
                                    self.do_left_outer_join(cur_row)
                                }
                                JoinType::LeftSemi if has_match[i][j] => {
                                    self.convert_row_for_builder(cur_row)
                                }
                                JoinType::LeftSemi if !has_match[i][j] => continue,
                                JoinType::LeftAnti if has_match[i][j] => continue,
                                JoinType::LeftAnti if !has_match[i][j] => {
                                    self.convert_row_for_builder(cur_row)
                                }
                                _ => Err(invalid_join_error.clone()),
                            }
                        }?;

                        // Append chunk from the join result to the chunk builder if it exists
                        if let Some(return_chunk) = join_result {
                            if return_chunk.cardinality() > 0 {
                                has_match[i][j] = true;

                                // Skip adding if it's a Left Anti/Semi Join and the probe side
                                // chunk exists. Rows for Left Anti/Semi Join are added at the end
                                // when the probe side chunk is None instead.
                                if let (JoinType::LeftSemi | JoinType::LeftAnti, true) =
                                    (self.join_type, probe_side_chunk_exists)
                                {
                                    continue;
                                }

                                let append_result =
                                    self.append_chunk(SlicedDataChunk::new_checked(return_chunk)?)?;
                                if let Some(inner_chunk) = append_result {
                                    yield inner_chunk.reorder_columns(&self.output_indices);
                                }
                            }
                        }

                        // Until we don't have any more last_chunks to append to the chunk builder,
                        // keep appending them to the chunk builder
                        while self.last_chunk.is_some() {
                            let temp_chunk: Option<SlicedDataChunk> =
                                std::mem::take(&mut self.last_chunk);
                            if let Some(inner_chunk) = self.append_chunk(temp_chunk.unwrap())? {
                                yield inner_chunk.reorder_columns(&self.output_indices);
                            }
                        }
                    }
                }
            }
        }

        // Consume and yield all the remaining chunks in the chunk builder
        if let Some(data_chunk) = self.chunk_builder.consume_all()? {
            yield data_chunk.reorder_columns(&self.output_indices);
        }
    }

    /// Creates an expression that returns true if the value of the datums in all the probe side
    /// key columns match the given `key_datums`.
    fn create_expression(&self, key_datums: &[Datum]) -> BoxedExpression {
        self.create_expression_helper(key_datums, 0)
    }

    /// Recursively builds the expression
    fn create_expression_helper(&self, key_datums: &[Datum], i: usize) -> BoxedExpression {
        assert!(i < key_datums.len());

        let literal = Box::new(LiteralExpression::new(
            self.build_side_data_types[self.build_side_key_idxs[i]].clone(),
            key_datums[i].clone(),
        ));

        let input_ref = Box::new(InputRefExpression::new(
            self.probe_side_key_types[i].clone(),
            self.probe_side_key_idxs[i],
        ));

        // For null safe equal pair, use `IS NOT DISTINCT FROM`.
        let equal = if self.null_safe[i] {
            new_not_distinct_from_expr(literal, input_ref, DataType::Boolean)
        } else {
            new_binary_expr(Type::Equal, DataType::Boolean, literal, input_ref)
        };

        if i + 1 == key_datums.len() {
            equal
        } else {
            new_nullable_binary_expr(
                Type::And,
                DataType::Boolean,
                equal,
                self.create_expression_helper(key_datums, i + 1),
            )
        }
    }

    /// Inner joins the `cur_row` with the `probe_side_chunk`. The non-equi condition is also
    /// evaluated to hide the rows in the join result that don't match the condition.
    fn do_inner_join(
        &self,
        cur_row: &RowRef,
        probe_side_chunk: DataChunk,
    ) -> Result<Option<DataChunk>> {
        let build_side_chunk = convert_row_to_chunk(
            cur_row,
            probe_side_chunk.capacity(),
            &self.build_side_data_types,
        )?;

        let new_chunk = concatenate(&build_side_chunk, &probe_side_chunk)?;

        if let Some(cond) = self.condition.as_ref() {
            let visibility = cond.eval(&new_chunk)?;
            Ok(Some(
                new_chunk.with_visibility(visibility.as_bool().iter().collect()),
            ))
        } else {
            Ok(Some(new_chunk))
        }
    }

    /// Pad the row out with NULLs and return it.
    fn do_left_outer_join(&self, cur_row: &RowRef) -> Result<Option<DataChunk>> {
        let mut build_datum_refs = cur_row.values().collect_vec();

        let builder_data_types = self.chunk_builder.data_types();
        let difference = builder_data_types.len() - build_datum_refs.len();

        for _ in 0..difference {
            build_datum_refs.push(None);
        }

        let one_row_chunk = convert_datum_refs_to_chunk(&build_datum_refs, 1, &builder_data_types)?;

        Ok(Some(one_row_chunk))
    }

    /// Converts row to a data chunk
    fn convert_row_for_builder(&self, cur_row: &RowRef) -> Result<Option<DataChunk>> {
        Ok(Some(convert_row_to_chunk(
            cur_row,
            1,
            &self.chunk_builder.data_types(),
        )?))
    }

    /// Appends `input_chunk` to `self.chunk_builder`. If there is a leftover chunk, assign it
    /// to `self.last_chunk`. Note that `self.last_chunk` is always None before this is called.
    fn append_chunk(&mut self, input_chunk: SlicedDataChunk) -> Result<Option<DataChunk>> {
        let (mut left_data_chunk, return_chunk) = self.chunk_builder.append_chunk(input_chunk)?;
        std::mem::swap(&mut self.last_chunk, &mut left_data_chunk);
        Ok(return_chunk)
    }
}

pub struct LookupJoinExecutorBuilder {}

#[async_trait::async_trait]
impl BoxedExecutorBuilder for LookupJoinExecutorBuilder {
    async fn new_boxed_executor<C: BatchTaskContext>(
        source: &ExecutorBuilder<C>,
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
        for order_key in &table_desc.order_key {
            let key_idx = probe_side_column_ids
                .iter()
                .position(|&i| table_desc.columns[order_key.index as usize].column_id == i)
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

        Ok(Box::new(LookupJoinExecutor {
            join_type,
            condition,
            build_child: Some(build_child),
            build_side_data_types,
            build_side_key_idxs,
            probe_side_source,
            probe_side_key_types,
            probe_side_key_idxs,
            null_safe,
            chunk_builder: DataChunkBuilder::with_default_size(original_schema.data_types()),
            schema: actual_schema,
            output_indices,
            last_chunk: None,
            identity: source.plan_node().get_identity().clone(),
        }))
    }
}

#[cfg(test)]
mod tests {
    use risingwave_common::array::{DataChunk, DataChunkTestExt};
    use risingwave_common::catalog::{Field, Schema};
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

        Box::new(LookupJoinExecutor {
            join_type,
            condition,
            build_side_data_types: build_child.schema().data_types(),
            build_child: Some(build_child),
            build_side_key_idxs: vec![0],
            probe_side_key_types: vec![probe_side_schema.data_types()[0].clone()],
            probe_side_source: FakeProbeSideSourceBuilder::new(probe_side_schema),
            probe_side_key_idxs: vec![0],
            null_safe: vec![null_safe],
            chunk_builder: DataChunkBuilder::with_default_size(original_schema.data_types()),
            schema: original_schema.clone(),
            output_indices: (0..original_schema.len()).into_iter().collect(),
            last_chunk: None,
            identity: "TestLookupJoinExecutor".to_string(),
        })
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
             2 5.5 2 4.4
             2 5.5 2 5.5
             2 8.4 2 4.4
             2 8.4 2 5.5
             5 4.1 5 3.7
             5 4.1 5 2.3
             5 9.1 5 3.7
             5 9.1 5 2.3",
        );

        do_test(JoinType::Inner, None, false, expected).await;
    }

    #[tokio::test]
    async fn test_null_safe_inner_join() {
        let expected = DataChunk::from_pretty(
            "i f   i f
             1 6.1 1 9.2
             2 5.5 2 4.4
             2 5.5 2 5.5
             2 8.4 2 4.4
             2 8.4 2 5.5
             5 4.1 5 3.7
             5 4.1 5 2.3
             5 9.1 5 3.7
             5 9.1 5 2.3
             .  .  .  .",
        );

        do_test(JoinType::Inner, None, true, expected).await;
    }

    #[tokio::test]
    async fn test_left_outer_join() {
        let expected = DataChunk::from_pretty(
            "i f   i f
             1 6.1 1 9.2
             2 5.5 2 4.4
             2 5.5 2 5.5
             2 8.4 2 4.4
             2 8.4 2 5.5
             3 3.9 . .
             5 4.1 5 3.7
             5 4.1 5 2.3
             5 9.1 5 3.7
             5 9.1 5 2.3
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
