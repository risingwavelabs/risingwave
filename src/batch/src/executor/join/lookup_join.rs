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

use futures::StreamExt;
use futures_async_stream::try_stream;
use itertools::Itertools;
use risingwave_common::array::{Array, DataChunk, RowRef};
use risingwave_common::catalog::{ColumnDesc, Field, Schema};
use risingwave_common::error::{ErrorCode, Result, RwError};
use risingwave_common::types::{DataType, ToOwnedDatum};
use risingwave_common::util::chunk_coalesce::{DataChunkBuilder, SlicedDataChunk};
use risingwave_expr::expr::{build_from_prost, BoxedExpression};
use risingwave_pb::batch_plan::exchange_info::DistributionMode;
use risingwave_pb::batch_plan::exchange_source::LocalExecutePlan::Plan;
use risingwave_pb::batch_plan::plan_node::NodeBody;
use risingwave_pb::batch_plan::{
    ExchangeInfo, ExchangeNode, ExchangeSource as ProstExchangeSource, LocalExecutePlan,
    PlanFragment, PlanNode, RowSeqScanNode, ScanRange,
};
use risingwave_pb::plan_common::CellBasedTableDesc;
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

/// Probe side source for the `LookupJoinExecutor`
pub struct ProbeSideSource<C> {
    build_side_eq_types: Vec<DataType>,
    build_side_idxs: Vec<usize>,
    table_desc: CellBasedTableDesc,
    probe_side_schema: Schema,
    probe_side_column_ids: Vec<i32>,
    source_templates: Vec<ProstExchangeSource>,
    context: C,
    task_id: TaskId,
    epoch: u64,
}

/// Used to build the executor for the probe side
#[async_trait::async_trait]
pub trait ProbeSideSourceBuilder: Send {
    async fn build_source(&self, cur_row: &RowRef) -> Result<BoxedExecutor>;
}

impl<C: BatchTaskContext> ProbeSideSource<C> {
    /// Creates the `RowSeqScanNode` that will be used for scanning the probe side table
    /// based on the passed `RowRef`.
    fn create_row_seq_scan_node(&self, cur_row: &RowRef) -> Result<NodeBody> {
        // Check that the data types of both sides of the equality predicate are the same
        // TODO: Handle the cases where the data types of both sides are different but castable
        // (e.g. int32 and int64)
        let probe_side_eq_types = self
            .table_desc
            .order_key
            .iter()
            .map(|order| {
                self.probe_side_schema.fields[order.index as usize]
                    .data_type
                    .clone()
            })
            .collect_vec();

        if !(0..self.build_side_eq_types.len()).all(|i| {
            i < probe_side_eq_types.len() && self.build_side_eq_types[i] == probe_side_eq_types[i]
        }) {
            return Err(ErrorCode::NotImplemented(
                    "Lookup Joins where the two sides of an equality predicate have different data types".to_string(),
                    None.into()
            ).into());
        }

        let eq_conds = self
            .build_side_idxs
            .iter()
            .map(|&idx| {
                cur_row
                    .value_at(idx)
                    .to_owned_datum()
                    .unwrap()
                    .to_protobuf()
            })
            .collect();

        let scan_range = Some(ScanRange {
            eq_conds,
            lower_bound: None,
            upper_bound: None,
        });

        Ok(NodeBody::RowSeqScan(RowSeqScanNode {
            table_desc: Some(self.table_desc.clone()),
            column_ids: self.probe_side_column_ids.clone(),
            scan_range,
            vnode_bitmap: None,
        }))
    }

    /// Creates all the `ProstExchangeSource` that will be sent to the `ExchangeExecutor` using
    /// the source templates.
    fn build_prost_exchange_sources(&self, cur_row: &RowRef) -> Result<Vec<ProstExchangeSource>> {
        let Plan(inner_template_plan) = self.source_templates[0].get_local_execute_plan().unwrap();

        let local_execute_plan = LocalExecutePlan {
            plan: Some(PlanFragment {
                root: Some(PlanNode {
                    children: vec![],
                    identity: Uuid::new_v4().to_string(),
                    node_body: Some(self.create_row_seq_scan_node(cur_row)?),
                }),
                exchange_info: Some(ExchangeInfo {
                    mode: DistributionMode::Single as i32,
                    ..Default::default()
                }),
            }),
            epoch: inner_template_plan.epoch,
        };

        let prost_exchange_sources = self
            .source_templates
            .iter()
            .map(|source| ProstExchangeSource {
                task_output_id: Some(source.get_task_output_id().unwrap().clone()),
                host: Some(source.get_host().unwrap().clone()),
                local_execute_plan: Some(Plan(local_execute_plan.clone())),
            })
            .collect();

        Ok(prost_exchange_sources)
    }
}

#[async_trait::async_trait]
impl<C: BatchTaskContext> ProbeSideSourceBuilder for ProbeSideSource<C> {
    /// Builds and returns the `ExchangeExecutor` used for the probe side of the
    /// `LookupJoinExecutor`.
    async fn build_source(&self, cur_row: &RowRef) -> Result<BoxedExecutor> {
        let sources = self.build_prost_exchange_sources(cur_row)?;

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
/// 1) Iterate through each row in the build side
/// 2) For each row R, find the rows that match with R on the join conditions on the probe side by
/// using the `ExchangeExecutor` to query the probe side
/// 3) Join R and the matching rows on the probe side together with either inner or left outer join
/// 4) Repeat 2-3) for every row in the build side
pub struct LookupJoinExecutor<P> {
    join_type: JoinType,
    condition: Option<BoxedExpression>,
    build_child: Option<BoxedExecutor>,
    build_side_data_types: Vec<DataType>,
    probe_side_source: P,
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

        while let Some(data_chunk) = build_side_stream.next().await {
            let data_chunk = data_chunk?;

            for row_idx in 0..data_chunk.capacity() {
                let (cur_row, is_visible) = data_chunk.row_at(row_idx)?;
                if !is_visible {
                    continue;
                }

                let probe_child = self.probe_side_source.build_source(&cur_row).await?;
                let mut probe_side_stream = probe_child.execute();

                let mut chunk_added = false;

                // The handling of the different join types and which operation they should perform
                // under each condition is very complex. The gist of it is:
                //
                // Inner Join: Always do_inner_join until probe_side_chunk is none.
                //
                // Left Outer Join: Always do_inner_join until probe_side_chunk is None. If this
                //      occurs and no chunk has been added yet, null-pad the current row and add it.
                //
                // Left Semi Join: Always do_left_semi_anti_join until probe_side_chunk is None or
                //      a chunk has been added, whichever is first.
                //
                // Left Anti Join: Always do_left_semi_anti_join until probe_side_chunk is None or
                //      current row has a match on probe side, whichever is first. If the former
                //      occurs and no match has been found yet, then add current row.
                // TODO: Simplify the logic of handling the different join types
                loop {
                    let probe_side_chunk = probe_side_stream.next().await;

                    let join_result = if let Some(chunk) = probe_side_chunk {
                        let chunk = chunk?;

                        match self.join_type {
                            JoinType::Inner | JoinType::LeftOuter => {
                                self.do_inner_join(&cur_row, chunk)
                            }
                            JoinType::LeftSemi | JoinType::LeftAnti if !chunk_added => {
                                self.do_left_semi_anti_join(&cur_row, chunk)
                            }
                            JoinType::LeftSemi | JoinType::LeftAnti if chunk_added => {
                                break;
                            }
                            _ => Err(invalid_join_error.clone()),
                        }
                    } else if !chunk_added {
                        match self.join_type {
                            JoinType::Inner | JoinType::LeftSemi => {
                                break;
                            }
                            JoinType::LeftOuter => self.do_left_outer_join(&cur_row),
                            JoinType::LeftAnti => self.convert_row_for_builder(&cur_row),
                            _ => Err(invalid_join_error.clone()),
                        }
                    } else {
                        break;
                    }?;

                    // Append chunk from the join result to the chunk builder if it exists
                    if let Some(return_chunk) = join_result {
                        if return_chunk.cardinality() > 0 {
                            chunk_added = true;

                            // Don't join if it's a LeftAnti Join
                            if self.join_type != JoinType::LeftAnti {
                                let append_result =
                                    self.append_chunk(SlicedDataChunk::new_checked(return_chunk)?)?;
                                if let Some(inner_chunk) = append_result {
                                    yield inner_chunk.reorder_columns(&self.output_indices);
                                }
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

        // Consume and yield all the remaining chunks in the chunk builder
        if let Some(data_chunk) = self.chunk_builder.consume_all()? {
            yield data_chunk.reorder_columns(&self.output_indices);
        }
    }

    /// Inner Joins the `cur_row` with the `probe_side_chunk`. The non-equi condition is also
    /// evaluated to hide the rows in the join result that don't match the condition.
    fn do_inner_join(
        &self,
        cur_row: &RowRef,
        probe_side_chunk: DataChunk,
    ) -> Result<Option<DataChunk>> {
        let probe_chunk = probe_side_chunk.compact()?;

        let const_row_chunk =
            convert_row_to_chunk(cur_row, probe_chunk.capacity(), &self.build_side_data_types)?;

        let new_chunk = concatenate(&const_row_chunk, &probe_chunk)?;

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

    /// If the inner join of `cur_row` and `probe_side_chunk` has a non-zero cardinality, convert
    /// `cur_row` to chunk and return it. Otherwise, return None.
    fn do_left_semi_anti_join(
        &self,
        cur_row: &RowRef,
        probe_side_chunk: DataChunk,
    ) -> Result<Option<DataChunk>> {
        let chunk = self.do_inner_join(cur_row, probe_side_chunk)?;
        let match_exists = chunk.unwrap().cardinality() > 0; // Inner Join always returns Some(chunk)

        if match_exists {
            let return_chunk = self.convert_row_for_builder(cur_row)?;
            Ok(return_chunk)
        } else {
            Ok(None)
        }
    }

    fn convert_row_for_builder(&self, cur_row: &RowRef) -> Result<Option<DataChunk>> {
        Ok(Some(convert_row_to_chunk(
            cur_row,
            1,
            &self.chunk_builder.data_types(),
        )?))
    }

    /// Appends `input_chunk` to the `self.chunk_builder`. If there is a leftover chunk, assign it
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
        mut inputs: Vec<BoxedExecutor>,
    ) -> Result<BoxedExecutor> {
        ensure!(inputs.len() == 1, "LookupJoinExeuctor should have 1 child!");

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

        let build_child = inputs.remove(0);
        let build_side_data_types = build_child.schema().data_types();

        let probe_side_table_desc = lookup_join_node.get_probe_side_table_desc()?;

        let probe_side_schema = Schema {
            fields: probe_side_table_desc
                .columns
                .iter()
                .map(|column_desc| Field::from(&ColumnDesc::from(column_desc)))
                .collect_vec(),
        };
        let probe_side_column_ids = lookup_join_node.get_probe_side_column_ids().to_vec();

        let fields = if join_type == JoinType::LeftSemi || join_type == JoinType::LeftAnti {
            build_child.schema().fields.clone()
        } else {
            [
                build_child.schema().fields.clone(),
                probe_side_column_ids
                    .iter()
                    .map(|&i| probe_side_schema.fields[i as usize].clone())
                    .collect(),
            ]
            .concat()
        };

        let original_schema = Schema { fields };
        let actual_schema = output_indices
            .iter()
            .map(|&idx| original_schema[idx].clone())
            .collect();

        // Initialize parameters for creating the probe side source
        let mut build_side_idxs = vec![];
        for build_side_key in lookup_join_node.get_build_side_key() {
            build_side_idxs.push(*build_side_key as usize)
        }

        let build_side_eq_types = build_side_idxs
            .iter()
            .map(|&idx| build_side_data_types[idx].clone())
            .collect_vec();

        ensure!(!lookup_join_node.get_sources().is_empty());
        let probe_side_source = ProbeSideSource {
            build_side_eq_types,
            build_side_idxs,
            table_desc: probe_side_table_desc.clone(),
            probe_side_schema,
            probe_side_column_ids,
            source_templates: lookup_join_node.get_sources().to_vec(),
            context: source.context().clone(),
            task_id: source.task_id.clone(),
            epoch: source.epoch(),
        };

        Ok(Box::new(LookupJoinExecutor {
            join_type,
            condition,
            build_child: Some(build_child),
            build_side_data_types,
            probe_side_source,
            chunk_builder: DataChunkBuilder::with_default_size(original_schema.data_types()),
            schema: actual_schema,
            output_indices,
            last_chunk: None,
            identity: "LookupJoinExecutor".to_string(),
        }))
    }
}

#[cfg(test)]
mod tests {
    use risingwave_common::array::{DataChunk, DataChunkTestExt};
    use risingwave_common::catalog::{Field, Schema};
    use risingwave_common::types::{DataType, ScalarImpl};
    use risingwave_common::util::chunk_coalesce::DataChunkBuilder;
    use risingwave_expr::expr::expr_binary_nonnull::new_binary_expr;
    use risingwave_expr::expr::{BoxedExpression, InputRefExpression, LiteralExpression};
    use risingwave_pb::expr::expr_node::Type;

    use crate::executor::join::JoinType;
    use crate::executor::test_utils::{
        diff_executor_output, FakeProbeSideSourceBuilder, MockExecutor,
    };
    use crate::executor::{BoxedExecutor, LookupJoinExecutor};

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
             5 9.1",
        ));

        Box::new(executor)
    }

    fn create_lookup_join_executor(
        join_type: JoinType,
        condition: Option<BoxedExpression>,
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
            probe_side_source: FakeProbeSideSourceBuilder::new(probe_side_schema),
            chunk_builder: DataChunkBuilder::with_default_size(original_schema.data_types()),
            schema: original_schema.clone(),
            output_indices: (0..original_schema.len()).into_iter().collect(),
            last_chunk: None,
            identity: "TestLookupJoinExecutor".to_string(),
        })
    }

    async fn do_test(join_type: JoinType, condition: Option<BoxedExpression>, expected: DataChunk) {
        let lookup_join_executor = create_lookup_join_executor(join_type, condition);
        let mut expected_mock_exec = MockExecutor::new(lookup_join_executor.schema().clone());
        expected_mock_exec.add(expected);
        diff_executor_output(lookup_join_executor, Box::new(expected_mock_exec)).await;
    }

    #[tokio::test]
    async fn test_inner_join() {
        let expected = DataChunk::from_pretty(
            "i f   i f
             1 6.1 1 9.2
             2 8.4 2 4.4
             2 8.4 2 5.5
             2 5.5 2 4.4
             2 5.5 2 5.5
             5 9.1 5 3.7
             5 9.1 5 2.3",
        );

        do_test(JoinType::Inner, None, expected).await;
    }

    #[tokio::test]
    async fn test_left_outer_join() {
        let expected = DataChunk::from_pretty(
            "i f   i f
             1 6.1 1 9.2
             2 8.4 2 4.4
             2 8.4 2 5.5
             3 3.9 . .
             2 5.5 2 4.4
             2 5.5 2 5.5
             5 9.1 5 3.7
             5 9.1 5 2.3",
        );

        do_test(JoinType::LeftOuter, None, expected).await;
    }

    #[tokio::test]
    async fn test_inner_join_with_condition() {
        let expected = DataChunk::from_pretty(
            "i f   i f
             1 6.1 1 9.2
             2 8.4 2 5.5
             2 5.5 2 5.5",
        );

        do_test(
            JoinType::Inner,
            Some(new_binary_expr(
                Type::LessThan,
                DataType::Boolean,
                Box::new(LiteralExpression::new(
                    DataType::Int32,
                    Some(ScalarImpl::Int32(5)),
                )),
                Box::new(InputRefExpression::new(DataType::Float32, 3)),
            )),
            expected,
        )
        .await;
    }

    #[tokio::test]
    async fn test_left_outer_join_with_condition() {
        let expected = DataChunk::from_pretty(
            "i f   i f
             1 6.1 1 9.2
             2 8.4 2 5.5
             3 3.9 . .
             2 5.5 2 5.5
             5 9.1 . .",
        );

        do_test(
            JoinType::LeftOuter,
            Some(new_binary_expr(
                Type::LessThan,
                DataType::Boolean,
                Box::new(LiteralExpression::new(
                    DataType::Int32,
                    Some(ScalarImpl::Int32(5)),
                )),
                Box::new(InputRefExpression::new(DataType::Float32, 3)),
            )),
            expected,
        )
        .await;
    }
}
