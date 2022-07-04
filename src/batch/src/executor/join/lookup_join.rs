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

use futures::StreamExt;
use futures_async_stream::try_stream;
use itertools::Itertools;
use risingwave_common::array::{Array, DataChunk, RowRef};
use risingwave_common::catalog::{Field, Schema};
use risingwave_common::error::{ErrorCode, Result, RwError};
use risingwave_common::types::{DataType, ToOwnedDatum};
use risingwave_common::util::chunk_coalesce::{DataChunkBuilder, SlicedDataChunk};
use risingwave_common::util::select_all;
use risingwave_expr::expr::{build_from_prost, BoxedExpression};
use risingwave_pb::batch_plan::exchange_source::LocalExecutePlan::Plan;
use risingwave_pb::batch_plan::plan_node::NodeBody;
use risingwave_pb::batch_plan::{
    ExchangeSource as ProstExchangeSource, LocalExecutePlan, PlanFragment, PlanNode,
    RowSeqScanNode, ScanRange,
};
use risingwave_pb::plan_common::CellBasedTableDesc;
use uuid::Uuid;

use crate::exchange_source::ExchangeSourceImpl;
use crate::execution::grpc_exchange::GrpcExchangeSource;
use crate::executor::join::row_level_iter::RowLevelIter;
use crate::executor::join::{
    concatenate, convert_datum_refs_to_chunk, convert_row_to_chunk, JoinType,
};
use crate::executor::{
    data_chunk_stream, BoxedDataChunkStream, BoxedExecutor, BoxedExecutorBuilder, Executor,
    ExecutorBuilder,
};
use crate::task::BatchTaskContext;

// Build side = "Left side", where we go through its rows one by one
// Probe side = "Right side", where we find matches for each row from the build side

#[derive(Default)]
pub struct ProbeSideSourceParams {
    table_desc: CellBasedTableDesc,
    column_ids: Vec<i32>,
    build_side_idxs: Vec<usize>,
    source_templates: Vec<ProstExchangeSource>,
}

/// Contains the functions of the `GatherExecutor` that are exposed
pub trait GatherExecutor: Send {
    fn new_boxed(cur_row: &RowRef, params: &ProbeSideSourceParams) -> Result<Box<Self>>;

    fn execute(self: Box<Self>) -> BoxedDataChunkStream;
}

/// `DefaultGatherExecutor` sends and retrieves data from the probe side table using RPC calls
struct DefaultGatherExecutor {
    prost_sources: Vec<ProstExchangeSource>,
}

impl DefaultGatherExecutor {
    /// Creates the `RowSeqScanNode` that will be used for scanning the probe side table
    /// based on the passed `RowRef` and the `ProbeSideSourceParams`.
    fn create_row_seq_scan_node(
        cur_row: &RowRef,
        params: &ProbeSideSourceParams,
    ) -> Result<NodeBody> {
        let eq_conds = params
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
            table_desc: Some(params.table_desc.clone()),
            column_ids: params.column_ids.clone(),
            scan_range,
            vnode_bitmap: None,
        }))
    }

    /// Sends RPC calls to retrieve data from the probe side table using the vector of
    /// `ProstExchangeSources` that was created after calling `create_probe_side_prost_sources()`
    #[try_stream(boxed, ok = DataChunk, error = RwError)]
    async fn get_probe_side_chunk(self: Box<Self>) {
        let mut exchange_sources: Vec<ExchangeSourceImpl> = vec![];

        for prost_source in &self.prost_sources {
            let exchange_source =
                ExchangeSourceImpl::Grpc(GrpcExchangeSource::create(prost_source.clone()).await?);
            exchange_sources.push(exchange_source);
        }

        // Merge the streams from all the exchange sources into one stream
        let mut stream = select_all(
            exchange_sources
                .into_iter()
                .map(data_chunk_stream)
                .collect_vec(),
        )
        .boxed();

        while let Some(data_chunk) = stream.next().await {
            let data_chunk = data_chunk?;
            yield data_chunk
        }
    }
}

impl GatherExecutor for DefaultGatherExecutor {
    /// Creates a new boxed `DefaultGatherExecutor` that stores a vector of `ProstExchangeSources`
    /// that eill be used for sending RPC calls. They are made using the `source_templates` in the
    /// passed `ProbeSideSourceParams`.
    fn new_boxed(cur_row: &RowRef, params: &ProbeSideSourceParams) -> Result<Box<Self>> {
        let Plan(inner_template_plan) =
            params.source_templates[0].get_local_execute_plan().unwrap();

        let local_execute_plan = LocalExecutePlan {
            plan: Some(PlanFragment {
                root: Some(PlanNode {
                    children: vec![],
                    identity: Uuid::new_v4().to_string(),
                    node_body: Some(DefaultGatherExecutor::create_row_seq_scan_node(
                        cur_row, params,
                    )?),
                }),
                exchange_info: None,
            }),
            epoch: inner_template_plan.epoch,
        };

        let prost_sources = params
            .source_templates
            .iter()
            .map(|source| ProstExchangeSource {
                task_output_id: Some(source.get_task_output_id().unwrap().clone()),
                host: Some(source.get_host().unwrap().clone()),
                local_execute_plan: Some(Plan(local_execute_plan.clone())),
            })
            .collect();

        Ok(Box::new(DefaultGatherExecutor { prost_sources }))
    }

    /// Execute the `GatherExecutor` and retrieve the data chunk stream
    fn execute(self: Box<Self>) -> BoxedDataChunkStream {
        self.get_probe_side_chunk()
    }
}

/// Lookup Join Executor.
/// High-level idea:
/// 1) Iterate through each row in the build side
/// 2) For each row R, find the rows that match with R on the join conditions on the probe side by
/// using the `GatherExecutor` to query the probe side
/// 3) Join R and the matching rows on the probe side together with either inner or left outer join
/// 4) Repeat 2-3) for every row in the build side
pub struct LookupJoinExecutor<GE> {
    join_type: JoinType,
    condition: Option<BoxedExpression>,
    build_side_source: RowLevelIter,
    build_side_data_types: Vec<DataType>,
    probe_side_data_types: Vec<DataType>,
    probe_side_source_params: ProbeSideSourceParams,
    chunk_builder: DataChunkBuilder,
    schema: Schema,
    output_indices: Vec<usize>,
    last_chunk: Option<SlicedDataChunk>,
    identity: String,
    unused: PhantomData<GE>, // Only exists so we can use generic type
}

impl<GE: 'static + GatherExecutor> Executor for LookupJoinExecutor<GE> {
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

impl<GE: 'static + GatherExecutor> LookupJoinExecutor<GE> {
    #[try_stream(boxed, ok = DataChunk, error = RwError)]
    async fn do_execute(mut self: Box<Self>) {
        self.build_side_source.load_data().await?;

        loop {
            let cur_row = self.build_side_source.get_current_row_ref();
            if cur_row.is_none() {
                break;
            }
            let cur_row = cur_row.unwrap();

            let gather_executor = GE::new_boxed(&cur_row, &self.probe_side_source_params)?;
            let mut probe_side_stream = gather_executor.execute();

            let mut chunk_added = false;

            loop {
                let probe_side_chunk = probe_side_stream.next().await;
                let chunk_is_none = probe_side_chunk.is_none();

                // If None is received for the first time on the probe_side_stream AND it's a Left
                // Outer Join AND no chunks have been added for the current row, don't break yet
                // as we need to add NULL-padded values
                if chunk_is_none && (self.join_type == JoinType::Inner || chunk_added) {
                    break;
                }

                // Join the cur_row and the probe_side_chunk depending on the given join type
                // Currently, Lookup Join only supports Inner and Left Outer Join
                let join_result = match self.join_type {
                    JoinType::Inner => self.do_inner_join(probe_side_chunk),
                    // Only perform a Left Outer Join if chunk_is_none (and by extension no chunk
                    // has been added yet for the current row)
                    JoinType::LeftOuter if chunk_is_none => self.do_left_outer_join(),
                    JoinType::LeftOuter if !chunk_is_none => self.do_inner_join(probe_side_chunk),
                    _ => Err(ErrorCode::NotImplemented(
                        format!(
                            "Lookup Join does not support join type {:?}",
                            self.join_type
                        ),
                        None.into(),
                    )
                    .into()),
                }?;

                // Append chunk from the join result to the chunk builder if it exists
                if let Some(return_chunk) = join_result {
                    if return_chunk.cardinality() > 0 {
                        // println!{"{:?}", return_chunk}
                        chunk_added = true;

                        if let Some(inner_chunk) =
                            self.append_chunk(SlicedDataChunk::new_checked(return_chunk)?)?
                        {
                            yield inner_chunk.reorder_columns(&self.output_indices);
                        }
                    }
                }

                // Until we don't have any more last_chunks to append to the chunk builder,
                // keep appending them to the chunk builder
                while self.last_chunk.is_some() {
                    let mut temp_chunk: Option<SlicedDataChunk> = None;
                    std::mem::swap(&mut temp_chunk, &mut self.last_chunk);
                    if let Some(inner_chunk) = self.append_chunk(temp_chunk.unwrap())? {
                        yield inner_chunk.reorder_columns(&self.output_indices);
                    }
                }
            }

            self.build_side_source.advance_row();
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
        probe_side_chunk: Option<Result<DataChunk>>,
    ) -> Result<Option<DataChunk>> {
        if let Some(probe_chunk) = probe_side_chunk {
            let cur_row = self.build_side_source.get_current_row_ref().unwrap();
            let probe_chunk = probe_chunk?;

            let const_row_chunk = convert_row_to_chunk(
                &cur_row,
                probe_chunk.capacity(),
                &self.build_side_data_types,
            )?;

            let new_chunk = concatenate(&const_row_chunk, &probe_chunk)?;

            if let Some(cond) = self.condition.as_ref() {
                let visibility = cond.eval(&new_chunk)?;
                Ok(Some(
                    new_chunk.with_visibility(visibility.as_bool().iter().collect()),
                ))
            } else {
                Ok(Some(new_chunk))
            }
        } else {
            Ok(None)
        }
    }

    /// Pad the row out with NULLs and return it.
    fn do_left_outer_join(&mut self) -> Result<Option<DataChunk>> {
        let mut build_datum_refs = self
            .build_side_source
            .get_current_row_ref()
            .unwrap()
            .values()
            .collect_vec();

        for _ in 0..self.probe_side_data_types.len() {
            build_datum_refs.push(None);
        }

        let one_row_chunk =
            convert_datum_refs_to_chunk(&build_datum_refs, 1, &self.schema.data_types())?;

        Ok(Some(one_row_chunk))
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

        let build_child = inputs.remove(0);
        let build_side_data_types = build_child.schema().data_types();

        let probe_side_schema = Schema {
            fields: lookup_join_node
                .get_probe_side_schema()
                .iter()
                .map(Field::from)
                .collect(),
        };

        let output_indices: Vec<usize> = lookup_join_node
            .get_output_indices()
            .iter()
            .map(|&x| x as usize)
            .collect();

        let fields = [
            build_child.schema().fields.clone(),
            probe_side_schema.fields.clone(),
        ]
        .concat();
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

        let probe_side_table_desc = lookup_join_node.get_probe_side_table_desc()?;

        ensure!(!lookup_join_node.get_sources().is_empty());
        let probe_side_source_params = ProbeSideSourceParams {
            table_desc: probe_side_table_desc.clone(),
            column_ids: lookup_join_node.get_probe_side_column_ids().to_vec(),
            build_side_idxs,
            source_templates: lookup_join_node.get_sources().to_vec(),
        };

        Ok(Box::new(LookupJoinExecutor::<DefaultGatherExecutor> {
            join_type,
            condition,
            build_side_source: RowLevelIter::new(build_child),
            build_side_data_types,
            probe_side_data_types: probe_side_schema.data_types(),
            probe_side_source_params,
            chunk_builder: DataChunkBuilder::with_default_size(original_schema.data_types()),
            schema: actual_schema,
            output_indices,
            last_chunk: None,
            identity: "LookupJoinExecutor".to_string(),
            unused: PhantomData,
        }))
    }
}

#[cfg(test)]
mod tests {
    use std::marker::PhantomData;

    use risingwave_common::array::{DataChunk, DataChunkTestExt};
    use risingwave_common::catalog::{Field, Schema};
    use risingwave_common::types::{DataType, ScalarImpl};
    use risingwave_common::util::chunk_coalesce::DataChunkBuilder;
    use risingwave_expr::expr::expr_binary_nonnull::new_binary_expr;
    use risingwave_expr::expr::{BoxedExpression, InputRefExpression, LiteralExpression};
    use risingwave_pb::expr::expr_node::Type;

    use crate::executor::join::row_level_iter::RowLevelIter;
    use crate::executor::join::JoinType;
    use crate::executor::test_utils::{diff_executor_output, MockExecutor, MockGatherExecutor};
    use crate::executor::{BoxedExecutor, LookupJoinExecutor, ProbeSideSourceParams};

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

        let fields = [
            build_child.schema().fields.clone(),
            build_child.schema().fields.clone(),
        ]
        .concat();
        let original_schema = Schema { fields };

        Box::new(LookupJoinExecutor::<MockGatherExecutor> {
            join_type,
            condition,
            build_side_data_types: build_child.schema().data_types(),
            probe_side_data_types: build_child.schema().data_types(),
            build_side_source: RowLevelIter::new(build_child),
            probe_side_source_params: ProbeSideSourceParams::default(),
            chunk_builder: DataChunkBuilder::with_default_size(original_schema.data_types()),
            schema: original_schema.clone(),
            output_indices: (0..original_schema.len()).into_iter().collect(),
            last_chunk: None,
            identity: "TestLookupJoinExecutor".to_string(),
            unused: PhantomData,
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
