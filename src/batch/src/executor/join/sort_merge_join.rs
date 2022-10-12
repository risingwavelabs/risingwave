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
use std::sync::Arc;

use futures::TryStreamExt;
use futures_async_stream::try_stream;
use risingwave_common::array::DataChunk;
use risingwave_common::catalog::Schema;
use risingwave_common::error::RwError;
use risingwave_common::util::chunk_coalesce::DataChunkBuilder;
use risingwave_common::util::sort_util::OrderType;
use risingwave_pb::batch_plan::plan_node::NodeBody;
use risingwave_pb::plan_common::OrderType as OrderTypeProst;

use crate::executor::join::JoinType;
use crate::executor::{
    BoxedDataChunkStream, BoxedExecutor, BoxedExecutorBuilder, Executor, ExecutorBuilder,
};
use crate::task::BatchTaskContext;

/// [`SortMergeJoinExecutor`] will not sort the data. If the join key is not sorted, optimizer
/// should insert a sort executor above the data source.
pub struct SortMergeJoinExecutor {
    /// Ascending or descending. Note that currently the sort order of probe side and build side
    /// should be the same.
    sort_order: OrderType,
    /// Currently only inner join is supported.
    join_type: JoinType,
    /// Original output schema.
    original_schema: Schema,
    /// Actual output schema.
    schema: Schema,
    /// We may only need certain columns.
    /// output_indices are the indices of the columns that we needed.
    output_indices: Vec<usize>,
    /// Sort key column index of probe side and build side.
    /// They should have the same length and be one-to-one mapping.
    probe_key_idxs: Vec<usize>,
    build_key_idxs: Vec<usize>,
    /// Probe side source (left table).
    probe_side_source: BoxedExecutor,
    /// Build side source (right table).
    build_side_source: BoxedExecutor,
    /// Identity string of the executor.
    identity: String,
    /// The maximum size of the chunk produced by executor at a time.
    chunk_size: usize,
}

impl Executor for SortMergeJoinExecutor {
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

impl SortMergeJoinExecutor {
    /// The code logic:
    /// For each loop, only process one tuple from probe side and one tuple from build side. After
    /// one loop, either probe side or build side will advance to next tuple and start a new
    /// loop. The key design is, maintain the last probe key and last join results of current
    /// row. When start a new loop, check whether current row is the same with last probe key.
    /// If yes, directly append last join results until next unequal probe key.
    ///
    /// The complexity is hard to avoid. May need more tests to ensure the correctness.
    #[try_stream(boxed, ok = DataChunk, error = RwError)]
    async fn do_execute(self: Box<Self>) {
        let data_types = self.original_schema.data_types();

        let mut chunk_builder = DataChunkBuilder::new(data_types, self.chunk_size);

        // TODO: support more join types
        let stream = match (self.sort_order, self.join_type) {
            (OrderType::Ascending, JoinType::Inner) => Self::do_inner_join::<true>,
            (OrderType::Descending, JoinType::Inner) => Self::do_inner_join::<false>,
            _ => todo!(),
        };

        #[for_await]
        for chunk in stream(
            &mut chunk_builder,
            self.probe_side_source,
            self.build_side_source,
            self.probe_key_idxs,
            self.build_key_idxs,
        ) {
            yield chunk?.reorder_columns(&self.output_indices)
        }

        // Handle remaining chunk
        if let Some(chunk) = chunk_builder.consume_all() {
            yield chunk.reorder_columns(&self.output_indices)
        }
    }

    #[try_stream(boxed, ok = DataChunk, error = RwError)]
    async fn do_inner_join<const ASCENDING: bool>(
        chunk_builder: &mut DataChunkBuilder,
        probe_side: BoxedExecutor,
        build_side: BoxedExecutor,
        probe_key_idxs: Vec<usize>,
        build_key_idxs: Vec<usize>,
    ) {
        let mut build_chunk_iter = build_side.execute();
        let mut last_probe_key = None;
        let mut last_matched_build_rows: Vec<(Arc<DataChunk>, usize)> = Vec::new();
        // Dummy first build side chunk
        let mut build_chunk = Arc::new(DataChunk::new(Vec::new(), 0));
        let mut build_row_idx = 0;

        #[for_await]
        for probe_chunk in probe_side.execute() {
            let probe_chunk = probe_chunk?;
            for probe_row in probe_chunk.rows() {
                let probe_key = probe_row.row_by_indices(&probe_key_idxs);
                // If current probe key equals to last probe key, reuse join results.
                if let Some(last_probe_key) = &last_probe_key && *last_probe_key == probe_key {
                    for (chunk, row_idx) in &last_matched_build_rows {
                        let build_row = chunk.row_at_unchecked_vis(*row_idx);
                        if let Some(spilled) = chunk_builder.append_one_row_from_datum_refs(probe_row.values().chain(build_row.values())) {
                            yield spilled
                        }
                    }
                }
                // Otherwise, do merge join from scratch.
                else {
                    last_matched_build_rows.clear();
                    // Iterate over build side table by rows.
                    loop {
                        if let Some(next_build_row_idx) = build_chunk.next_visible_row_idx(build_row_idx) {
                            let build_row = build_chunk.row_at_unchecked_vis(next_build_row_idx);
                            let build_key = build_row.row_by_indices(&build_key_idxs);
                            // TODO: [`Row`] may not be PartialOrd. May use some trait like
                            // [`ScalarPartialOrd`].
                            if probe_key == build_key {
                                last_matched_build_rows.push((build_chunk.clone(), next_build_row_idx));
                                if let Some(spilled) = chunk_builder.append_one_row_from_datum_refs(probe_row.values().chain(build_row.values())) {
                                    yield spilled
                                }
                            } else if ASCENDING && probe_key < build_key || !ASCENDING && probe_key > build_key {
                                break;
                            }
                            build_row_idx = next_build_row_idx + 1;
                        }
                        // Current build side chunk is drained, fetch the next chunk.
                        else if let Some(next_build_chunk) = build_chunk_iter.try_next().await? {
                            build_chunk = Arc::new(next_build_chunk);
                            build_row_idx = 0;
                        }
                        // Now all build side chunks are fetched.
                        else {
                            break
                        }
                    }
                    last_probe_key = Some(probe_key);
                }
            }
        }
    }
}

impl SortMergeJoinExecutor {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        sort_order: OrderType,
        join_type: JoinType,
        output_indices: Vec<usize>,
        probe_key_idxs: Vec<usize>,
        build_key_idxs: Vec<usize>,
        probe_side_source: BoxedExecutor,
        build_side_source: BoxedExecutor,
        identity: String,
        chunk_size: usize,
    ) -> Self {
        let original_schema = match join_type {
            JoinType::Inner => Schema::from_iter(
                probe_side_source
                    .schema()
                    .fields()
                    .iter()
                    .chain(build_side_source.schema().fields().iter())
                    .cloned(),
            ),
            _ => todo!(),
        };
        let schema = Schema::from_iter(
            output_indices
                .iter()
                .map(|&idx| original_schema[idx].clone()),
        );
        Self {
            sort_order,
            join_type,
            original_schema,
            schema,
            output_indices,
            probe_key_idxs,
            build_key_idxs,
            probe_side_source,
            build_side_source,
            identity,
            chunk_size,
        }
    }
}

#[async_trait::async_trait]
impl BoxedExecutorBuilder for SortMergeJoinExecutor {
    async fn new_boxed_executor<C: BatchTaskContext>(
        source: &ExecutorBuilder<'_, C>,
        inputs: Vec<BoxedExecutor>,
    ) -> risingwave_common::error::Result<BoxedExecutor> {
        let [left_child, right_child]: [_; 2] = inputs.try_into().unwrap();

        let sort_merge_join_node = try_match_expand!(
            source.plan_node().get_node_body().unwrap(),
            NodeBody::SortMergeJoin
        )?;

        let sort_order = sort_merge_join_node.get_direction()?;
        // Only allow it in ascending order.
        ensure!(sort_order == OrderTypeProst::Ascending);
        let sort_order = OrderType::Ascending;
        let join_type = JoinType::from_prost(sort_merge_join_node.get_join_type()?);

        let output_indices: Vec<usize> = sort_merge_join_node
            .output_indices
            .iter()
            .map(|&x| x as usize)
            .collect();
        let probe_key_idxs = sort_merge_join_node
            .get_left_key()
            .iter()
            .map(|&idx| idx as usize)
            .collect();
        let build_key_idxs = sort_merge_join_node
            .get_right_key()
            .iter()
            .map(|&idx| idx as usize)
            .collect();

        Ok(Box::new(Self::new(
            sort_order,
            join_type,
            output_indices,
            probe_key_idxs,
            build_key_idxs,
            left_child,
            right_child,
            source.plan_node().get_identity().clone(),
            source.context.get_config().developer.batch_chunk_size,
        )))
    }
}

#[cfg(test)]
mod tests {
    use risingwave_common::array::DataChunk;
    use risingwave_common::catalog::{Field, Schema};
    use risingwave_common::test_prelude::DataChunkTestExt;
    use risingwave_common::types::DataType;
    use risingwave_common::util::sort_util::OrderType;

    use crate::executor::join::sort_merge_join::SortMergeJoinExecutor;
    use crate::executor::join::JoinType;
    use crate::executor::test_utils::{diff_executor_output, MockExecutor};
    use crate::executor::BoxedExecutor;

    const CHUNK_SIZE: usize = 1024;

    struct TestFixture {
        left_types: Vec<DataType>,
        right_types: Vec<DataType>,
        join_type: JoinType,
    }

    /// Sql for creating test data:
    /// ```sql
    /// drop table t1 if exists;
    /// create table t1(v1 int, v2 float);
    /// insert into t1 values
    /// (1, 6.1::FLOAT), (2, 8.4::FLOAT), (3, 3.9::FLOAT), (3, 6.6::FLOAT), (4, 0.7::FLOAT),
    /// (6, 5.5::FLOAT), (6, 5.6::FLOAT), (8, 7.0::FLOAT);
    ///
    /// drop table t2 if exists;
    /// create table t2(v1 int, v2 real);
    /// insert into t2 values
    /// (2, 6.1::REAL), (3, 8.9::REAL), (6, 3.4::REAL), (8, 3.5::REAL), (9, 7.5::REAL),
    /// (10, null), (11, 8::REAL), (12, null), (20, 5.7::REAL), (30, 9.6::REAL),
    /// (100, null), (200, 8.18::REAL);
    /// ```
    impl TestFixture {
        fn with_join_type(join_type: JoinType) -> Self {
            Self {
                left_types: vec![DataType::Int32, DataType::Float32],
                right_types: vec![DataType::Int32, DataType::Float64],
                join_type,
            }
        }

        fn create_left_executor(&self) -> BoxedExecutor {
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
                 3 6.6
                 4 0.7
                 6 5.5
                 6 5.6
                 8 7.0",
            ));

            Box::new(executor)
        }

        fn create_right_executor(&self) -> BoxedExecutor {
            let schema = Schema {
                fields: vec![
                    Field::unnamed(DataType::Int32),
                    Field::unnamed(DataType::Float64),
                ],
            };
            let mut executor = MockExecutor::new(schema);

            executor.add(DataChunk::from_pretty(
                "i F
                 2 6.1
                 3 8.9
                 6 3.4
                 8 3.5",
            ));

            executor.add(DataChunk::from_pretty(
                " i F
                  9 7.5
                 10 .
                 11 8
                 12 .",
            ));

            executor.add(DataChunk::from_pretty(
                "  i F
                  20 5.7
                  30 9.6
                 100 .
                 200 8.18",
            ));

            Box::new(executor)
        }

        fn create_join_executor(&self) -> BoxedExecutor {
            let join_type = self.join_type;

            let left_child = self.create_left_executor();
            let right_child = self.create_right_executor();

            let fields = left_child
                .schema()
                .fields
                .iter()
                .chain(right_child.schema().fields.iter())
                .cloned()
                .collect();
            let schema = Schema { fields };
            let schema_len = schema.len();
            Box::new(SortMergeJoinExecutor::new(
                OrderType::Ascending,
                join_type,
                (0..schema_len).into_iter().collect(),
                vec![0],
                vec![0],
                left_child,
                right_child,
                "SortMergeJoinExecutor2".to_string(),
                CHUNK_SIZE,
            ))
        }

        async fn do_test(&self, expected: DataChunk) {
            let join_executor = self.create_join_executor();
            let mut expected_mock_exec = MockExecutor::new(join_executor.schema().clone());
            expected_mock_exec.add(expected);

            diff_executor_output(join_executor, Box::new(expected_mock_exec)).await;
        }
    }

    /// sql: select * from t1, t2 where t1.v1 = t2.v1
    #[tokio::test]
    async fn test_inner_join() {
        let test_fixture = TestFixture::with_join_type(JoinType::Inner);

        let expected_chunk = DataChunk::from_pretty(
            "i f   i F
             2 8.4 2 6.1
             3 3.9 3 8.9
             3 6.6 3 8.9
             6 5.5 6 3.4
             6 5.6 6 3.4
             8 7.0 8 3.5",
        );

        test_fixture.do_test(expected_chunk).await;
    }
}
