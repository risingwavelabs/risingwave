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

use std::cmp::Ordering;

use futures_async_stream::try_stream;
use risingwave_common::array::{DataChunk, Row, RowRef};
use risingwave_common::catalog::Schema;
use risingwave_common::error::RwError;
use risingwave_common::types::to_datum_ref;
use risingwave_common::util::chunk_coalesce::DataChunkBuilder;
use risingwave_common::util::sort_util::OrderType;
use risingwave_pb::batch_plan::plan_node::NodeBody;
use risingwave_pb::plan_common::OrderType as OrderTypeProst;

use crate::error::BatchError;
use crate::executor::join::row_level_iter::RowLevelIter;
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
    join_type: JoinType,
    /// Row-level iteration of probe side.
    probe_side_source: RowLevelIter,
    /// Row-level iteration of build side.
    build_side_source: RowLevelIter,
    /// Return data chunk in batch.
    chunk_builder: DataChunkBuilder,
    /// Join result of last row. It only contains the build side. Should concatenate with probe row
    /// when write into chunk builder.
    last_join_results: Vec<Row>,
    /// Last probe row key (Part of probe row). None for the first probe.
    last_probe_key: Option<Row>,
    schema: Schema,
    /// We may only need certain columns.
    /// output_indices are the indices of the columns that we needed.
    output_indices: Vec<usize>,
    /// Sort key column index of probe side and build side.
    /// They should have the same length and be one-to-one mapping.
    probe_key_idxs: Vec<usize>,
    build_key_idxs: Vec<usize>,
    /// Record the index that have been written into chunk builder.
    last_join_results_write_idx: usize,
    /// Identity string of the executor
    identity: String,
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
    async fn do_execute(mut self: Box<Self>) {
        // Init data source.
        self.probe_side_source.load_data().await?;
        self.build_side_source.load_data().await?;

        loop {
            // If current row is the same with last row. Directly append cached join results.
            if self.compare_with_last_row(self.probe_side_source.get_current_row_ref()) {
                let current_row_ref = self.probe_side_source.get_current_row_ref();
                if let Some(cur_probe_row) = current_row_ref {
                    while self.last_join_results_write_idx < self.last_join_results.len() {
                        let build_row = &self.last_join_results[self.last_join_results_write_idx];
                        self.last_join_results_write_idx += 1;
                        // Concatenate to get the final join results.
                        let datum_refs = cur_probe_row
                            .values()
                            .chain(build_row.0.iter().map(to_datum_ref));
                        if let Some(ret_chunk) = self
                            .chunk_builder
                            .append_one_row_from_datum_refs(datum_refs)?
                        {
                            yield ret_chunk.reorder_columns(&self.output_indices);
                        }
                    }
                }
                self.last_join_results_write_idx = 0;
                self.probe_side_source.advance_row();
                if self.compare_with_last_row(self.probe_side_source.get_current_row_ref()) {
                    continue;
                } else {
                    // Clear last_join result if not equal occur.
                    self.last_join_results.clear();
                }
            }

            // Do not care the vis.
            let cur_probe_row_opt = self.probe_side_source.get_current_row_ref();
            let cur_build_row_opt = self.build_side_source.get_current_row_ref();

            match (cur_probe_row_opt, cur_build_row_opt) {
                (Some(cur_probe_row_ref), Some(cur_build_row_ref)) => {
                    let probe_key = cur_probe_row_ref.row_by_indices(&self.probe_key_idxs);
                    let build_key = cur_build_row_ref.row_by_indices(&self.build_key_idxs);

                    // TODO: [`Row`] may not be PartialOrd. May use some trait like
                    // [`ScalarPartialOrd`].
                    match probe_key.cmp(&build_key) {
                        Ordering::Greater => {
                            if self.sort_order == OrderType::Descending {
                                // Before advance to next row, record last probe key.
                                self.last_probe_key = Some(probe_key);
                                self.probe_side_source.advance_row();
                                if !self.compare_with_last_row(
                                    self.probe_side_source.get_current_row_ref(),
                                ) {
                                    self.last_join_results.clear();
                                }
                            } else {
                                self.build_side_source.advance_row();
                            }
                        }

                        Ordering::Less => {
                            if self.sort_order == OrderType::Descending {
                                self.build_side_source.advance_row();
                            } else {
                                self.last_probe_key = Some(probe_key);
                                self.probe_side_source.advance_row();
                                if !self.compare_with_last_row(
                                    self.probe_side_source.get_current_row_ref(),
                                ) {
                                    self.last_join_results.clear();
                                }
                            }
                        }

                        Ordering::Equal => {
                            // Matched rows. Write into chunk builder and maintain last join
                            // results.
                            self.last_join_results
                                .push(cur_build_row_ref.to_owned_row());
                            let join_datum_refs =
                                cur_probe_row_ref.values().chain(cur_build_row_ref.values());
                            let ret = self
                                .chunk_builder
                                .append_one_row_from_datum_refs(join_datum_refs)?;
                            self.build_side_source.advance_row();
                            if let Some(ret_chunk) = ret {
                                yield ret_chunk.reorder_columns(&self.output_indices);
                            }
                        }
                    }
                }

                (Some(cur_probe_row_ref), None) => {
                    self.last_probe_key =
                        Some(cur_probe_row_ref.row_by_indices(&self.probe_key_idxs));
                    self.probe_side_source.advance_row();
                }
                // Once probe row is None, consume all results or terminate.
                (_, _) => {
                    if let Some(ret) = self.chunk_builder.consume_all()? {
                        yield ret.reorder_columns(&self.output_indices)
                    } else {
                        break;
                    };
                }
            }
        }
    }
}

impl SortMergeJoinExecutor {
    #[allow(clippy::too_many_arguments)]
    pub(super) fn new(
        join_type: JoinType,
        schema: Schema,
        output_indices: Vec<usize>,
        probe_side_source: RowLevelIter,
        build_side_source: RowLevelIter,
        probe_key_idxs: Vec<usize>,
        build_key_idxs: Vec<usize>,
        identity: String,
    ) -> Self {
        Self {
            join_type,
            chunk_builder: DataChunkBuilder::with_default_size(schema.data_types()),
            schema,
            probe_side_source,
            build_side_source,
            probe_key_idxs,
            build_key_idxs,
            last_join_results_write_idx: 0,
            last_join_results: vec![],
            last_probe_key: None,
            sort_order: OrderType::Ascending,
            identity,
            output_indices,
        }
    }

    fn compare_with_last_row(&self, cur_row: Option<RowRef>) -> bool {
        self.last_probe_key
            .clone()
            .zip(cur_row)
            .map_or(false, |(row1, row2)| {
                row1 == row2.row_by_indices(&self.probe_key_idxs)
            })
    }
}

#[async_trait::async_trait]
impl BoxedExecutorBuilder for SortMergeJoinExecutor {
    async fn new_boxed_executor<C: BatchTaskContext>(
        source: &ExecutorBuilder<C>,
        mut inputs: Vec<BoxedExecutor>,
    ) -> risingwave_common::error::Result<BoxedExecutor> {
        ensure!(
            inputs.len() == 2,
            "SortMergeJoinExecutor should have 2 children!"
        );

        let sort_merge_join_node = try_match_expand!(
            source.plan_node().get_node_body().unwrap(),
            NodeBody::SortMergeJoin
        )?;

        let sort_order = sort_merge_join_node.get_direction()?;
        // Only allow it in ascending order.
        ensure!(sort_order == OrderTypeProst::Ascending);
        let join_type = JoinType::from_prost(sort_merge_join_node.get_join_type()?);

        let left_child = inputs.remove(0);
        let right_child = inputs.remove(0);

        let fields = left_child
            .schema()
            .fields
            .iter()
            .chain(right_child.schema().fields.iter())
            .cloned()
            .collect();
        let output_indices: Vec<usize> = sort_merge_join_node
            .output_indices
            .iter()
            .map(|&x| x as usize)
            .collect();
        let original_schema = Schema { fields };
        let actual_schema = output_indices
            .iter()
            .map(|&idx| original_schema[idx].clone())
            .collect();
        let left_key = sort_merge_join_node.get_left_key();
        let mut probe_key_idxs = vec![];
        for idx in left_key {
            probe_key_idxs.push(*idx as usize);
        }

        let right_key = sort_merge_join_node.get_right_key();
        let mut build_key_idxs = vec![];
        for idx in right_key {
            build_key_idxs.push(*idx as usize);
        }
        match join_type {
            JoinType::Inner => {
                // TODO: Support more join type.
                let probe_table_source = RowLevelIter::new(left_child);
                let build_table_source = RowLevelIter::new(right_child);
                Ok(Box::new(Self::new(
                    join_type,
                    actual_schema,
                    output_indices,
                    probe_table_source,
                    build_table_source,
                    probe_key_idxs,
                    build_key_idxs,
                    "SortMergeJoinExecutor2".to_string(),
                )))
            }
            _ => Err(BatchError::UnsupportedFunction(format!(
                "Do not support {:?} join type now.",
                join_type
            ))
            .into()),
        }
    }
}

#[cfg(test)]
mod tests {
    use risingwave_common::array::DataChunk;
    use risingwave_common::catalog::{Field, Schema};
    use risingwave_common::test_prelude::DataChunkTestExt;
    use risingwave_common::types::DataType;

    use crate::executor::join::sort_merge_join::{RowLevelIter, SortMergeJoinExecutor};
    use crate::executor::join::JoinType;
    use crate::executor::test_utils::{diff_executor_output, MockExecutor};
    use crate::executor::BoxedExecutor;

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
                join_type,
                schema,
                (0..schema_len).into_iter().collect(),
                RowLevelIter::new(left_child),
                RowLevelIter::new(right_child),
                vec![0],
                vec![0],
                "SortMergeJoinExecutor2".to_string(),
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
