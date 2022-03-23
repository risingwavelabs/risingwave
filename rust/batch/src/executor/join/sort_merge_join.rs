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

use risingwave_common::array::{DataChunk, Row, RowRef};
use risingwave_common::catalog::Schema;
use risingwave_common::error::ErrorCode::InternalError;
use risingwave_common::util::chunk_coalesce::DataChunkBuilder;
use risingwave_common::util::sort_util::OrderType;
use risingwave_pb::plan::plan_node::NodeBody;
use risingwave_pb::plan::OrderType as OrderTypeProst;

use crate::executor::join::row_level_iter::RowLevelIter;
use crate::executor::join::JoinType;
use crate::executor::{BoxedExecutor, BoxedExecutorBuilder, Executor, ExecutorBuilder};

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
    /// Sort key column index of probe side and build side.
    /// They should have the same length and be one-to-one mapping.
    probe_key_idxs: Vec<usize>,
    build_key_idxs: Vec<usize>,
    /// Record the index that have been written into chunk builder.
    last_join_results_write_idx: usize,
    /// Identity string of the executor
    identity: String,
}

#[async_trait::async_trait]
impl Executor for SortMergeJoinExecutor {
    async fn open(&mut self) -> risingwave_common::error::Result<()> {
        // Init data source.
        self.probe_side_source.load_data().await?;
        self.build_side_source.load_data().await
    }

    /// The code logic:
    /// For each loop, only process one tuple from probe side and one tuple from build side. After
    /// one loop, either probe side or build side will advance to next tuple and start a new
    /// loop. The key design is, maintain the last probe key and last join results of current
    /// row. When start a new loop, check whether current row is the same with last probe key.
    /// If yes, directly append last join results until next unequal probe key.
    ///
    /// The complexity is hard to avoid. May need more tests to ensure the correctness.
    async fn next(&mut self) -> risingwave_common::error::Result<Option<DataChunk>> {
        loop {
            // If current row is the same with last row. Directly append cached join results.
            if self.compare_with_last_row(self.probe_side_source.get_current_row_ref()) {
                if let Some(cur_probe_row) = self.probe_side_source.get_current_row_ref() {
                    while self.last_join_results_write_idx < self.last_join_results.len() {
                        let build_row = &self.last_join_results[self.last_join_results_write_idx];
                        self.last_join_results_write_idx += 1;
                        // Concatenate to get the final join results.
                        let join_row =
                            Self::combine_two_row_ref(cur_probe_row.clone(), build_row.into());
                        if let Some(ret_chunk) = self.chunk_builder.append_one_row_ref(join_row)? {
                            return Ok(Some(ret_chunk));
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
                    let probe_key = Row::from(
                        cur_probe_row_ref
                            .value_by_slice(&self.probe_key_idxs)
                            .clone(),
                    );
                    let build_key = Row::from(
                        cur_build_row_ref
                            .value_by_slice(&self.build_key_idxs)
                            .clone(),
                    );

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
                                .push(cur_build_row_ref.clone().into());
                            let join_row = Self::combine_two_row_ref(
                                cur_probe_row_ref.clone(),
                                cur_build_row_ref.clone(),
                            );
                            let ret = self.chunk_builder.append_one_row_ref(join_row.clone())?;
                            self.build_side_source.advance_row();
                            if let Some(ret_chunk) = ret {
                                return Ok(Some(ret_chunk));
                            }
                        }
                    }
                }

                (Some(cur_probe_row_ref), None) => {
                    self.last_probe_key = Some(
                        cur_probe_row_ref
                            .value_by_slice(&self.probe_key_idxs)
                            .into(),
                    );
                    self.probe_side_source.advance_row();
                }
                // Once probe row is None, consume all results or terminate.
                (_, _) => {
                    return if let Some(ret) = self.chunk_builder.consume_all()? {
                        Ok(Some(ret))
                    } else {
                        Ok(None)
                    };
                }
            }
        }
    }

    async fn close(&mut self) -> risingwave_common::error::Result<()> {
        Ok(())
    }

    fn schema(&self) -> &Schema {
        &self.schema
    }

    fn identity(&self) -> &str {
        &self.identity
    }
}

impl SortMergeJoinExecutor {
    pub(super) fn new(
        join_type: JoinType,
        schema: Schema,
        probe_side_source: RowLevelIter,
        build_side_source: RowLevelIter,
        probe_key_idxs: Vec<usize>,
        build_key_idxs: Vec<usize>,
        identity: String,
    ) -> Self {
        Self {
            join_type,
            chunk_builder: DataChunkBuilder::new_with_default_size(schema.data_types()),
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
        }
    }
    fn compare_with_last_row(&self, cur_row: Option<RowRef>) -> bool {
        self.last_probe_key
            .clone()
            .zip(cur_row)
            .map_or(false, |(row1, row2)| {
                row1 == row2.value_by_slice(&self.probe_key_idxs).into()
            })
    }

    fn combine_two_row_ref<'a>(left_row: RowRef<'a>, right_row: RowRef<'a>) -> RowRef<'a> {
        let row_vec = left_row
            .0
            .into_iter()
            .chain(right_row.0.into_iter())
            .collect::<Vec<_>>();
        RowRef::new(row_vec)
    }
}

impl BoxedExecutorBuilder for SortMergeJoinExecutor {
    fn new_boxed_executor(
        source: &ExecutorBuilder,
    ) -> risingwave_common::error::Result<BoxedExecutor> {
        ensure!(source.plan_node().get_children().len() == 2);

        let sort_merge_join_node = try_match_expand!(
            source.plan_node().get_node_body().unwrap(),
            NodeBody::SortMergeJoin
        )?;

        let sort_order = sort_merge_join_node.get_direction()?;
        // Only allow it in ascending order.
        ensure!(sort_order == OrderTypeProst::Ascending);
        let join_type = JoinType::from_prost(sort_merge_join_node.get_join_type()?);
        // Note: Assume that optimizer has set up probe side and build side. Left is the probe side,
        // right is the build side. This is the same for all join executors.
        let left_plan_opt = source.plan_node().get_children().get(0);
        let right_plan_opt = source.plan_node().get_children().get(1);
        match (left_plan_opt, right_plan_opt) {
            (Some(left_plan), Some(right_plan)) => {
                let left_child = source.clone_for_plan(left_plan).build()?;
                let right_child = source.clone_for_plan(right_plan).build()?;

                let fields = left_child
                    .schema()
                    .fields
                    .iter()
                    .chain(right_child.schema().fields.iter())
                    .cloned()
                    .collect();
                let schema = Schema { fields };

                let left_keys = sort_merge_join_node.get_left_keys();
                let mut probe_key_idxs = vec![];
                for key in left_keys {
                    probe_key_idxs.push(*key as usize);
                }

                let right_keys = sort_merge_join_node.get_right_keys();
                let mut build_key_idxs = vec![];
                for key in right_keys {
                    build_key_idxs.push(*key as usize);
                }
                match join_type {
                    JoinType::Inner => {
                        // TODO: Support more join type.
                        let probe_table_source = RowLevelIter::new(left_child);
                        let build_table_source = RowLevelIter::new(right_child);
                        Ok(Box::new(Self::new(
                            join_type,
                            schema,
                            probe_table_source,
                            build_table_source,
                            probe_key_idxs,
                            build_key_idxs,
                            "SortMergeJoinExecutor".to_string(),
                        )))
                    }
                    _ => unimplemented!("Do not support {:?} join type now.", join_type),
                }
            }
            (_, _) => Err(InternalError("Filter must have one children".to_string()).into()),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::convert::TryFrom;
    use std::sync::Arc;

    use risingwave_common::array::column::Column;
    use risingwave_common::array::{DataChunk, F32Array, F64Array, I32Array};
    use risingwave_common::catalog::{Field, Schema};
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

            {
                let column1 = Column::new(Arc::new(
                    array! {I32Array, [Some(1), Some(2), Some(3)]}.into(),
                ));
                let column2 = Column::new(Arc::new(
                    array! {F32Array, [Some(6.1f32), Some(8.4f32), Some(3.9f32)]}.into(),
                ));

                let chunk =
                    DataChunk::try_from(vec![column1, column2]).expect("Failed to create chunk!");
                executor.add(chunk);
            }

            {
                let column1 = Column::new(Arc::new(
                    array! {I32Array, [Some(3), Some(4), Some(6), Some(6), Some(8)]}.into(),
                ));
                let column2 = Column::new(Arc::new(
                    array! {F32Array, [
                        Some(6.6f32), Some(0.7f32), Some(5.5f32), Some(5.6f32), Some(7.0f32)
                    ]}
                    .into(),
                ));

                let chunk =
                    DataChunk::try_from(vec![column1, column2]).expect("Failed to create chunk!");
                executor.add(chunk);
            }

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

            {
                let column1 = Column::new(Arc::new(
                    array! {I32Array, [Some(2), Some(3), Some(6), Some(8)]}.into(),
                ));

                let column2 = Column::new(Arc::new(
                    array! {F64Array, [Some(6.1f64), Some(8.9f64), Some(3.4f64), Some(3.5f64)]}
                        .into(),
                ));

                let chunk =
                    DataChunk::try_from(vec![column1, column2]).expect("Failed to create chunk!");
                executor.add(chunk);
            }

            {
                let column1 = Column::new(Arc::new(
                    array! {I32Array, [Some(9), Some(10), Some(11), Some(12)]}.into(),
                ));

                let column2 = Column::new(Arc::new(
                    array! {F64Array, [Some(7.5f64), None, Some(8f64), None]}.into(),
                ));

                let chunk =
                    DataChunk::try_from(vec![column1, column2]).expect("Failed to create chunk!");
                executor.add(chunk);
            }

            {
                let column1 = Column::new(Arc::new(
                    array! {I32Array, [Some(20), Some(30), Some(100), Some(200)]}.into(),
                ));

                let column2 = Column::new(Arc::new(
                    array! {F64Array, [Some(5.7f64),  Some(9.6f64), None, Some(8.18f64)]}.into(),
                ));

                let chunk =
                    DataChunk::try_from(vec![column1, column2]).expect("Failed to create chunk!");
                executor.add(chunk);
            }

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

            Box::new(SortMergeJoinExecutor::new(
                join_type,
                schema,
                RowLevelIter::new(left_child),
                RowLevelIter::new(right_child),
                vec![0],
                vec![0],
                "SortMergeJoinExecutor".to_string(),
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

        let column1 = Column::new(Arc::new(
            array! {I32Array, [Some(2), Some(3), Some(3), Some(6), Some(6), Some(8)]}.into(),
        ));

        let column2 = Column::new(Arc::new(array! {F32Array, [Some(8.4f32), Some(3.9f32), Some(6.6f32), Some(5.5f32), Some(5.6f32), Some(7.0f32)]}.into()));

        let column3 = Column::new(Arc::new(
            array! {I32Array, [Some(2), Some(3), Some(3), Some(6), Some(6), Some(8)]}.into(),
        ));

        let column4 = Column::new(Arc::new(array! {F64Array, [Some(6.1f64), Some(8.9f64), Some(8.9f64), Some(3.4f64), Some(3.4f64), Some(3.5f64)]}.into()));

        let expected_chunk = DataChunk::try_from(vec![column1, column2, column3, column4])
            .expect("Failed to create chunk!");

        test_fixture.do_test(expected_chunk).await;
    }
}
