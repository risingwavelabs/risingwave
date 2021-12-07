use crate::executor::join::nested_loop_join::ProbeSideSource;
use crate::executor::join::JoinType;
use crate::executor::{BoxedExecutor, BoxedExecutorBuilder, Executor, ExecutorBuilder};
use prost::Message;
use risingwave_common::array::{DataChunk, Row, RowRef};
use risingwave_common::catalog::{Field, Schema};
use risingwave_common::error::ErrorCode::InternalError;
use risingwave_common::error::{ErrorCode, RwError};
use risingwave_common::util::chunk_coalesce::DataChunkBuilder;
use risingwave_common::util::sort_util::OrderType;
use risingwave_pb::plan::OrderType as OrderTypeProst;
use risingwave_pb::plan::{plan_node::PlanNodeType, SortMergeJoinNode};
use std::cmp::Ordering;

/// [`SortMergeJoinExecutor`] will not sort the data. If the join key is not sorted, optimizer
/// should insert a sort executor above the data source.
pub struct SortMergeJoinExecutor {
    /// Ascending or descending. Note that currently the sort order of probe side and build side
    /// should be the same.
    sort_order: OrderType,
    join_type: JoinType,
    /// Row-level iteration of probe side.
    probe_side_source: ProbeSideSource,
    /// Row-level iteration of build side.
    build_side_source: ProbeSideSource,
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
}

#[async_trait::async_trait]
impl Executor for SortMergeJoinExecutor {
    async fn open(&mut self) -> risingwave_common::error::Result<()> {
        // Init data source.
        self.probe_side_source.init().await?;
        self.build_side_source.init().await
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
            if self.compare_with_last_row(self.probe_side_source.current_row_ref_unchecked_vis()?) {
                if let Some(cur_probe_row) =
                    self.probe_side_source.current_row_ref_unchecked_vis()?
                {
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
                self.probe_side_source.advance().await?;
                if self
                    .compare_with_last_row(self.probe_side_source.current_row_ref_unchecked_vis()?)
                {
                    continue;
                } else {
                    // Clear last_join result if not equal occur.
                    self.last_join_results.clear();
                }
            }

            // Do not care the vis.
            let cur_probe_row_opt = self.probe_side_source.current_row_ref_unchecked_vis()?;
            let cur_build_row_opt = self.build_side_source.current_row_ref_unchecked_vis()?;

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
                                self.probe_side_source.advance().await?;
                                if !self.compare_with_last_row(
                                    self.probe_side_source.current_row_ref_unchecked_vis()?,
                                ) {
                                    self.last_join_results.clear();
                                }
                            } else {
                                self.build_side_source.advance().await?;
                            }
                        }

                        Ordering::Less => {
                            if self.sort_order == OrderType::Descending {
                                self.build_side_source.advance().await?;
                            } else {
                                self.last_probe_key = Some(probe_key);
                                self.probe_side_source.advance().await?;
                                if !self.compare_with_last_row(
                                    self.probe_side_source.current_row_ref_unchecked_vis()?,
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
                            self.build_side_source.advance().await?;
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
                    self.probe_side_source.advance().await?;
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
        self.probe_side_source.clean().await?;
        self.build_side_source.clean().await
    }

    fn schema(&self) -> &Schema {
        &self.schema
    }
}

impl SortMergeJoinExecutor {
    pub(super) fn new(
        join_type: JoinType,
        schema: Schema,
        probe_side_source: ProbeSideSource,
        build_side_source: ProbeSideSource,
        probe_key_idxs: Vec<usize>,
        build_key_idxs: Vec<usize>,
    ) -> Self {
        Self {
            join_type,
            chunk_builder: DataChunkBuilder::new_with_default_size(schema.data_types_clone()),
            schema,
            probe_side_source,
            build_side_source,
            probe_key_idxs,
            build_key_idxs,
            last_join_results_write_idx: 0,
            last_join_results: vec![],
            last_probe_key: None,
            sort_order: OrderType::Ascending,
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
        ensure!(source.plan_node().get_node_type() == PlanNodeType::SortMergeJoin);
        ensure!(source.plan_node().get_children().len() == 2);
        let sort_merge_join_node =
            SortMergeJoinNode::decode(&(source.plan_node()).get_body().value[..])
                .map_err(|e| RwError::from(ErrorCode::ProstError(e)))?;
        let sort_order = sort_merge_join_node.get_direction();
        // Only allow it in ascending order.
        ensure!(sort_order == OrderTypeProst::Ascending);
        let join_type = JoinType::from_prost(sort_merge_join_node.get_join_type());
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
                    .map(|f| Field {
                        data_type: f.data_type.clone(),
                    })
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
                        let probe_table_source = ProbeSideSource::new(left_child);
                        let build_table_source = ProbeSideSource::new(right_child);
                        Ok(Box::new(Self::new(
                            join_type,
                            schema,
                            probe_table_source,
                            build_table_source,
                            probe_key_idxs,
                            build_key_idxs,
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
    use crate::executor::join::nested_loop_join::ProbeSideSource;
    use crate::executor::join::sort_merge_join::SortMergeJoinExecutor;
    use crate::executor::join::JoinType;
    use crate::executor::test_utils::diff_executor_output;
    use crate::executor::test_utils::MockExecutor;
    use crate::executor::BoxedExecutor;
    use risingwave_common::array::column::Column;
    use risingwave_common::array::DataChunk;
    use risingwave_common::array::{F32Array, F64Array, I32Array};
    use risingwave_common::catalog::{Field, Schema};
    use risingwave_common::types::{DataTypeRef, Float32Type, Float64Type, Int32Type};
    use std::convert::TryFrom;
    use std::sync::Arc;

    struct TestFixture {
        left_types: Vec<DataTypeRef>,
        right_types: Vec<DataTypeRef>,
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
                left_types: vec![
                    Arc::new(Int32Type::new(false)),
                    Arc::new(Float32Type::new(true)),
                ],
                right_types: vec![
                    Arc::new(Int32Type::new(false)),
                    Arc::new(Float64Type::new(true)),
                ],
                join_type,
            }
        }

        fn create_left_executor(&self) -> BoxedExecutor {
            let schema = Schema {
                fields: vec![
                    Field {
                        data_type: Int32Type::create(false),
                    },
                    Field {
                        data_type: Float32Type::create(true),
                    },
                ],
            };
            let mut executor = MockExecutor::new(schema);

            {
                let column1 = Column::new(
                    Arc::new(array! {I32Array, [Some(1), Some(2), Some(3)]}.into()),
                    self.left_types[0].clone(),
                );
                let column2 = Column::new(
                    Arc::new(array! {F32Array, [Some(6.1f32), Some(8.4f32), Some(3.9f32)]}.into()),
                    self.left_types[1].clone(),
                );

                let chunk =
                    DataChunk::try_from(vec![column1, column2]).expect("Failed to create chunk!");
                executor.add(chunk);
            }

            {
                let column1 = Column::new(
                    Arc::new(
                        array! {I32Array, [Some(3), Some(4), Some(6), Some(6), Some(8)]}.into(),
                    ),
                    self.left_types[0].clone(),
                );
                let column2 = Column::new(
          Arc::new(array! {F32Array, [Some(6.6f32), Some(0.7f32), Some(5.5f32), Some(5.6f32), Some(7.0f32)]}.into()),
          self.left_types[1].clone(),
        );

                let chunk =
                    DataChunk::try_from(vec![column1, column2]).expect("Failed to create chunk!");
                executor.add(chunk);
            }

            Box::new(executor)
        }

        fn create_right_executor(&self) -> BoxedExecutor {
            let schema = Schema {
                fields: vec![
                    Field {
                        data_type: Int32Type::create(false),
                    },
                    Field {
                        data_type: Float64Type::create(true),
                    },
                ],
            };
            let mut executor = MockExecutor::new(schema);

            {
                let column1 = Column::new(
                    Arc::new(array! {I32Array, [Some(2), Some(3), Some(6), Some(8)]}.into()),
                    self.right_types[0].clone(),
                );

                let column2 = Column::new(
                    Arc::new(
                        array! {F64Array, [Some(6.1f64), Some(8.9f64), Some(3.4f64), Some(3.5f64)]}
                            .into(),
                    ),
                    self.right_types[1].clone(),
                );

                let chunk =
                    DataChunk::try_from(vec![column1, column2]).expect("Failed to create chunk!");
                executor.add(chunk);
            }

            {
                let column1 = Column::new(
                    Arc::new(array! {I32Array, [Some(9), Some(10), Some(11), Some(12)]}.into()),
                    self.right_types[0].clone(),
                );

                let column2 = Column::new(
                    Arc::new(array! {F64Array, [Some(7.5f64), None, Some(8f64), None]}.into()),
                    self.right_types[1].clone(),
                );

                let chunk =
                    DataChunk::try_from(vec![column1, column2]).expect("Failed to create chunk!");
                executor.add(chunk);
            }

            {
                let column1 = Column::new(
                    Arc::new(array! {I32Array, [Some(20), Some(30), Some(100), Some(200)]}.into()),
                    self.right_types[0].clone(),
                );

                let column2 = Column::new(
                    Arc::new(
                        array! {F64Array, [Some(5.7f64),  Some(9.6f64), None, Some(8.18f64)]}
                            .into(),
                    ),
                    self.right_types[1].clone(),
                );

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
                .map(|f| Field {
                    data_type: f.data_type.clone(),
                })
                .collect();
            let schema = Schema { fields };

            Box::new(SortMergeJoinExecutor::new(
                join_type,
                schema,
                ProbeSideSource::new(left_child),
                ProbeSideSource::new(right_child),
                vec![0],
                vec![0],
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

        let column1 = Column::new(
            Arc::new(
                array! {I32Array, [Some(2), Some(3), Some(3), Some(6), Some(6), Some(8)]}.into(),
            ),
            test_fixture.left_types[0].clone(),
        );

        let column2 = Column::new(
      Arc::new(array! {F32Array, [Some(8.4f32), Some(3.9f32), Some(6.6f32), Some(5.5f32), Some(5.6f32), Some(7.0f32)]}.into()),
      test_fixture.left_types[1].clone(),
    );

        let column3 = Column::new(
            Arc::new(
                array! {I32Array, [Some(2), Some(3), Some(3), Some(6), Some(6), Some(8)]}.into(),
            ),
            test_fixture.right_types[0].clone(),
        );

        let column4 = Column::new(
      Arc::new(array! {F64Array, [Some(6.1f64), Some(8.9f64), Some(8.9f64), Some(3.4f64), Some(3.4f64), Some(3.5f64)]}.into()),
      test_fixture.right_types[1].clone(),
    );

        let expected_chunk = DataChunk::try_from(vec![column1, column2, column3, column4])
            .expect("Failed to create chunk!");

        test_fixture.do_test(expected_chunk).await;
    }
}
