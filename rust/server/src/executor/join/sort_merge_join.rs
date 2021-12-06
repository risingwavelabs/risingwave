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
                        Ok(Box::new(Self {
                            join_type,
                            chunk_builder: DataChunkBuilder::new_with_default_size(
                                schema.data_types_clone(),
                            ),
                            schema,
                            probe_side_source: probe_table_source,
                            build_side_source: build_table_source,

                            probe_key_idxs,
                            build_key_idxs,
                            last_join_results_write_idx: 0,
                            last_join_results: vec![],
                            last_probe_key: None,
                            sort_order: OrderType::Ascending,
                        }))
                    }
                    _ => unimplemented!("Do not support {:?} join type now.", join_type),
                }
            }
            (_, _) => Err(InternalError("Filter must have one children".to_string()).into()),
        }
    }
}
