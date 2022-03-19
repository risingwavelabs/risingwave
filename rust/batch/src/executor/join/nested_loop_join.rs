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
//
use std::option::Option::Some;
use std::sync::Arc;

use risingwave_common::array::column::Column;
use risingwave_common::array::data_chunk_iter::RowRef;
use risingwave_common::array::{ArrayBuilderImpl, DataChunk, Row};
use risingwave_common::catalog::Schema;
use risingwave_common::error::ErrorCode::InternalError;
use risingwave_common::error::Result;
use risingwave_common::expr::{build_from_prost as expr_build_from_prost, BoxedExpression};
use risingwave_common::types::DataType;
use risingwave_common::util::chunk_coalesce::{DataChunkBuilder, SlicedDataChunk};
use risingwave_pb::plan::plan_node::NodeBody;

use crate::executor::join::chunked_data::RowId;
use crate::executor::join::row_level_iter::RowLevelIter;
use crate::executor::join::JoinType;
use crate::executor::{BoxedExecutor, BoxedExecutorBuilder, Executor, ExecutorBuilder};

/// Nested loop join executor.
///
///
/// High Level Idea:
/// 1. Iterate tuple from probe table.
/// 2. Concatenated with inner chunk, eval expression and get sel vector
/// 3. Create new chunk with new sel vector and yield to upper.
pub struct NestedLoopJoinExecutor {
    /// Expression to eval join condition
    join_expr: BoxedExpression,
    /// Executor should handle different join type.
    join_type: JoinType,
    /// Manage inner source and expression evaluation.
    state: NestedLoopJoinState,
    schema: Schema,
    /// Return data chunk in batch.
    chunk_builder: DataChunkBuilder,
    /// Cache the chunk that has not been written into chunk builder
    /// during join probing. Flush it in begin of execution.
    last_chunk: Option<SlicedDataChunk>,
    /// The data type of probe side. Cache to avoid copy too much.
    probe_side_schema: Vec<DataType>,
    /// Row-level iteration of probe side.
    probe_side_source: RowLevelIter,
    /// The table used for look up matched rows.
    build_table: RowLevelIter,

    /// Used in probe remaining iteration.
    probe_remain_chunk_idx: usize,
    probe_remain_row_idx: usize,

    /// Identity string of the executor
    identity: String,
}

/// If current row finished probe, executor will advance to next row.
/// If there is batched chunk generated in probing, return it. Note that:
/// `None` do not mean no matched row find (it is written in [`chunk_builder`])
struct ProbeResult {
    cur_row_finished: bool,
    chunk: Option<DataChunk>,
}

enum NestedLoopJoinState {
    /// [`Build`] should load all inner table into memory.
    Build,
    /// One Difference between [`FirstProbe`] and [`Probe`]:
    /// Only init the probe side source until [`FirstProbe`] so that
    /// avoid unnecessary init if build side fail.
    FirstProbe,
    /// [`Probe`] finds matching rows for all outer table.
    Probe,
    ProbeRemaining,
    Done,
}

#[async_trait::async_trait]
impl Executor for NestedLoopJoinExecutor {
    async fn open(&mut self) -> Result<()> {
        match self.state {
            NestedLoopJoinState::Build => {
                // TODO: Do not all read to memory if OOM.
                self.build_table.load_data().await?;
                self.state = NestedLoopJoinState::FirstProbe;
            }
            _ => unreachable!(),
        }
        Ok(())
    }

    async fn next(&mut self) -> Result<Option<DataChunk>> {
        if let Some(last_chunk) = self.last_chunk.take() {
            let (left_data_chunk, return_chunk) = self.chunk_builder.append_chunk(last_chunk)?;
            self.last_chunk = left_data_chunk;
            if let Some(return_chunk_inner) = return_chunk {
                return Ok(Some(return_chunk_inner));
            }
        }
        // Make sure all buffered chunk are flushed before generate more chunks.
        // It's impossible that: left chunk not null and return chunk is null.
        assert!(self.last_chunk.is_none());

        loop {
            match self.state {
                NestedLoopJoinState::Build => unreachable!(),

                NestedLoopJoinState::FirstProbe => {
                    let ret = self.probe(true).await?;
                    self.state = NestedLoopJoinState::Probe;
                    if let Some(data_chunk) = ret {
                        return Ok(Some(data_chunk));
                    }
                }
                NestedLoopJoinState::Probe => {
                    let ret = self.probe(false).await?;
                    if let Some(data_chunk) = ret {
                        return Ok(Some(data_chunk));
                    }
                }

                NestedLoopJoinState::ProbeRemaining => {
                    let ret = self.probe_remaining()?;
                    if let Some(data_chunk) = ret {
                        return Ok(Some(data_chunk));
                    }
                    self.state = NestedLoopJoinState::Done;
                }

                NestedLoopJoinState::Done => {
                    return if let Some(data_chunk) = self.chunk_builder.consume_all()? {
                        Ok(Some(data_chunk))
                    } else {
                        Ok(None)
                    }
                }
            }
        }
    }
    async fn close(&mut self) -> Result<()> {
        Ok(())
    }

    fn schema(&self) -> &Schema {
        &self.schema
    }

    fn identity(&self) -> &str {
        &self.identity
    }
}

impl NestedLoopJoinExecutor {
    /// Create constant data chunk (one tuple repeat `capacity` times).
    fn convert_row_to_chunk(
        &self,
        row_ref: &RowRef<'_>,
        num_tuples: usize,
        data_types: &[DataType],
    ) -> Result<DataChunk> {
        let num_columns = data_types.len();
        let mut output_array_builders = data_types
            .iter()
            .map(|data_type| data_type.create_array_builder(num_tuples))
            .collect::<Result<Vec<ArrayBuilderImpl>>>()?;
        for _i in 0..num_tuples {
            for (col_idx, builder) in output_array_builders
                .iter_mut()
                .enumerate()
                .take(num_columns)
            {
                builder.append_datum_ref(row_ref.value_at(col_idx))?;
            }
        }

        // Finish each array builder and get Column.
        let result_columns = output_array_builders
            .into_iter()
            .map(|builder| builder.finish().map(|arr| Column::new(Arc::new(arr))))
            .collect::<Result<Vec<Column>>>()?;

        Ok(DataChunk::builder().columns(result_columns).build())
    }
}

impl BoxedExecutorBuilder for NestedLoopJoinExecutor {
    fn new_boxed_executor(source: &ExecutorBuilder) -> Result<BoxedExecutor> {
        ensure!(source.plan_node().get_children().len() == 2);

        let nested_loop_join_node = try_match_expand!(
            source.plan_node().get_node_body().unwrap(),
            NodeBody::NestedLoopJoin
        )?;

        let join_type = JoinType::from_prost(nested_loop_join_node.get_join_type()?);
        let join_expr = expr_build_from_prost(nested_loop_join_node.get_join_cond()?)?;
        // Note: Assume that optimizer has set up probe side and build side. Left is the probe side,
        // right is the build side. This is the same for all join executors.
        let left_plan_opt = source.plan_node().get_children().get(0);
        let right_plan_opt = source.plan_node().get_children().get(1);
        match (left_plan_opt, right_plan_opt) {
            (Some(left_plan), Some(right_plan)) => {
                let left_child = source.clone_for_plan(left_plan).build()?;
                let probe_side_schema = left_child.schema().data_types();
                let right_child = source.clone_for_plan(right_plan).build()?;

                // TODO(Bowen): Merge this with derive schema in Logical Join (#790).
                let fields = match join_type {
                    JoinType::LeftSemi => left_child.schema().fields.clone(),
                    JoinType::LeftAnti => left_child.schema().fields.clone(),
                    JoinType::RightSemi => right_child.schema().fields.clone(),
                    JoinType::RightAnti => right_child.schema().fields.clone(),
                    _ => left_child
                        .schema()
                        .fields
                        .iter()
                        .chain(right_child.schema().fields.iter())
                        .cloned()
                        .collect(),
                };

                let schema = Schema { fields };
                match join_type {
                    JoinType::Inner
                    | JoinType::LeftOuter
                    | JoinType::RightOuter
                    | JoinType::LeftSemi
                    | JoinType::LeftAnti
                    | JoinType::RightSemi
                    | JoinType::RightAnti => {
                        // TODO: Support FULL OUTER.
                        let outer_table_source = RowLevelIter::new(left_child);

                        let join_state = NestedLoopJoinState::Build;
                        Ok(Box::new(Self {
                            join_expr,
                            join_type,
                            state: join_state,
                            chunk_builder: DataChunkBuilder::new_with_default_size(
                                schema.data_types(),
                            ),
                            schema,
                            last_chunk: None,
                            probe_side_schema,
                            probe_side_source: outer_table_source,
                            build_table: RowLevelIter::new(right_child),
                            probe_remain_chunk_idx: 0,
                            probe_remain_row_idx: 0,
                            identity: "NestedLoopJoinExecutor".to_string(),
                        }))
                    }
                    _ => unimplemented!("Do not support {:?} join type now.", join_type),
                }
            }
            (_, _) => Err(InternalError("Filter must have one children".to_string()).into()),
        }
    }
}

impl NestedLoopJoinExecutor {
    /// Probe matched rows in `probe_table`.
    /// # Arguments
    /// * `first_probe`: whether the first probing. Init outer source if yes.
    /// * `probe_table`: Table to be probed.
    /// If nothing gained from `probe_table.join()`, Advance to next row until all outer tuples are
    /// exhausted.
    async fn probe(&mut self, first_probe: bool) -> Result<Option<DataChunk>> {
        if first_probe {
            self.probe_side_source.load_data().await?;
        }
        let cur_row = self.probe_side_source.get_current_row_ref();
        // TODO(Bowen): If we assume the scanned chunk is always compact (no invisible tuples),
        // remove this check.
        if cur_row.is_some() {
            // Dispatch to each kind of join type
            let probe_result = match self.join_type {
                JoinType::Inner => self.do_inner_join(),
                JoinType::LeftOuter => self.do_left_outer_join(),
                JoinType::LeftSemi => self.do_left_semi_join(),
                JoinType::LeftAnti => self.do_left_anti_join(),
                JoinType::RightOuter => self.do_right_outer_join(),
                JoinType::RightSemi => self.do_right_semi_join(),
                JoinType::RightAnti => self.do_right_anti_join(),
                _ => unimplemented!("Do not support other join types!"),
            }?;

            if probe_result.cur_row_finished {
                self.probe_side_source.advance_row();
                // Probe row is changed, scan from the starting point of build table again.
                self.build_table.reset_chunk();
            }

            if let Some(ret_chunk) = probe_result.chunk {
                // Note: we can avoid the append chunk in join -- Only do it in the begin of outer
                // loop. But currently seems like it do not have too much gain. Will
                // keep look on it.
                if ret_chunk.capacity() > 0 {
                    let (mut left_data_chunk, return_data_chunk) = self
                        .chunk_builder
                        .append_chunk(SlicedDataChunk::new_checked(ret_chunk)?)?;
                    // Have checked last chunk is None in before. Now swap to buffer it.
                    std::mem::swap(&mut self.last_chunk, &mut left_data_chunk);
                    if let Some(inner_chunk) = return_data_chunk {
                        return Ok(Some(inner_chunk));
                    }
                }
            }
        } else {
            self.state = if self.join_type.need_join_remaining() {
                NestedLoopJoinState::ProbeRemaining
            } else {
                NestedLoopJoinState::Done
            };
        }
        Ok(None)
    }

    /// Similar to [`probe_remaining`] in [`HashJoin`]. For nested loop join, iterate the build
    /// table and append row if not matched in [`NestedLoopJoinState::Probe`].
    fn probe_remaining(&mut self) -> Result<Option<DataChunk>> {
        match self.join_type {
            JoinType::RightOuter => self.do_probe_remaining_right_outer(),
            JoinType::RightAnti => self.do_probe_remaining_right_anti(),
            _ => unimplemented!(""),
        }
    }

    fn do_inner_join(&mut self) -> Result<ProbeResult> {
        if let Some(build_side_chunk) = self.build_table.get_current_chunk() {
            // Checked the option before, so impossible to panic.
            let probe_row = self.probe_side_source.get_current_row_ref().unwrap();
            let const_row_chunk = self.convert_row_to_chunk(
                &probe_row,
                build_side_chunk.capacity(),
                &self.probe_side_schema,
            )?;
            let new_chunk = Self::concatenate(&const_row_chunk, build_side_chunk)?;
            // Join with current row.
            let sel_vector = self.join_expr.eval(&new_chunk)?;
            let ret_chunk = new_chunk.with_visibility(sel_vector.as_bool().try_into()?);
            self.build_table.advance_chunk();
            Ok(ProbeResult {
                cur_row_finished: false,
                chunk: Some(ret_chunk),
            })
        } else {
            self.build_table.reset_chunk();
            Ok(ProbeResult {
                cur_row_finished: true,
                chunk: None,
            })
        }
    }

    fn do_left_outer_join(&mut self) -> Result<ProbeResult> {
        let ret = self.do_inner_join()?;
        if let Some(inner) = ret.chunk.as_ref() {
            if inner.cardinality() > 0 {
                self.probe_side_source.set_cur_row_matched(true);
            }
        }
        // Append (probed_row, None) to chunk builder if current row finished probing and do not
        // find any match.
        if ret.cur_row_finished && !self.probe_side_source.get_cur_row_matched() {
            assert!(ret.chunk.is_none());
            let mut probe_row = self.probe_side_source.get_current_row_ref().unwrap();
            for _ in 0..self.build_table.get_schema().fields.len() {
                probe_row.0.push(None);
            }
            let one_row_chunk =
                self.convert_row_to_chunk(&probe_row, 1, &self.schema.data_types())?;
            return Ok(ProbeResult {
                cur_row_finished: true,
                chunk: Some(one_row_chunk),
            });
        }
        Ok(ret)
    }

    fn do_left_semi_join(&mut self) -> Result<ProbeResult> {
        let mut ret = self.do_inner_join()?;
        if let Some(inner) = ret.chunk.as_ref() {
            if inner.cardinality() > 0 {
                self.probe_side_source.set_cur_row_matched(true);
            }
        }
        ret.chunk = None;
        // Append (probed_row, None) to chunk builder if current row finished probing and do not
        // find any match.
        if self.probe_side_source.get_cur_row_matched() {
            let probe_row = self.probe_side_source.get_current_row_ref().unwrap();
            let one_row_chunk =
                self.convert_row_to_chunk(&probe_row, 1, &self.probe_side_schema)?;
            return Ok(ProbeResult {
                cur_row_finished: true,
                chunk: Some(one_row_chunk),
            });
        }
        Ok(ret)
    }

    fn do_left_anti_join(&mut self) -> Result<ProbeResult> {
        let mut ret = self.do_inner_join()?;
        if let Some(inner) = ret.chunk.as_ref() {
            if inner.cardinality() > 0 {
                self.probe_side_source.set_cur_row_matched(true);
            }
        }
        ret.chunk = None;
        if ret.cur_row_finished && !self.probe_side_source.get_cur_row_matched() {
            assert!(ret.chunk.is_none());
            let probe_row = self.probe_side_source.get_current_row_ref().unwrap();
            let one_row_chunk =
                self.convert_row_to_chunk(&probe_row, 1, &self.probe_side_schema)?;
            return Ok(ProbeResult {
                cur_row_finished: true,
                chunk: Some(one_row_chunk),
            });
        }
        Ok(ret)
    }

    fn do_right_outer_join(&mut self) -> Result<ProbeResult> {
        let ret = self.do_inner_join()?;
        // Mark matched rows to prepare for probe remaining.
        self.mark_matched_rows(&ret)?;
        Ok(ret)
    }

    /// Scan through probe results chunk and mark matched rows in build table. Therefore in probe
    /// remaining, scan through build table and we can know each row whether has been matched in
    /// probing. Used by `RIGHT_SEMI/ANTI/OUTER` Join.
    fn mark_matched_rows<'a>(&'a mut self, ret: &'a ProbeResult) -> Result<Vec<Row>> {
        let mut rows_ref = vec![];
        if let Some(inner_chunk) = ret.chunk.as_ref() {
            for row_idx in 0..inner_chunk.capacity() {
                let (row_ref, vis) = inner_chunk.row_at(row_idx)?;
                if vis {
                    let row_id = RowId::new(self.build_table.get_chunk_idx() - 1, row_idx);
                    // Only write this row if it have not been marked before.
                    if !self.build_table.is_build_matched(row_id)?
                        && self.join_type == JoinType::RightSemi
                    {
                        rows_ref.push(row_ref.row_by_slice(
                            &(self.probe_side_schema.len()..row_ref.size()).collect::<Vec<usize>>(),
                        ));
                    }
                    self.build_table.set_build_matched(row_id)?;
                    self.probe_side_source.set_cur_row_matched(true);
                }
            }
        }
        Ok(rows_ref)
    }

    fn do_right_semi_join(&mut self) -> Result<ProbeResult> {
        let mut ret = self.do_inner_join()?;
        let rows = self.mark_matched_rows(&ret)?;
        ret.chunk = None;
        if !rows.is_empty() {
            let ret_chunk =
                DataChunk::from_rows(&rows, &self.build_table.get_schema().data_types())?;
            ret.chunk = Some(ret_chunk);
        }
        Ok(ret)
    }

    fn do_right_anti_join(&mut self) -> Result<ProbeResult> {
        let mut ret = self.do_inner_join()?;
        // Mark matched rows to prepare for probe remaining.
        self.mark_matched_rows(&ret)?;
        ret.chunk = None;
        Ok(ret)
    }

    fn do_probe_remaining_right_outer(&mut self) -> Result<Option<DataChunk>> {
        while let Some(cur_row) = self.build_table.get_current_row_ref() {
            // If the build row has not been matched by probe row before.
            if !self
                .build_table
                .is_build_matched(self.build_table.get_current_row_id())?
            {
                let mut cur_row_vec = cur_row.0;
                for _ in 0..self.probe_side_source.get_schema().fields.len() {
                    cur_row_vec.insert(0, None);
                }
                if let Some(ret_chunk) = self
                    .chunk_builder
                    .append_one_row_ref(RowRef::new(cur_row_vec))?
                {
                    return Ok(Some(ret_chunk));
                }
            }
            self.build_table.advance_row();
        }
        Ok(None)
    }

    fn do_probe_remaining_right_anti(&mut self) -> Result<Option<DataChunk>> {
        while let Some(cur_row) = self.build_table.get_current_row_ref() {
            if !self
                .build_table
                .is_build_matched(self.build_table.get_current_row_id())?
            {
                if let Some(ret_chunk) = self.chunk_builder.append_one_row_ref(cur_row)? {
                    return Ok(Some(ret_chunk));
                }
            }
            self.build_table.advance_row();
        }
        Ok(None)
    }

    /// The layout be like:
    ///
    /// [ `left` chunk     |  `right` chunk     ]
    ///
    /// # Arguments
    ///
    /// * `left` Data chunk padded to the left half of result data chunk..
    /// * `right` Data chunk padded to the right half of result data chunk.
    ///
    /// Note: Use this function with careful: It is not designed to be a general concatenate of two
    /// chunk: Usually one side should be const row chunk and the other side is normal chunk.
    /// Currently only feasible to use in join executor.
    /// If two normal chunk, the result is undefined.
    fn concatenate(left: &DataChunk, right: &DataChunk) -> Result<DataChunk> {
        assert_eq!(left.capacity(), right.capacity());
        let mut concated_columns = Vec::with_capacity(left.columns().len() + right.columns().len());
        concated_columns.extend_from_slice(left.columns());
        concated_columns.extend_from_slice(right.columns());
        // Only handle one side is constant row chunk: One of visibility must be None.
        let vis = match (left.visibility(), right.visibility()) {
            (None, _) => right.visibility().clone(),
            (_, None) => left.visibility().clone(),
            (Some(_), Some(_)) => {
                unimplemented!(
                    "The concatenate behaviour of two chunk with visibility is undefined"
                )
            }
        };
        let builder = DataChunk::builder().columns(concated_columns);
        let data_chunk = if let Some(vis) = vis {
            builder.visibility(vis).build()
        } else {
            builder.build()
        };
        Ok(data_chunk)
    }
}

#[cfg(test)]
mod tests {
    use std::convert::TryFrom;
    use std::sync::Arc;

    use risingwave_common::array::column::Column;
    use risingwave_common::array::*;
    use risingwave_common::catalog::{Field, Schema};
    use risingwave_common::expr::expr_binary_nonnull::new_binary_expr;
    use risingwave_common::expr::InputRefExpression;
    use risingwave_common::types::{DataType, ScalarRefImpl};
    use risingwave_common::util::chunk_coalesce::DataChunkBuilder;
    use risingwave_pb::expr::expr_node::Type;

    use crate::executor::join::nested_loop_join::{
        NestedLoopJoinExecutor, NestedLoopJoinState, RowLevelIter,
    };
    use crate::executor::join::JoinType;
    use crate::executor::test_utils::{diff_executor_output, MockExecutor};
    use crate::executor::BoxedExecutor;

    /// Test combine two chunk into one.
    #[test]
    fn test_concatenate() {
        let num_of_columns: usize = 2;
        let length = 5;
        let mut columns = vec![];
        for i in 0..num_of_columns {
            let mut builder = PrimitiveArrayBuilder::<i32>::new(length).unwrap();
            for _ in 0..length {
                builder.append(Some(i as i32)).unwrap();
            }
            let arr = builder.finish().unwrap();
            columns.push(Column::new(Arc::new(arr.into())))
        }
        let chunk1: DataChunk = DataChunk::builder().columns(columns.clone()).build();
        let bool_vec = vec![true, false, true, false, false];
        let chunk2: DataChunk = DataChunk::builder()
            .columns(columns.clone())
            .visibility((bool_vec.clone()).try_into().unwrap())
            .build();
        let chunk = NestedLoopJoinExecutor::concatenate(&chunk1, &chunk2).unwrap();
        assert_eq!(chunk.capacity(), chunk1.capacity());
        assert_eq!(chunk.capacity(), chunk2.capacity());
        assert_eq!(chunk.columns().len(), chunk1.columns().len() * 2);
        assert_eq!(
            chunk.visibility().clone().unwrap(),
            (bool_vec).try_into().unwrap()
        );
    }

    /// Test the function of convert row into constant row chunk (one row repeat multiple times).
    #[test]
    fn test_convert_row_to_chunk() {
        let row = RowRef::new(vec![Some(ScalarRefImpl::Int32(3))]);
        let probe_side_schema = Schema {
            fields: vec![Field::unnamed(DataType::Int32)],
        };
        let probe_source = Box::new(MockExecutor::new(probe_side_schema.clone()));
        let build_source = Box::new(MockExecutor::new(probe_side_schema.clone()));
        // Note that only probe side schema of this executor is meaningful. All other fields are
        // meaningless. They are just used to pass Rust checker.
        let source = NestedLoopJoinExecutor {
            join_expr: Box::new(InputRefExpression::new(DataType::Int32, 0)),
            join_type: JoinType::Inner,
            state: NestedLoopJoinState::Build,
            chunk_builder: DataChunkBuilder::new_with_default_size(probe_side_schema.data_types()),
            schema: Schema { fields: vec![] },
            last_chunk: None,
            probe_side_schema: probe_side_schema.data_types(),
            probe_side_source: RowLevelIter::new(probe_source),
            build_table: RowLevelIter::new(build_source),
            probe_remain_chunk_idx: 0,
            probe_remain_row_idx: 0,
            identity: "NestedLoopJoinExecutor".to_string(),
        };
        let const_row_chunk = source
            .convert_row_to_chunk(&row, 5, &probe_side_schema.data_types())
            .unwrap();
        assert_eq!(const_row_chunk.capacity(), 5);
        assert_eq!(
            const_row_chunk.row_at(2).unwrap().0.value_at(0),
            Some(ScalarRefImpl::Int32(3))
        );
    }

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
                    array! {F32Array, [Some(6.6f32), Some(0.7f32), Some(5.5f32), Some(5.6f32), Some(7.0f32)]}.into(),
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

            // TODO(Bowen): Merge this with derive schema in Logical Join.
            let fields = match self.join_type {
                JoinType::LeftSemi => left_child.schema().fields.clone(),
                JoinType::LeftAnti => left_child.schema().fields.clone(),
                JoinType::RightSemi => right_child.schema().fields.clone(),
                JoinType::RightAnti => right_child.schema().fields.clone(),
                _ => left_child
                    .schema()
                    .fields
                    .iter()
                    .chain(right_child.schema().fields.iter())
                    .cloned()
                    .collect(),
            };
            let schema = Schema { fields };

            let probe_side_schema = left_child.schema().data_types();

            Box::new(NestedLoopJoinExecutor {
                join_expr: new_binary_expr(
                    Type::Equal,
                    DataType::Boolean,
                    Box::new(InputRefExpression::new(DataType::Int32, 0)),
                    Box::new(InputRefExpression::new(DataType::Int32, 2)),
                ),
                join_type,
                state: NestedLoopJoinState::Build,
                schema: schema.clone(),
                chunk_builder: DataChunkBuilder::new_with_default_size(schema.data_types()),
                last_chunk: None,
                probe_side_schema,
                probe_side_source: RowLevelIter::new(left_child),
                build_table: RowLevelIter::new(right_child),
                probe_remain_chunk_idx: 0,
                probe_remain_row_idx: 0,
                identity: "NestedLoopJoinExecutor".to_string(),
            })
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

    /// sql: select * from t1 left outer join t2 on t1.v1 = t2.v1
    #[tokio::test]
    async fn test_left_outer_join() {
        let test_fixture = TestFixture::with_join_type(JoinType::LeftOuter);

        let column1 = Column::new(Arc::new(
        array! {I32Array, [Some(1), Some(2), Some(3), Some(3), Some(4), Some(6), Some(6), Some(8)]}.into(),
        ));

        let column2 = Column::new(Arc::new(array! {F32Array, [Some(6.1f32), Some(8.4f32), Some(3.9f32), Some(6.6f32), Some(0.7f32), Some(5.5f32), Some(5.6f32), Some(7.0f32)]}.into()));

        let column3 = Column::new(Arc::new(
            array! {I32Array, [None, Some(2), Some(3), Some(3), None, Some(6), Some(6), Some(8)]}
                .into(),
        ));

        let column4 = Column::new(Arc::new(array! {F64Array, [None, Some(6.1f64), Some(8.9f64), Some(8.9f64), None, Some(3.4f64), Some(3.4f64), Some(3.5f64)]}.into()));

        let expected_chunk = DataChunk::try_from(vec![column1, column2, column3, column4])
            .expect("Failed to create chunk!");

        test_fixture.do_test(expected_chunk).await;
    }

    #[tokio::test]
    async fn test_left_semi_join() {
        let test_fixture = TestFixture::with_join_type(JoinType::LeftSemi);

        let column1 = Column::new(Arc::new(
            array! {I32Array, [Some(2), Some(3), Some(3), Some(6), Some(6), Some(8)]}.into(),
        ));

        let column2 = Column::new(Arc::new(array! {F32Array, [Some(8.4f32), Some(3.9f32), Some(6.6f32), Some(5.5f32), Some(5.6f32), Some(7.0f32)]}.into()));

        let expected_chunk =
            DataChunk::try_from(vec![column1, column2]).expect("Failed to create chunk!");

        test_fixture.do_test(expected_chunk).await;
    }

    #[tokio::test]
    async fn test_left_anti_join() {
        let test_fixture = TestFixture::with_join_type(JoinType::LeftAnti);

        let column1 = Column::new(Arc::new(array! {I32Array, [Some(1), Some(4)]}.into()));

        let column2 = Column::new(Arc::new(
            array! {F32Array, [Some(6.1f32), Some(0.7f32)]}.into(),
        ));

        let expected_chunk =
            DataChunk::try_from(vec![column1, column2]).expect("Failed to create chunk!");

        test_fixture.do_test(expected_chunk).await;
    }

    #[tokio::test]
    async fn test_right_semi_join() {
        let test_fixture = TestFixture::with_join_type(JoinType::RightSemi);

        let column1 = Column::new(Arc::new(
            array! {I32Array, [Some(2), Some(3), Some(6), Some(8)]}.into(),
        ));

        let column2 = Column::new(Arc::new(
            array! {F64Array, [Some(6.1f64), Some(8.9f64), Some(3.4f64), Some(3.5f64)]}.into(),
        ));

        let expected_chunk =
            DataChunk::try_from(vec![column1, column2]).expect("Failed to create chunk!");

        test_fixture.do_test(expected_chunk).await;
    }

    #[tokio::test]
    async fn test_right_anti_join() {
        let test_fixture = TestFixture::with_join_type(JoinType::RightAnti);

        let column1 = Column::new(Arc::new(
            array! {I32Array, [Some(9), Some(10), Some(11), Some(12), Some(20), Some(30), Some(100), Some(200)]}.into(),
        ));

        let column2 = Column::new(Arc::new(array! {F64Array, [Some(7.5f64), None, Some(8f64), None, Some(5.7f64), Some(9.6f64), None, Some(8.18f64)]}.into()));

        let expected_chunk =
            DataChunk::try_from(vec![column1, column2]).expect("Failed to create chunk!");

        test_fixture.do_test(expected_chunk).await;
    }
}
