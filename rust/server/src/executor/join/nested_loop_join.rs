use std::sync::Arc;

use prost::Message;

use crate::executor::join::chunked_data::{ChunkedData, RowId};
use crate::executor::join::JoinType;
use crate::executor::{BoxedExecutor, BoxedExecutorBuilder, Executor, ExecutorBuilder};
use crate::risingwave_common::array::Array;
use risingwave_common::array::column::Column;
use risingwave_common::array::data_chunk_iter::RowRef;
use risingwave_common::array::{ArrayBuilderImpl, DataChunk};
use risingwave_common::catalog::{Field, Schema};
use risingwave_common::error::ErrorCode::InternalError;
use risingwave_common::error::{ErrorCode, Result, RwError};
use risingwave_common::expr::build_from_prost as expr_build_from_prost;
use risingwave_common::expr::BoxedExpression;
use risingwave_common::types::{DataType, DataTypeRef};
use risingwave_common::util::chunk_coalesce::{DataChunkBuilder, SlicedDataChunk};
use risingwave_pb::plan::{plan_node::PlanNodeType, NestedLoopJoinNode};

/// Nested loop join executor.
///
/// Currently the Nested Loop Join do not only consider INNER JOIN.
/// OUTER/SEMI/ANTI JOIN will be added in future PR.
///
/// High Level Idea:
/// 1. Iterate tuple from [`OuterTableSource`]
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
    probe_side_schema: Vec<DataTypeRef>,
    /// Row-level iteration of probe side.
    probe_side_source: ProbeSideSource,
    /// The table used for look up matched rows.
    build_table: BuildTable,

    /// Used in probe remaining iteration.
    probe_remain_chunk_idx: usize,
    probe_remain_row_idx: usize,
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
                    if let Some(data_chunk) = self.chunk_builder.consume_all()? {
                        return Ok(Some(data_chunk));
                    } else {
                        return Ok(None);
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
}

impl NestedLoopJoinExecutor {
    /// Create constant data chunk (one tuple repeat `capacity` times).
    fn convert_row_to_chunk(&self, row_ref: &RowRef<'_>, num_tuples: usize) -> Result<DataChunk> {
        let data_types = &self.probe_side_schema;
        let num_columns = data_types.len();
        let mut output_array_builders = data_types
            .iter()
            .map(|data_type| DataType::create_array_builder(data_type.as_ref(), num_tuples))
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
            .zip(data_types.iter())
            .map(|(builder, data_type)| {
                builder
                    .finish()
                    .map(|arr| Column::new(Arc::new(arr), data_type.clone()))
            })
            .collect::<Result<Vec<Column>>>()?;

        Ok(DataChunk::new(result_columns, None))
    }
}

impl BoxedExecutorBuilder for NestedLoopJoinExecutor {
    fn new_boxed_executor(source: &ExecutorBuilder) -> Result<BoxedExecutor> {
        ensure!(source.plan_node().get_node_type() == PlanNodeType::NestedLoopJoin);
        ensure!(source.plan_node().get_children().len() == 2);

        let nested_loop_join_node =
            NestedLoopJoinNode::decode(&(source.plan_node()).get_body().value[..])
                .map_err(|e| RwError::from(ErrorCode::ProstError(e)))?;

        let join_type = JoinType::from_prost(nested_loop_join_node.get_join_type());
        let join_expr = expr_build_from_prost(nested_loop_join_node.get_join_cond())?;
        // Note: Assume that optimizer has set up probe side and build side. Left is the probe side,
        // right is the build side. This is the same for all join executors.
        let left_plan_opt = source.plan_node().get_children().get(0);
        let right_plan_opt = source.plan_node().get_children().get(1);
        match (left_plan_opt, right_plan_opt) {
            (Some(left_plan), Some(right_plan)) => {
                let left_child = source.clone_for_plan(left_plan).build()?;
                let probe_side_schema = left_child.schema().data_types_clone();
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
                match join_type {
                    JoinType::Inner | JoinType::LeftOuter | JoinType::RightOuter => {
                        // TODO: Support more join type.
                        let outer_table_source = ProbeSideSource::new(left_child);

                        let join_state = NestedLoopJoinState::Build;
                        Ok(Box::new(Self {
                            join_expr,
                            join_type,
                            state: join_state,
                            chunk_builder: DataChunkBuilder::new_with_default_size(
                                schema.data_types_clone(),
                            ),
                            schema,
                            last_chunk: None,
                            probe_side_schema,
                            probe_side_source: outer_table_source,
                            build_table: BuildTable::new(right_child),
                            probe_remain_chunk_idx: 0,
                            probe_remain_row_idx: 0,
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
            self.probe_side_source.init().await?;
        }
        let cur_row = self.probe_side_source.current_row()?;
        // TODO(Bowen): If we assume the scanned chunk is always compact (no invisible tuples),
        // remove this check.
        if let Some((_, vis)) = cur_row {
            // Only process visible tuple.
            if vis {
                // Dispatch to each kind of join type
                let probe_result = match self.join_type {
                    JoinType::Inner => self.do_inner_join(),
                    JoinType::LeftOuter => self.do_left_outer_join(),
                    JoinType::RightOuter => self.do_right_outer_join(),
                    _ => unimplemented!("Do not support other join types!"),
                }?;

                if probe_result.cur_row_finished {
                    self.probe_side_source.advance().await?;
                }
                if let Some(inner_chunk) = probe_result.chunk {
                    return Ok(Some(inner_chunk));
                }
            } else {
                // If not visible, proceed to next tuple.
                self.probe_side_source.advance().await?;
            }
        } else {
            self.probe_side_source.clean().await?;
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
            JoinType::RightOuter => self.do_probe_remaining(),
            _ => unimplemented!(),
        }
    }

    fn do_inner_join(&mut self) -> Result<ProbeResult> {
        while self.build_table.chunk_idx < self.build_table.inner_table.len() {
            // Checked the vis and None before.
            let probe_row = self.probe_side_source.current_row_unchecked();
            let build_side_chunk = &self.build_table.inner_table[self.build_table.chunk_idx];
            let const_row_chunk =
                self.convert_row_to_chunk(&probe_row, build_side_chunk.capacity())?;
            let new_chunk = Self::concatenate(&const_row_chunk, build_side_chunk)?;
            // Join with current row.
            let sel_vector = self.join_expr.eval(&new_chunk)?;
            let ret_chunk = new_chunk.with_visibility(sel_vector.as_bool().try_into()?);
            // Check the eval result and record some flags to prepare for outer/semi join.
            for (row_idx, vis_opt) in sel_vector.as_bool().iter().enumerate() {
                if let Some(vis) = vis_opt {
                    if vis {
                        if self.join_type.need_join_remaining() {
                            self.build_table.set_build_matched(RowId::new(
                                self.build_table.chunk_idx,
                                row_idx,
                            ))?;
                        }
                        self.probe_side_source.cur_row_matched = true;
                    }
                }
            }
            // Note: we can avoid the append chunk in join -- Only do it in the begin of outer loop.
            // But currently seems like it do not have too much gain. Will keep look on
            // it.
            if ret_chunk.capacity() > 0 {
                let (mut left_data_chunk, return_data_chunk) = self
                    .chunk_builder
                    .append_chunk(SlicedDataChunk::new_checked(ret_chunk)?)?;
                // Have checked last chunk is None in before. Now swap to buffer it.
                std::mem::swap(&mut self.last_chunk, &mut left_data_chunk);
                if let Some(inner_chunk) = return_data_chunk {
                    return Ok(ProbeResult {
                        cur_row_finished: self.build_table.chunk_idx
                            >= self.build_table.inner_table.len(),
                        chunk: Some(inner_chunk),
                    });
                }
            }
            self.build_table.chunk_idx += 1;
        }
        self.build_table.chunk_idx = 0;
        Ok(ProbeResult {
            cur_row_finished: true,
            chunk: None,
        })
    }

    fn do_left_outer_join(&mut self) -> Result<ProbeResult> {
        let ret = self.do_inner_join()?;
        // Append (probed_row, None) to chunk builder if current row finished probing and do not
        // find any match.
        if ret.cur_row_finished {
            if !self.probe_side_source.cur_row_matched {
                assert!(ret.chunk.is_none());
                let mut probe_row = self.probe_side_source.current_row_unchecked();
                for _ in 0..self.build_table.data_source.schema().fields.len() {
                    probe_row.0.push(None);
                }
                let ret = self.chunk_builder.append_one_row_ref(probe_row)?;
                return Ok(ProbeResult {
                    cur_row_finished: true,
                    chunk: ret,
                });
            }
            self.probe_side_source.cur_row_matched = false;
        }
        Ok(ret)
    }

    fn do_probe_remaining(&mut self) -> Result<Option<DataChunk>> {
        // Iterate build tables, check the build match map and append (None, build_row) if needed.
        while self.probe_remain_chunk_idx < self.build_table.inner_table.len() {
            let chunk = &self.build_table.inner_table[self.probe_remain_chunk_idx];
            while self.probe_remain_row_idx < chunk.capacity() {
                if !self.build_table.is_build_matched(RowId::new(
                    self.probe_remain_chunk_idx,
                    self.probe_remain_row_idx,
                ))? {
                    let (cur_row_ref, vis) = chunk.row_at(self.probe_remain_row_idx)?;
                    // Only proceed visible tuples.
                    if vis {
                        let mut cur_row = cur_row_ref.0;
                        for _ in 0..self.probe_side_source.outer.schema().fields.len() {
                            cur_row.insert(0, None);
                        }
                        if let Some(ret_chunk) = self
                            .chunk_builder
                            .append_one_row_ref(RowRef::new(cur_row))?
                        {
                            return Ok(Some(ret_chunk));
                        }
                    }
                }
                self.probe_remain_row_idx += 1;
            }
            self.probe_remain_row_idx = 0;
            self.probe_remain_chunk_idx += 1;
        }
        self.probe_remain_chunk_idx = 0;
        self.probe_remain_row_idx = 0;
        Ok(None)
    }

    fn do_right_outer_join(&mut self) -> Result<ProbeResult> {
        self.do_inner_join()
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
        // let vis;
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
        Ok(DataChunk::new(concated_columns, vis))
    }
}

/// This is designed for nested loop join to support row level iteration.
/// It will managed the scan state of outer table (Reset [`DataChunk`] Iter
/// when exhaust current chunk) for executor to simplify logic.
struct ProbeSideSource {
    outer: BoxedExecutor,
    cur_chunk: Option<DataChunk>,
    /// The row index to read in current chunk.
    idx: usize,
    cur_row_matched: bool,
}

impl ProbeSideSource {
    /// Note the difference between `new` and `init`. `new` do not load data but `init` will
    /// call executor. The `Done` do not really means there is no more data in outer
    /// table, it is just used for init (Otherwise we have to use Option).
    fn new(outer: BoxedExecutor) -> Self {
        Self {
            outer,
            cur_chunk: None,
            idx: 0,
            cur_row_matched: false,
        }
    }

    async fn init(&mut self) -> Result<()> {
        self.outer.open().await?;
        self.cur_chunk = self.outer.next().await?;
        Ok(())
    }

    /// Return the current outer tuple.
    fn current_row(&self) -> Result<Option<(RowRef<'_>, bool)>> {
        match &self.cur_chunk {
            Some(chunk) => Some(chunk.row_at(self.idx)).transpose(),
            None => Ok(None),
        }
    }

    /// Get current tuple directly without None check.
    fn current_row_unchecked(&self) -> RowRef<'_> {
        self.current_row().unwrap().unwrap().0
    }

    /// Try advance to next outer tuple. If it is Done but still invoked, it's
    /// a developer error.
    async fn advance(&mut self) -> Result<()> {
        self.idx += 1;
        match &self.cur_chunk {
            Some(chunk) => {
                if self.idx >= chunk.capacity() {
                    self.cur_chunk = self.outer.next().await?;
                }
            }
            None => {
                unreachable!("Should never advance while no more data to scan")
            }
        };
        Ok(())
    }

    async fn clean(&mut self) -> Result<()> {
        self.outer.close().await
    }

    fn get_schema(&self) -> &Schema {
        self.outer.schema()
    }
}

/// [`BuildTable`] contains the tuple to be probed. It is also called inner relation in join.
/// `inner_table` is a buffer for all data. For all probe key, directly fetch data in `inner_table`
/// without call executor. The executor is only called when building `inner_table`.
type InnerTable = Vec<DataChunk>;
struct BuildTable {
    data_source: BoxedExecutor,
    /// Buffering of inner table. TODO: Spill to disk or more fine-grained memory management to
    /// avoid OOM.
    inner_table: InnerTable,
    /// Pos of chunk in inner table. Tracks current probing progress.
    chunk_idx: usize,

    /// Used only when join remaining is required after probing.
    ///
    /// See [`JoinType::need_join_remaining`]
    build_matched: Option<ChunkedData<bool>>,
}

impl BuildTable {
    fn new(data_source: BoxedExecutor) -> Self {
        Self {
            data_source,
            inner_table: vec![],
            chunk_idx: 0,
            build_matched: None,
        }
    }

    /// Load all data of inner relation into buffer.
    /// Called in first probe so that do not need to invoke executor multiple roundtrip.
    /// # Arguments
    /// * `inner_source` - The source executor to load inner table. It is designed to pass as param
    ///   cuz
    /// only used in init.
    async fn load_data(&mut self) -> Result<()> {
        self.data_source.open().await?;
        while let Some(chunk) = self.data_source.next().await? {
            self.inner_table.push(chunk);
        }
        self.build_matched = Some(ChunkedData::<bool>::with_chunk_sizes(
            self.inner_table.iter().map(|c| c.capacity()),
        )?);
        self.data_source.close().await
    }

    /// Copied from hash join. Consider remove the duplication.
    fn set_build_matched(&mut self, build_row_id: RowId) -> Result<()> {
        match self.build_matched {
            Some(ref mut flags) => {
                flags[build_row_id] = true;
                Ok(())
            }
            None => Err(RwError::from(InternalError(
                "Build match flags not found!".to_string(),
            ))),
        }
    }

    fn is_build_matched(&self, build_row_id: RowId) -> Result<bool> {
        match self.build_matched {
            Some(ref flags) => Ok(flags[build_row_id]),
            None => Err(RwError::from(InternalError(
                "Build match flags not found!".to_string(),
            ))),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use crate::executor::join::nested_loop_join::{
        BuildTable, NestedLoopJoinExecutor, NestedLoopJoinState, ProbeSideSource,
    };
    use crate::executor::join::JoinType;
    use crate::executor::test_utils::MockExecutor;
    use risingwave_common::array::column::Column;
    use risingwave_common::array::*;
    use risingwave_common::catalog::{Field, Schema};
    use risingwave_common::expr::InputRefExpression;
    use risingwave_common::types::{Int32Type, ScalarRefImpl};
    use risingwave_common::util::chunk_coalesce::DataChunkBuilder;

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
            columns.push(Column::new(Arc::new(arr.into()), Int32Type::create(false)))
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
            fields: vec![Field {
                data_type: Arc::new(Int32Type::new(false)),
            }],
        };
        let probe_source = Box::new(MockExecutor::new(probe_side_schema.clone()));
        let build_source = Box::new(MockExecutor::new(probe_side_schema.clone()));
        // Note that only probe side schema of this executor is meaningful. All other fields are
        // meaningless. They are just used to pass Rust checker.
        let source = NestedLoopJoinExecutor {
            join_expr: Box::new(InputRefExpression::new(Int32Type::create(false), 0)),
            join_type: JoinType::Inner,
            state: NestedLoopJoinState::Build,
            chunk_builder: DataChunkBuilder::new_with_default_size(
                probe_side_schema.data_types_clone(),
            ),
            schema: Schema { fields: vec![] },
            last_chunk: None,
            probe_side_schema: probe_side_schema.data_types_clone(),
            probe_side_source: ProbeSideSource::new(probe_source),
            build_table: BuildTable::new(build_source),
            probe_remain_chunk_idx: 0,
            probe_remain_row_idx: 0,
        };
        let const_row_chunk = source.convert_row_to_chunk(&row, 5).unwrap();
        assert_eq!(const_row_chunk.capacity(), 5);
        assert_eq!(
            const_row_chunk.row_at(2).unwrap().0.value_at(0),
            Some(ScalarRefImpl::Int32(3))
        );
    }
}
