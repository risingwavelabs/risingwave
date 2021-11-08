use std::mem::swap;
use std::sync::Arc;

use prost::Message;

use crate::executor::join::JoinType;
use crate::executor::ExecutorResult::{Batch, Done};
use crate::executor::{
    BoxedExecutor, BoxedExecutorBuilder, Executor, ExecutorBuilder, ExecutorResult,
};
use risingwave_common::array::column::Column;
use risingwave_common::array::data_chunk_iter::RowRef;
use risingwave_common::array::{ArrayBuilderImpl, DataChunk};
use risingwave_common::catalog::{Field, Schema};
use risingwave_common::error::ErrorCode::InternalError;
use risingwave_common::error::{ErrorCode, Result, RwError};
use risingwave_common::expr::build_from_prost as expr_build_from_prost;
use risingwave_common::expr::BoxedExpression;
use risingwave_common::types::DataType;
use risingwave_common::util::chunk_coalesce::{DataChunkBuilder, SlicedDataChunk};
use risingwave_pb::plan::{plan_node::PlanNodeType, NestedLoopJoinNode};

/// Nested loop join executor.
///
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
    /// Outer relation data source (return in tuple level).
    probe_side_source: ProbeSideSource,
    /// Inner relation data source (Used for building build table).
    build_side_source: BoxedExecutor,
    /// Manage inner source and expression evaluation.
    state: NestedLoopJoinState,
    schema: Schema,
    /// Return data chunk in batch.
    chunk_builder: DataChunkBuilder,
    /// Cache the chunk that has not been append during join probing.
    /// Flush it in begin of execution.
    last_chunk: Option<SlicedDataChunk>,
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
    FirstProbe(BuildTable),
    /// [`Probe`] finds matching rows for all outer table.
    Probe(BuildTable),
    ProbeRemaining(BuildTable),
    Done,
}

#[async_trait::async_trait]
impl Executor for NestedLoopJoinExecutor {
    fn init(&mut self) -> risingwave_common::error::Result<()> {
        Ok(())
    }

    async fn execute(&mut self) -> risingwave_common::error::Result<ExecutorResult> {
        if let Some(last_chunk) = self.last_chunk.take() {
            let (left_data_chunk, return_chunk) = self.chunk_builder.append_chunk(last_chunk)?;
            self.last_chunk = left_data_chunk;
            if let Some(return_chunk_inner) = return_chunk {
                return Ok(Batch(return_chunk_inner));
            }
        }
        // Make sure all buffered chunk are flushed before generate more chunks.
        // It's impossible that: left chunk not null and return chunk is null.
        assert!(self.last_chunk.is_none());

        loop {
            let mut cur_state = NestedLoopJoinState::Done;
            swap(&mut cur_state, &mut self.state);

            match cur_state {
                NestedLoopJoinState::Build => {
                    let mut build_table = BuildTable::new();
                    build_table.init(&mut self.build_side_source).await?;
                    self.state = NestedLoopJoinState::FirstProbe(build_table);
                }
                NestedLoopJoinState::FirstProbe(build_table) => {
                    let ret = self.probe(true, build_table).await?;
                    if let Some(data_chunk) = ret {
                        return Ok(ExecutorResult::Batch(data_chunk));
                    }
                }
                NestedLoopJoinState::Probe(build_table) => {
                    let ret = self.probe(false, build_table).await?;
                    if let Some(data_chunk) = ret {
                        return Ok(ExecutorResult::Batch(data_chunk));
                    }
                }

                NestedLoopJoinState::ProbeRemaining(_) => {
                    unimplemented!("Probe remaining is not support for nested loop join yet")
                }

                NestedLoopJoinState::Done => return Ok(ExecutorResult::Done),
            }
        }
    }
    fn clean(&mut self) -> risingwave_common::error::Result<()> {
        self.probe_side_source.clean()
    }

    fn schema(&self) -> &Schema {
        &self.schema
    }
}

impl BoxedExecutorBuilder for NestedLoopJoinExecutor {
    fn new_boxed_executor(source: &ExecutorBuilder) -> Result<BoxedExecutor> {
        ensure!(source.plan_node().get_node_type() as i32 == PlanNodeType::NestedLoopJoin as i32);
        ensure!(source.plan_node().get_children().len() == 2);

        let nested_loop_join_node =
            NestedLoopJoinNode::decode(&(source.plan_node()).get_body().value[..])
                .map_err(|e| RwError::from(ErrorCode::ProstError(e)))?;

        let join_type = JoinType::from_prost(nested_loop_join_node.get_join_type());
        let join_expr = expr_build_from_prost(nested_loop_join_node.get_join_cond())?;
        // Note: Assume that optimizer has set up probe side and build side. Left is the probe side, right is
        // the build side. This is the same for all join executors.
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
                match join_type {
                    JoinType::Inner => {
                        // TODO: Support more join type.
                        let outer_table_source = ProbeSideSource::new(left_child);

                        let join_state = NestedLoopJoinState::Build;

                        Ok(Box::new(Self {
                            join_expr,
                            join_type,
                            probe_side_source: outer_table_source,
                            build_side_source: right_child,
                            state: join_state,
                            chunk_builder: DataChunkBuilder::new_with_default_size(
                                schema
                                    .fields
                                    .iter()
                                    .map(|field| field.data_type.clone())
                                    .collect(),
                            ),
                            schema,
                            last_chunk: None,
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
    /// If nothing gained from `probe_table.join()`, Advance to next row until all outer tuples are exhausted.
    async fn probe(
        &mut self,
        first_probe: bool,
        mut build_table: BuildTable,
    ) -> Result<Option<DataChunk>> {
        if first_probe {
            self.probe_side_source.init().await?;
        }
        let cur_row = self.probe_side_source.current_row()?;
        if let Some((row, vis)) = cur_row {
            // Only process visible tuple.
            if vis {
                // Dispatch to each kind of join type
                let probe_result = match self.join_type {
                    JoinType::Inner => build_table.do_inner_join(
                        row,
                        &mut self.chunk_builder,
                        &mut self.join_expr,
                        &self.probe_side_source,
                        &mut self.last_chunk,
                    ),
                    _ => unimplemented!("Do not support other join types!"),
                }?;

                // Update the state after join.
                self.state = NestedLoopJoinState::Probe(build_table);
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
            self.state = NestedLoopJoinState::Done;
            return self.chunk_builder.consume_all();
        }
        Ok(None)
    }

    /// Similar to [`probe_remaining`] in [`HashJoin`]. It should be done when join operator has a row id matched
    /// table and wants to support RIGHT OUTER/ RIGHT SEMI/ FULL OUTER.
    fn probe_remaining(&mut self) -> Result<Option<DataChunk>> {
        todo!()
    }
}

/// This is designed for nested loop join to support row level iteration.
/// It will managed the scan state of outer table (Reset [`DataChunk`] Iter
/// when exhaust current chunk) for executor to simplify logic.
struct ProbeSideSource {
    outer: BoxedExecutor,
    cur_chunk: ExecutorResult,
    /// The row index to read in current chunk.
    idx: usize,
}

impl ProbeSideSource {
    /// Note the difference between `new` and `init`. `new` do not load data but `init` will
    /// call executor. The `ExecutorResult::Done` do not really means there is no more data in outer
    /// table, it is just used for init (Otherwise we have to use Option).
    fn new(outer: BoxedExecutor) -> Self {
        Self {
            outer,
            cur_chunk: ExecutorResult::Done,
            idx: 0,
        }
    }

    async fn init(&mut self) -> Result<()> {
        self.outer.init()?;
        self.cur_chunk = self.outer.execute().await?;
        Ok(())
    }

    /// Return the current outer tuple.
    fn current_row(&self) -> Result<Option<(RowRef<'_>, bool)>> {
        match &self.cur_chunk {
            Batch(chunk) => Some(chunk.row_at(self.idx)).transpose(),
            Done => Ok(None),
        }
    }

    /// Create constant data chunk (one tuple repeat `capacity` times).
    fn convert_row_to_chunk(&self, row_ref: &RowRef<'_>, num_tuples: usize) -> Result<DataChunk> {
        let data_types = self.outer.schema().data_types_clone();
        let num_columns = data_types.len();
        let mut output_array_builders = data_types
            .iter()
            .map(|data_type| DataType::create_array_builder(data_type.clone(), num_tuples))
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

    /// Try advance to next outer tuple. If it is Done but still invoked, it's
    /// a developer error.
    async fn advance(&mut self) -> Result<()> {
        self.idx += 1;
        match &self.cur_chunk {
            Batch(chunk) => {
                if self.idx >= chunk.capacity() {
                    self.cur_chunk = self.outer.execute().await?;
                }
            }
            Done => {
                unreachable!("Should never advance while no more data to scan")
            }
        };
        Ok(())
    }

    fn clean(&mut self) -> Result<()> {
        self.outer.clean()
    }

    fn get_schema(&self) -> &Schema {
        self.outer.schema()
    }
}

/// [`BuildTable`] contains the tuple to be probed. It is also called inner relation in join.
/// `inner_table` is a buffer for all data. For all probe key, directly fetch data in `inner_table` without call executor.
/// The executor is only called when building `inner_table`.
type InnerTable = Vec<DataChunk>;
struct BuildTable {
    /// Buffering of inner table. TODO: Spill to disk or more fine-grained memory management to avoid OOM.
    inner_table: InnerTable,
    /// Pos of chunk in inner table. Tracks current probing progress.
    chunk_idx: usize,
}

impl BuildTable {
    fn new() -> Self {
        Self {
            inner_table: vec![],
            chunk_idx: 0,
        }
    }

    /// Load all data of inner relation into buffer.
    /// Called in first probe so that do not need to invoke executor multiple roundtrip.
    /// # Arguments
    /// * `inner_source` - The source executor to load inner table. It is designed to pass as param cuz
    /// only used in init.
    async fn init(&mut self, inner_source: &mut BoxedExecutor) -> Result<()> {
        inner_source.init()?;
        while let Batch(chunk) = inner_source.execute().await? {
            self.inner_table.push(chunk);
        }
        inner_source.clean()
    }

    /// The layout be like:
    /// [ `left` chunk     |  `right` chunk     ]
    /// # Arguments
    /// * `left` Data chunk padded to the right half of result data chunk..
    /// * `right` Data chunk padded to the right half of result data chunk.
    /// Note: Use this function with careful: It is not designed to be a general concatenate of two chunk:
    /// Usually one side should be const row chunk and the other side is normal chunk. Currently only feasible to use in join
    /// executor.
    /// If two normal chunk, the result is undefined.
    fn concatenate(left: &DataChunk, right: &DataChunk) -> Result<DataChunk> {
        assert_eq!(left.capacity(), right.capacity());
        let mut concated_columns = Vec::with_capacity(left.columns().len() + right.columns().len());
        concated_columns.extend_from_slice(left.columns());
        concated_columns.extend_from_slice(right.columns());
        let vis;
        // Only handle one side is constant row chunk: One of visibility must be None.
        match (left.visibility(), right.visibility()) {
            (None, _) => {
                vis = right.visibility().clone();
            }
            (_, None) => {
                vis = left.visibility().clone();
            }
            (Some(_), Some(_)) => {
                unimplemented!(
                    "The concatenate behaviour of two chunk with visibility is undefined"
                )
            }
        }
        Ok(DataChunk::new(concated_columns, vis))
    }

    fn do_inner_join(
        &mut self,
        probe_row: RowRef<'_>,
        chunk_builder: &mut DataChunkBuilder,
        join_expr: &mut BoxedExpression,
        probe_side_source: &ProbeSideSource,
        last_chunk: &mut Option<SlicedDataChunk>,
    ) -> Result<ProbeResult> {
        while self.chunk_idx < self.inner_table.len() {
            let build_side_chunk = &self.inner_table[self.chunk_idx];
            let const_row_chunk =
                probe_side_source.convert_row_to_chunk(&probe_row, build_side_chunk.capacity())?;
            let new_chunk = Self::concatenate(&const_row_chunk, build_side_chunk)?;
            // join with current row
            let sel_vector = join_expr.eval(&new_chunk)?;
            let ret_chunk = new_chunk.with_visibility(sel_vector.as_bool().try_into()?);
            // Note: we can avoid the append chunk in join -- Only do it in the begin of outer loop. But currently seems like
            // it do not have too much gain. Will keep look on it.
            if ret_chunk.capacity() > 0 {
                let (mut left_data_chunk, return_data_chunk) =
                    chunk_builder.append_chunk(SlicedDataChunk::new_checked(ret_chunk)?)?;
                //Have checked last_input is None in before. Now swap the chunk into left chunk.
                std::mem::swap(last_chunk, &mut left_data_chunk);
                if let Some(inner_chunk) = return_data_chunk {
                    return Ok(ProbeResult {
                        cur_row_finished: self.chunk_idx >= self.inner_table.len(),
                        chunk: Some(inner_chunk),
                    });
                }
            }
            self.chunk_idx += 1;
        }
        self.chunk_idx = 0;
        Ok(ProbeResult {
            cur_row_finished: true,
            chunk: None,
        })
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use crate::executor::join::nested_loop_join::{BuildTable, ProbeSideSource};
    use crate::executor::seq_scan::SeqScanExecutor;
    use crate::executor::ExecutorResult;
    use crate::risingwave_common::catalog::Field;
    use crate::storage::SimpleMemTable;
    use risingwave_common::array::column::Column;
    use risingwave_common::array::data_chunk_iter::RowRef;
    use risingwave_common::array::*;
    use risingwave_common::catalog::test_utils::mock_table_id;
    use risingwave_common::catalog::Schema;
    use risingwave_common::types::Int32Type;
    use risingwave_common::types::ScalarRefImpl;

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
        let chunk = BuildTable::concatenate(&chunk1, &chunk2).unwrap();
        assert_eq!(chunk.capacity(), chunk1.capacity());
        assert_eq!(chunk.capacity(), chunk2.capacity());
        assert_eq!(chunk.columns().len(), chunk1.columns().len() * 2);
        assert_eq!(
            chunk.visibility().clone().unwrap(),
            (bool_vec).try_into().unwrap()
        );
    }

    #[test]
    fn test_convert_row_to_chunk() {
        let row = RowRef::new(vec![Some(ScalarRefImpl::Int32(3))]);
        let columns = vec![];
        let schema = Schema {
            fields: vec![Field {
                data_type: Arc::new(Int32Type::new(false)),
            }],
        };
        let seq_scan_exec = SeqScanExecutor::new(
            true,
            Arc::new(SimpleMemTable::new(&mock_table_id(), &columns)),
            vec![],
            vec![],
            vec![],
            0,
            schema,
        );
        let chunk = DataChunk::new(
            vec![Column::new(
                Arc::new(I32ArrayBuilder::new(1024).unwrap().finish().unwrap().into()),
                Arc::new(Int32Type::new(true)),
            )],
            None,
        );
        let source = ProbeSideSource {
            outer: Box::new(seq_scan_exec),
            cur_chunk: ExecutorResult::Batch(chunk),
            idx: 0,
        };
        let const_row_chunk = source.convert_row_to_chunk(&row, 5).unwrap();
        assert_eq!(const_row_chunk.capacity(), 5);
        assert_eq!(
            const_row_chunk.row_at(2).unwrap().0.value_at(0),
            Some(ScalarRefImpl::Int32(3))
        );
    }
}
