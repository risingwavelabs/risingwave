use crate::array::column::Column;
use crate::array::data_chunk_iter::RowRef;
use crate::array::DataChunk;
use crate::error::ErrorCode::{InternalError, ProtobufError};
use crate::error::{Result, RwError};
use crate::executor::join::JoinType;
use crate::executor::ExecutorResult::{Batch, Done};
use crate::executor::{
    BoxedExecutor, BoxedExecutorBuilder, Executor, ExecutorBuilder, ExecutorResult, Schema,
};
use crate::expr::{build_from_proto, BoxedExpression};
use crate::types::DataType;
use protobuf::Message;
use risingwave_proto::plan::{NestedLoopJoinNode, PlanNode_PlanNodeType};
use std::mem::swap;
use std::sync::Arc;

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
}

type ProbeResult = Option<DataChunk>;
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
    fn init(&mut self) -> crate::error::Result<()> {
        Ok(())
    }

    async fn execute(&mut self) -> crate::error::Result<ExecutorResult> {
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
    fn clean(&mut self) -> crate::error::Result<()> {
        self.probe_side_source.clean()
    }

    fn schema(&self) -> &Schema {
        &self.schema
    }
}

impl BoxedExecutorBuilder for NestedLoopJoinExecutor {
    fn new_boxed_executor(source: &ExecutorBuilder) -> Result<BoxedExecutor> {
        ensure!(source.plan_node().get_node_type() == PlanNode_PlanNodeType::NESTED_LOOP_JOIN);
        ensure!(source.plan_node().get_children().len() == 2);

        let nested_loop_join_node =
            NestedLoopJoinNode::parse_from_bytes(source.plan_node().get_body().get_value())
                .map_err(|e| RwError::from(ProtobufError(e)))?;

        let join_type = JoinType::from_proto(nested_loop_join_node.get_join_type());
        let join_expr = build_from_proto(nested_loop_join_node.get_join_cond())?;
        let left_plan_opt = source.plan_node().get_children().get(0);
        let right_plan_opt = source.plan_node().get_children().get(1);
        match (left_plan_opt, right_plan_opt) {
            (Some(left_plan), Some(right_plan)) => {
                let left_child =
                    ExecutorBuilder::new(left_plan, source.global_task_env().clone()).build()?;
                let right_child =
                    ExecutorBuilder::new(right_plan, source.global_task_env().clone()).build()?;

                let fields = vec![];
                // TODO: fix this when exchange's schema is ready
                // let fields = left_child
                //   .schema()
                //   .fields
                //   .iter()
                //   .chain(right_child.schema().fields.iter())
                //   .map(|f| Field {
                //     data_type: f.data_type.clone(),
                //   })
                //   .collect();
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
                            schema: Schema { fields },
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
        match cur_row {
            Some((row, vis)) => {
                // Only process visible tuple.
                if vis {
                    let probe_result =
                        build_table.join(row, &self.probe_side_source, &mut self.join_expr)?;
                    self.state = NestedLoopJoinState::Probe(build_table);
                    if let Some(data_chunk) = probe_result {
                        return Ok(Some(data_chunk));
                    } else {
                        self.probe_side_source.advance().await?;
                    }
                } else {
                    // If not visible, proceed to next tuple.
                    self.probe_side_source.advance().await?;
                }
            }
            // No more outer tuple means job finished.
            None => self.state = NestedLoopJoinState::Done,
        };

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
        match &self.cur_chunk {
            Batch(chunk) => {
                let num_columns = chunk.columns().len();
                assert_eq!(row_ref.size(), num_columns);
                let mut array_builders = Vec::with_capacity(num_columns);
                let mut data_types = Vec::with_capacity(num_columns);
                // Create array builders and data types.
                for column in chunk.columns() {
                    array_builders.push(DataType::create_array_builder(
                        column.data_type().clone(),
                        num_tuples,
                    )?);
                    data_types.push(column.data_type().clone());
                }

                // Append scalar to these builders.
                for _i in 0..num_tuples {
                    for (col_idx, builder) in
                        array_builders.iter_mut().enumerate().take(num_columns)
                    {
                        builder.append_datum_ref(row_ref.value_at(col_idx))?;
                    }
                }

                // Finish each array builder and get Column.
                let result_columns = array_builders
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
            Done => unreachable!("Should never convert row to chunk while no more data to scan"),
        }
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

    /// Find all joined results from `row`.
    /// Note that we may need to `join` multiple times for the same single row.
    /// If [`ProbeResult`] is None, All Join results for current `row` has been return and
    /// executor should advance to next outer row.
    /// Note: It do have to be a Option if the joined result is always batched,
    /// for example 1024, executor can check the length of output result and knows whether probe has ended for current row.
    /// But for now, the return [`ProbeResult`] is not materialized (may contains lots of invisible tuples).
    /// In future consider return a results set (all tuples are materialized).
    /// # Arguments
    /// * `row` - Tuple to be be probed.
    /// * `outer_source` - Help `row` convert into a chunk.
    fn join(
        &mut self,
        row: RowRef<'_>,
        outer_source: &ProbeSideSource,
        join_expr: &mut BoxedExpression,
    ) -> Result<ProbeResult> {
        // Developer error if occur.
        assert!(self.chunk_idx <= self.inner_table.len());
        if self.chunk_idx < self.inner_table.len() {
            let chunk = &self.inner_table[self.chunk_idx];
            let constant_row_chunk = outer_source.convert_row_to_chunk(&row, chunk.capacity())?;
            // Concatenate two chunk into a new one first.
            let new_chunk = Self::concatenate(&constant_row_chunk, chunk)?;
            let sel_vector = join_expr.eval(&new_chunk)?;
            // Materialize the joined chunk result.
            let joined_chunk = new_chunk
                .with_visibility(sel_vector.as_bool().try_into()?)
                .compact()?;
            self.chunk_idx += 1;
            Ok(Some(joined_chunk))
        } else {
            self.chunk_idx = 0;
            Ok(None)
        }
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
}

#[cfg(test)]
mod tests {

    use crate::array::column::Column;
    use crate::array::data_chunk_iter::RowRef;
    use crate::array::*;

    use crate::catalog::test_utils::mock_table_id;
    use crate::executor::join::nested_loop_join::{BuildTable, ProbeSideSource};
    use crate::executor::seq_scan::SeqScanExecutor;
    use crate::executor::{ExecutorResult, Schema};
    use crate::storage::SimpleMemTable;
    use crate::types::Int32Type;
    use crate::types::ScalarRefImpl;
    use std::sync::Arc;

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
        let schema = Schema { fields: vec![] };
        let seq_scan_exec = SeqScanExecutor::new(
            Arc::new(SimpleMemTable::new(&mock_table_id(), &columns)),
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
