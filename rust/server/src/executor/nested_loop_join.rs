use crate::array2::data_chunk_iter::RowRef;
use crate::array2::{DataChunk, DataChunkRef};
use crate::buffer::Bitmap;
use crate::error::Result;
use crate::executor::join::JoinType;
use crate::executor::ExecutorResult::{Batch, Done};
use crate::executor::{BoxedExecutor, Executor, ExecutorResult};
use crate::expr::BoxedExpression;
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
struct NestedLoopJoinExecutor {
    /// Executor should handle different join type.
    join_type: JoinType,
    /// Outer relation data source (return in tuple level).
    outer_source: OuterTableSource,
    /// Inner relation data source (Used for building probe table).
    inner_source: BoxedExecutor,
    /// Manage inner source and expression evaluation.
    state: NestedLoopJoinState,
}

type ProbeResult = Option<DataChunkRef>;
enum NestedLoopJoinState {
    FirstProbe(ProbeTable),
    Probe(ProbeTable),
    ProbeRemaining(ProbeTable),
    Done,
}

impl Executor for NestedLoopJoinExecutor {
    fn init(&mut self) -> crate::error::Result<()> {
        self.outer_source.init()
    }

    fn execute(&mut self) -> crate::error::Result<ExecutorResult> {
        loop {
            let mut cur_state = NestedLoopJoinState::Done;
            swap(&mut cur_state, &mut self.state);

            match cur_state {
                NestedLoopJoinState::FirstProbe(probe_table) => {
                    let ret = self.probe(true, probe_table)?;
                    if let Some(data_chunk) = ret {
                        return Ok(ExecutorResult::Batch(data_chunk));
                    }
                }
                NestedLoopJoinState::Probe(probe_table) => {
                    let ret = self.probe(false, probe_table)?;
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
        self.outer_source.clean()
    }
}

impl NestedLoopJoinExecutor {
    /// Probe matched rows in `probe_table`.
    /// # Arguments
    /// * `first_probe`: whether the first probing. Build probe table if yes.
    /// * `probe_table`: Table to be probed.
    /// If nothing gained from `probe_table.join()`, Advance to next row until all outer tuples are exhausted.
    fn probe(
        &mut self,
        first_probe: bool,
        mut probe_table: ProbeTable,
    ) -> Result<Option<DataChunkRef>> {
        if first_probe {
            probe_table.init(&mut self.inner_source)?;
        }

        let cur_row = self.outer_source.current_row()?;
        match cur_row {
            Some((row, vis)) => {
                // Only process visible tuple.
                if vis {
                    let ret = probe_table.join(row, &self.outer_source)?;
                    if let Some(data_chunk) = ret {
                        return Ok(Some(data_chunk));
                    } else {
                        self.outer_source.advance()?;
                    }
                } else {
                    // If not visible, proceed to next tuple.
                    self.outer_source.advance()?;
                }
            }
            // No more outer tuple means job finished.
            None => self.state = NestedLoopJoinState::Done,
        };

        Ok(None)
    }

    /// Similar to [`probe_remaining`] in [`HashJoin`]. It should be done when join operator has a row id matched
    /// table and wants to support RIGHT OUTER/ RIGHT SEMI/ FULL OUTER.
    fn probe_remaining(&mut self) -> Result<Option<DataChunkRef>> {
        todo!()
    }
}

/// This is designed for nested loop join to support row level iteration.
/// It will managed the scan state of outer table (Reset [`DataChunk`] Iter
/// when exhaust current chunk) for executor to simplify logic.
struct OuterTableSource {
    outer: BoxedExecutor,
    cur_chunk: ExecutorResult,
    /// The row index to read in current chunk.
    idx: usize,
}

impl OuterTableSource {
    fn init(&mut self) -> Result<()> {
        self.outer.init()?;
        self.cur_chunk = self.outer.execute()?;
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
    fn as_chunk(&self, _row_ref: &RowRef<'_>, _capacity: usize) -> Result<DataChunk> {
        todo!("convert row into chunk is not implemented yet!")
    }

    /// Try advance to next outer tuple. If it is Done but still invoked, it's
    /// a developer error.
    fn advance(&mut self) -> Result<()> {
        self.idx += 1;
        match &self.cur_chunk {
            Batch(chunk) => {
                if self.idx >= chunk.capacity() {
                    self.cur_chunk = self.outer.execute()?;
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

/// [`ProbeTable`] contains the tuple to be probed. It is also called inner relation in join.
/// `inner_table` is a buffer for all data. For all probe key, directly fetch data in `inner_table` without call executor.
/// The executor is only called when building `inner_table`.
type InnerTable = Vec<DataChunkRef>;
struct ProbeTable {
    /// Eval joined result by reusing Expression.
    join_cond: BoxedExpression,
    /// Buffering of inner table. TODO: Spill to disk or more fine-grained memory management to avoid OOM.
    inner_table: InnerTable,
    /// Pos of chunk in inner table. Tracks current probing progress.
    chunk_idx: usize,
}

impl ProbeTable {
    /// Load all data of inner relation into buffer.
    /// Called in first probe so that do not need to invoke executor multiple roundtrip.
    /// # Arguments
    /// * `inner_source` - The source executor to load inner table. It is designed to pass as param cuz
    /// only used in init.
    fn init(&mut self, inner_source: &mut BoxedExecutor) -> Result<()> {
        inner_source.init()?;
        while let Batch(chunk) = inner_source.execute()? {
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
    fn join(&mut self, row: RowRef<'_>, outer_source: &OuterTableSource) -> Result<ProbeResult> {
        // Developer error if occur.
        assert!(self.chunk_idx <= self.inner_table.len());
        if self.chunk_idx < self.inner_table.len() {
            let chunk = self.inner_table[self.chunk_idx].clone();
            // Concatenate two chunk into a new one first.
            let constant_row_chunk = outer_source.as_chunk(&row, chunk.capacity())?;
            let new_chunk = Self::concatenate(&constant_row_chunk, &*chunk)?;
            let sel_vector = self.join_cond.eval(&new_chunk)?;
            // Note that do not materialize results here. Just update the visibility.
            let joined_chunk =
                new_chunk.with_visibility(Bitmap::from_bool_array(sel_vector.as_bool())?);
            self.chunk_idx += 1;
            Ok(Some(Arc::new(joined_chunk)))
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
    /// If Two normal chunk, the result is undefined.
    fn concatenate(_left: &DataChunk, _right: &DataChunk) -> Result<DataChunk> {
        todo!()
    }
}
