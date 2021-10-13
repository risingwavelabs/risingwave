use crate::array::DataChunkRef;
use crate::error::Result;
use crate::executor::hash_map::HashKey;
use crate::executor::join::hash_join::HashJoinState::{FirstProbe, Probe, ProbeRemaining};
use crate::executor::join::hash_join_state::{BuildTable, ProbeTable};
use crate::executor::join::JoinType;
use crate::executor::ExecutorResult::Batch;
use crate::executor::{BoxedExecutor, Executor, ExecutorResult};
use crate::types::DataTypeRef;
use either::Either;
use std::convert::TryInto;
use std::mem::swap;

/// Parameters of equi-join.
/// We use following sql as an example in comments:
/// ```sql
/// select a.a1, a.a2, b.b1, b.b2 from a inner join b where a.a3 = b.b3 and a.a1 = b.b1
/// ```
#[derive(Clone, Default)]
pub(super) struct EquiJoinParams {
    join_type: JoinType,
    /// Column indexes of left keys in equi join, e.g., the column indexes of `b1` and `b3` in `b`.
    left_key_columns: Vec<usize>,
    /// Data types of left keys in equi join, e.g., the column types of `b1` and `b3` in `b`.
    left_key_types: Vec<DataTypeRef>,
    /// Column indexes of right keys in equi join, e.g., the column indexes of `a1` and `a3` in `a`.
    right_key_columns: Vec<usize>,
    /// Data types of right keys in equi join, e.g., the column types of `a1` and `a3` in `a`.
    right_key_types: Vec<DataTypeRef>,
    /// Column indexes of outputs in equi join, e.g. the column indexes of `a1`, `a2`, `b1`, `b2`.
    /// [`Either::Left`] is used to mark left side input, and [`Either::Right`] is used to mark right
    /// side input.
    output_columns: Vec<Either<usize, usize>>,
    /// Column types of outputs in equi join, e.g. the column types of `a1`, `a2`, `b1`, `b2`.
    output_data_types: Vec<DataTypeRef>,
}

/// Different states when executing a hash join.
enum HashJoinState<K> {
    /// Initial state of hash join.
    /// In this state, the executor [`Executor::init`] build side input, and calls [`Executor::next`]
    /// of build side input till [`ExecutorResult::Done`] is returned to create `BuildTable`.
    Build,
    /// First state after finishing build state.
    /// It's different from [`Probe`] in that we need to [`Executor::init`] probe side input.
    FirstProbe(ProbeTable<K>),
    /// State for executing join.
    /// In this state, the executor calls [`Executor::init`]  method of probe side input, and executes
    /// joining with the chunk against build table to create output.
    Probe(ProbeTable<K>),
    /// Optional state after [`Probe`].
    /// This state is only required for join types which may produce outputs from build side, for
    /// example, [`JoinType::RightOuter`].
    ///
    /// See [`JoinType::need_join_remaining()`]
    ProbeRemaining(ProbeTable<K>),
    /// Final state of hash join.
    Done,
}

pub(super) struct HashJoinExecutor<K> {
    /// Probe side
    left_child: BoxedExecutor,
    /// Build side
    right_child: BoxedExecutor,
    params: EquiJoinParams,
    state: HashJoinState<K>,
}

impl EquiJoinParams {
    #[inline(always)]
    pub(super) fn probe_key_columns(&self) -> &[usize] {
        &self.right_key_columns
    }

    #[inline(always)]
    pub(super) fn join_type(&self) -> JoinType {
        self.join_type
    }

    #[inline(always)]
    pub(super) fn output_types(&self) -> &[DataTypeRef] {
        &self.output_data_types
    }

    #[inline(always)]
    pub(super) fn output_columns(&self) -> &[Either<usize, usize>] {
        &self.output_columns
    }
}

impl<K: HashKey + Send + Sync> Executor for HashJoinExecutor<K> {
    fn init(&mut self) -> Result<()> {
        Ok(())
    }

    fn execute(&mut self) -> Result<ExecutorResult> {
        loop {
            let mut cur_state = HashJoinState::Done;
            swap(&mut cur_state, &mut self.state);

            match cur_state {
                HashJoinState::Build => self.build()?,
                HashJoinState::FirstProbe(probe_table) => {
                    let ret = self.probe(true, probe_table)?;
                    if let Some(data_chunk) = ret {
                        return Ok(ExecutorResult::Batch(data_chunk));
                    }
                }
                HashJoinState::Probe(probe_table) => {
                    let ret = self.probe(false, probe_table)?;
                    if let Some(data_chunk) = ret {
                        return Ok(ExecutorResult::Batch(data_chunk));
                    }
                }
                HashJoinState::ProbeRemaining(probe_table) => {
                    let ret = self.probe_remaining(probe_table)?;
                    if let Some(data_chunk) = ret {
                        return Ok(ExecutorResult::Batch(data_chunk));
                    }
                }
                HashJoinState::Done => return Ok(ExecutorResult::Done),
            }
        }
    }

    fn clean(&mut self) -> Result<()> {
        self.left_child.clean()?;
        self.right_child.clean()?;
        Ok(())
    }
}

impl<K: HashKey> HashJoinExecutor<K> {
    fn build(&mut self) -> Result<()> {
        self.right_child.init()?;
        let mut build_table = BuildTable::default();
        while let Batch(chunk) = self.right_child.execute()? {
            build_table.append_build_chunk(chunk)?;
        }

        let probe_table = build_table.try_into()?;

        self.state = FirstProbe(probe_table);
        Ok(())
    }

    fn probe(
        &mut self,
        first_probe: bool,
        mut probe_table: ProbeTable<K>,
    ) -> Result<Option<DataChunkRef>> {
        if first_probe {
            self.left_child.init()?;
        }

        match self.left_child.execute()? {
            ExecutorResult::Batch(data_chunk) => {
                let ret = probe_table.join(data_chunk).map(Some);
                self.state = Probe(probe_table);
                ret
            }
            ExecutorResult::Done => {
                if self.params.join_type.need_join_remaining() {
                    // switch state
                    self.state = ProbeRemaining(probe_table);
                } else {
                    self.state = HashJoinState::Done;
                }

                Ok(None)
            }
        }
    }

    fn probe_remaining(&mut self, mut probe_table: ProbeTable<K>) -> Result<Option<DataChunkRef>> {
        let ret = probe_table.join_remaining();
        self.state = HashJoinState::Done;
        ret
    }
}
