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

use std::fmt::Debug;

use itertools::Itertools;
use risingwave_common::array::{ArrayBuilderImpl, Op};
use risingwave_common::types::Datum;
use risingwave_storage::table::streaming_table::state_table::StateTable;
use risingwave_storage::StateStore;

use crate::executor::error::StreamExecutorResult;
use crate::executor::managed_state::aggregation::ManagedStateImpl;

/// States for [`crate::executor::LocalSimpleAggExecutor`],
/// [`crate::executor::GlobalSimpleAggExecutor`] and [`crate::executor::HashAggExecutor`].
pub struct AggState<S: StateStore> {
    /// Current managed states for all [`crate::executor::aggregation::AggCall`]s.
    pub managed_states: Vec<ManagedStateImpl<S>>,

    /// Previous outputs of managed states. Initializing with `None`.
    pub prev_states: Option<Vec<Datum>>,
}

impl<S: StateStore> Debug for AggState<S> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AggState")
            .field("prev_states", &self.prev_states)
            .finish()
    }
}

/// We assume the first state of aggregation is always `StreamingRowCountAgg`.
pub const ROW_COUNT_COLUMN: usize = 0;

impl<S: StateStore> AggState<S> {
    pub async fn row_count(
        &mut self,
        epoch: u64,
        state_table: &StateTable<S>,
    ) -> StreamExecutorResult<i64> {
        Ok(self.managed_states[ROW_COUNT_COLUMN]
            .get_output(epoch, state_table)
            .await?
            .map(|x| *x.as_int64())
            .unwrap_or(0))
    }

    pub fn prev_row_count(&self) -> i64 {
        match &self.prev_states {
            Some(states) => states[ROW_COUNT_COLUMN]
                .as_ref()
                .map(|x| *x.as_int64())
                .unwrap_or(0),
            None => 0,
        }
    }

    /// Returns whether `prev_states` is filled.
    pub fn is_dirty(&self) -> bool {
        self.prev_states.is_some()
    }

    /// Used for recording the output of current states as previous states, before applying new
    /// changes to the state. If the state is already marked dirty in this epoch, this function does
    /// no-op.
    /// After calling this function, `self.is_dirty()` will return `true`.
    pub async fn may_mark_as_dirty(
        &mut self,
        epoch: u64,
        state_tables: &[StateTable<S>],
    ) -> StreamExecutorResult<()> {
        if self.is_dirty() {
            return Ok(());
        }

        let mut outputs = vec![];
        for (state, state_table) in self.managed_states.iter_mut().zip_eq(state_tables.iter()) {
            outputs.push(state.get_output(epoch, state_table).await?);
        }
        self.prev_states = Some(outputs);
        Ok(())
    }

    /// Build changes into `builders` and `new_ops`, according to previous and current states. Note
    /// that for [`crate::executor::HashAggExecutor`].
    ///
    /// Returns how many rows are appended in builders.
    pub async fn build_changes(
        &mut self,
        builders: &mut [ArrayBuilderImpl],
        new_ops: &mut Vec<Op>,
        epoch: u64,
        state_tables: &[StateTable<S>],
    ) -> StreamExecutorResult<usize> {
        if !self.is_dirty() {
            return Ok(0);
        }

        let row_count = self
            .row_count(epoch, &state_tables[ROW_COUNT_COLUMN])
            .await?;
        let prev_row_count = self.prev_row_count();

        trace!(
            "prev_row_count = {}, row_count = {}",
            prev_row_count,
            row_count
        );

        let appended = match (prev_row_count, row_count) {
            (0, 0) => {
                // previous state is empty, current state is also empty.
                // FIXME: for `SimpleAgg`, should we still build some changes when `row_count` is 0
                // while other aggs may not be `0`?

                0
            }

            (0, _) => {
                // previous state is empty, current state is not empty, insert one `Insert` op.
                new_ops.push(Op::Insert);

                for ((builder, state), state_table) in builders
                    .iter_mut()
                    .zip_eq(self.managed_states.iter_mut())
                    .zip_eq(state_tables.iter())
                {
                    let data = state.get_output(epoch, state_table).await?;
                    trace!("append_datum (0 -> N): {:?}", &data);
                    builder.append_datum(&data);
                }

                1
            }

            (_, 0) => {
                // previous state is not empty, current state is empty, insert one `Delete` op.
                new_ops.push(Op::Delete);

                for (builder, state) in builders
                    .iter_mut()
                    .zip_eq(self.prev_states.as_ref().unwrap().iter())
                {
                    trace!("append_datum (N -> 0): {:?}", &state);
                    builder.append_datum(state);
                }

                1
            }

            _ => {
                // previous state is not empty, current state is not empty, insert two `Update` op.
                new_ops.push(Op::UpdateDelete);
                new_ops.push(Op::UpdateInsert);

                for (builder, prev_state, cur_state, state_table) in itertools::multizip((
                    builders.iter_mut(),
                    self.prev_states.as_ref().unwrap().iter(),
                    self.managed_states.iter_mut(),
                    state_tables.iter(),
                )) {
                    let cur_state = cur_state.get_output(epoch, state_table).await?;
                    trace!(
                        "append_datum (N -> N): prev = {:?}, cur = {:?}",
                        prev_state,
                        &cur_state
                    );

                    builder.append_datum(prev_state);
                    builder.append_datum(&cur_state);
                }

                2
            }
        };

        self.prev_states = None;

        Ok(appended)
    }
}
