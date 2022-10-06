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
use risingwave_common::array::{ArrayBuilderImpl, Op, Row};
use risingwave_common::types::Datum;
use risingwave_storage::StateStore;

use super::AggStateTable;
use crate::executor::error::StreamExecutorResult;
use crate::executor::managed_state::aggregation::ManagedStateImpl;

/// States for [`crate::executor::LocalSimpleAggExecutor`],
/// [`crate::executor::GlobalSimpleAggExecutor`] and [`crate::executor::HashAggExecutor`].
pub struct AggState<S: StateStore> {
    /// Group key of the state.
    group_key: Option<Row>,

    /// Current managed states for all [`crate::executor::aggregation::AggCall`]s.
    managed_states: Vec<ManagedStateImpl<S>>,

    /// Previous outputs of managed states. Initializing with `None`.
    prev_outputs: Option<Vec<Datum>>,
}

impl<S: StateStore> Debug for AggState<S> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AggState")
            .field("group_key", &self.group_key)
            .field("prev_outputs", &self.prev_outputs)
            .finish()
    }
}

/// We assume the first state of aggregation is always `StreamingRowCountAgg`.
pub const ROW_COUNT_COLUMN: usize = 0;

impl<S: StateStore> AggState<S> {
    pub fn new(
        group_key: Option<Row>,
        managed_states: Vec<ManagedStateImpl<S>>,
        prev_outputs: Option<Vec<Datum>>,
    ) -> Self {
        Self {
            group_key,
            managed_states,
            prev_outputs,
        }
    }

    pub fn managed_states(&mut self) -> &mut [ManagedStateImpl<S>] {
        self.managed_states.as_mut()
    }

    pub fn prev_row_count(&self) -> i64 {
        match &self.prev_outputs {
            Some(states) => states[ROW_COUNT_COLUMN]
                .as_ref()
                .map(|x| *x.as_int64())
                .unwrap_or(0),
            None => 0,
        }
    }

    /// Get the outputs of all managed states.
    async fn get_outputs(
        &mut self,
        agg_state_tables: &[Option<AggStateTable<S>>],
    ) -> StreamExecutorResult<Vec<Datum>> {
        futures::future::try_join_all(
            self.managed_states
                .iter_mut()
                .zip_eq(agg_state_tables)
                .map(|(state, agg_state_table)| state.get_output(agg_state_table.as_ref())),
        )
        .await
    }

    /// Build changes into `builders` and `new_ops`, according to previous and current agg outputs.
    /// Note that for [`crate::executor::HashAggExecutor`].
    ///
    /// Returns how many rows are appended in builders and the result row including group key
    /// prefix.
    ///
    /// The saved previous outputs will be updated to the latest outputs after building changes.
    pub async fn build_changes(
        &mut self,
        builders: &mut [ArrayBuilderImpl],
        new_ops: &mut Vec<Op>,
        agg_state_tables: &[Option<AggStateTable<S>>],
    ) -> StreamExecutorResult<(usize, Row)> {
        let curr_outputs = self.get_outputs(agg_state_tables).await?;

        let row_count = curr_outputs[ROW_COUNT_COLUMN]
            .as_ref()
            .map(|x| *x.as_int64())
            .unwrap_or(0);
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

                for (builder, new_value) in builders.iter_mut().zip_eq(curr_outputs.iter()) {
                    trace!("append_datum (0 -> N): {:?}", new_value);
                    builder.append_datum(new_value);
                }

                1
            }

            (_, 0) => {
                // previous state is not empty, current state is empty, insert one `Delete` op.
                new_ops.push(Op::Delete);

                for (builder, old_value) in builders
                    .iter_mut()
                    .zip_eq(self.prev_outputs.as_ref().unwrap().iter())
                {
                    trace!("append_datum (N -> 0): {:?}", old_value);
                    builder.append_datum(old_value);
                }

                1
            }

            _ => {
                // previous state is not empty, current state is not empty, insert two `Update` op.
                new_ops.push(Op::UpdateDelete);
                new_ops.push(Op::UpdateInsert);

                for (builder, old_value, new_value) in itertools::multizip((
                    builders.iter_mut(),
                    self.prev_outputs.as_ref().unwrap().iter(),
                    curr_outputs.iter(),
                )) {
                    trace!(
                        "append_datum (N -> N): prev = {:?}, cur = {:?}",
                        old_value,
                        new_value
                    );

                    builder.append_datum(old_value);
                    builder.append_datum(new_value);
                }

                2
            }
        };

        let result_row = Row::new(
            self.group_key
                .as_ref()
                .unwrap_or_else(Row::empty)
                .values()
                .chain(curr_outputs.iter())
                .cloned()
                .collect(),
        );
        self.prev_outputs = Some(curr_outputs);

        Ok((appended, result_row))
    }
}
