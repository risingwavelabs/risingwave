// Copyright 2023 RisingWave Labs
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::fmt::Debug;
use std::marker::PhantomData;

use itertools::Itertools;
use risingwave_common::array::stream_record::{Record, RecordType};
use risingwave_common::array::{ArrayRef, Op, StreamChunk};
use risingwave_common::buffer::Bitmap;
use risingwave_common::catalog::Schema;
use risingwave_common::estimate_size::EstimateSize;
use risingwave_common::must_match;
use risingwave_common::row::{OwnedRow, Row, RowExt};
use risingwave_common::types::DataType;
use risingwave_common::util::iter_util::ZipEqFast;
use risingwave_expr::agg::AggCall;
use risingwave_storage::StateStore;

use super::agg_state::{AggState, AggStateStorage};
use crate::common::table::state_table::StateTable;
use crate::common::StreamChunkBuilder;
use crate::executor::error::StreamExecutorResult;
use crate::executor::PkIndices;

pub trait Strategy {
    /// Infer the change type of the aggregation result. Don't need to take the ownership of
    /// `prev_outputs` and `curr_outputs`.
    fn infer_change_type(
        prev_row_count: usize,
        curr_row_count: usize,
        prev_outputs: Option<&OwnedRow>,
        curr_outputs: &OwnedRow,
    ) -> Option<RecordType>;
}

/// The strategy that always outputs the aggregation result no matter there're input rows or not.
pub struct AlwaysOutput;
/// The strategy that only outputs the aggregation result when there're input rows. If row count
/// drops to 0, the output row will be deleted.
pub struct OnlyOutputIfHasInput;

impl Strategy for AlwaysOutput {
    fn infer_change_type(
        prev_row_count: usize,
        curr_row_count: usize,
        prev_outputs: Option<&OwnedRow>,
        curr_outputs: &OwnedRow,
    ) -> Option<RecordType> {
        match prev_outputs {
            None => {
                // First time to build changes, assert to ensure correctness.
                // Note that it's not true vice versa, i.e. `prev_row_count == 0` doesn't imply
                // `prev_outputs == None`.
                assert_eq!(prev_row_count, 0);

                // Generate output no matter whether current row count is 0 or not.
                Some(RecordType::Insert)
            }
            Some(prev_outputs) => {
                if prev_row_count == 0 && curr_row_count == 0 || prev_outputs == curr_outputs {
                    // No rows exist, or output is not changed.
                    None
                } else {
                    Some(RecordType::Update)
                }
            }
        }
    }
}

impl Strategy for OnlyOutputIfHasInput {
    fn infer_change_type(
        prev_row_count: usize,
        curr_row_count: usize,
        prev_outputs: Option<&OwnedRow>,
        curr_outputs: &OwnedRow,
    ) -> Option<RecordType> {
        match (prev_row_count, curr_row_count) {
            (0, 0) => {
                // No rows of current group exist.
                None
            }
            (0, _) => {
                // Insert new output row for this newly emerged group.
                Some(RecordType::Insert)
            }
            (_, 0) => {
                // Delete old output row for this newly disappeared group.
                Some(RecordType::Delete)
            }
            (_, _) => {
                // Update output row.
                if prev_outputs.expect("must exist previous outputs") == curr_outputs {
                    // No output change.
                    None
                } else {
                    Some(RecordType::Update)
                }
            }
        }
    }
}

/// [`AggGroup`] manages agg states of all agg calls for one `group_key`.
pub struct AggGroup<S: StateStore, Strtg: Strategy> {
    /// Group key.
    group_key: Option<OwnedRow>,

    /// Current managed states for all [`AggCall`]s.
    states: Vec<AggState<S>>,

    /// Previous outputs of managed states. Initializing with `None`.
    prev_outputs: Option<OwnedRow>,

    /// Index of row count agg call (`count(*)`) in the call list.
    row_count_index: usize,

    _phantom: PhantomData<Strtg>,
}

impl<S: StateStore, Strtg: Strategy> Debug for AggGroup<S, Strtg> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AggGroup")
            .field("group_key", &self.group_key)
            .field("prev_outputs", &self.prev_outputs)
            .finish()
    }
}

impl<S: StateStore, Strtg: Strategy> EstimateSize for AggGroup<S, Strtg> {
    fn estimated_heap_size(&self) -> usize {
        self.states
            .iter()
            .map(|state| state.estimated_heap_size())
            .sum()
    }
}

impl<S: StateStore, Strtg: Strategy> AggGroup<S, Strtg> {
    /// Create [`AggGroup`] for the given [`AggCall`]s and `group_key`.
    /// For [`crate::executor::SimpleAggExecutor`], the `group_key` should be `None`.
    #[allow(clippy::too_many_arguments)]
    pub async fn create(
        group_key: Option<OwnedRow>,
        agg_calls: &[AggCall],
        storages: &[AggStateStorage<S>],
        result_table: &StateTable<S>,
        pk_indices: &PkIndices,
        row_count_index: usize,
        extreme_cache_size: usize,
        input_schema: &Schema,
    ) -> StreamExecutorResult<AggGroup<S, Strtg>> {
        let prev_outputs: Option<OwnedRow> = result_table.get_row(&group_key).await?;
        if let Some(prev_outputs) = &prev_outputs {
            assert_eq!(prev_outputs.len(), agg_calls.len());
        }

        let states =
            futures::future::try_join_all(agg_calls.iter().enumerate().map(|(idx, agg_call)| {
                AggState::create(
                    agg_call,
                    &storages[idx],
                    prev_outputs.as_ref().map(|outputs| &outputs[idx]),
                    pk_indices,
                    group_key.as_ref(),
                    extreme_cache_size,
                    input_schema,
                )
            }))
            .await?;

        Ok(Self {
            group_key,
            states,
            prev_outputs,
            row_count_index,
            _phantom: PhantomData,
        })
    }

    pub fn group_key(&self) -> Option<&OwnedRow> {
        self.group_key.as_ref()
    }

    fn prev_row_count(&self) -> usize {
        match &self.prev_outputs {
            Some(states) => states[self.row_count_index]
                .as_ref()
                .map(|x| *x.as_int64() as usize)
                .unwrap_or(0),
            None => 0,
        }
    }

    pub(crate) fn is_uninitialized(&self) -> bool {
        self.prev_outputs.is_none()
    }

    /// Apply input chunk to all managed agg states.
    /// `visibilities` contains the row visibility of the input chunk for each agg call.
    pub fn apply_chunk(
        &mut self,
        storages: &mut [AggStateStorage<S>],
        ops: &[Op],
        columns: &[ArrayRef],
        visibilities: Vec<Option<Bitmap>>,
    ) -> StreamExecutorResult<()> {
        let columns = columns.iter().map(|col| col.as_ref()).collect_vec();
        for ((state, storage), visibility) in self
            .states
            .iter_mut()
            .zip_eq_fast(storages)
            .zip_eq_fast(visibilities)
        {
            state.apply_chunk(ops, visibility.as_ref(), &columns, storage)?;
        }
        Ok(())
    }

    /// Flush in-memory state into state table if needed.
    /// The calling order of this method and `get_outputs` doesn't matter, but this method
    /// must be called before committing state tables.
    pub async fn flush_state_if_needed(
        &self,
        storages: &mut [AggStateStorage<S>],
    ) -> StreamExecutorResult<()> {
        futures::future::try_join_all(self.states.iter().zip_eq_fast(storages).filter_map(
            |(state, storage)| match state {
                AggState::Table(state) => Some(state.flush_state_if_needed(
                    must_match!(storage, AggStateStorage::Table { table } => table),
                    self.group_key(),
                )),
                _ => None,
            },
        ))
        .await?;
        Ok(())
    }

    /// Reset all in-memory states to their initial state, i.e. to reset all agg state structs to
    /// the status as if they are just created, no input applied and no row in state table.
    fn reset(&mut self) {
        self.states.iter_mut().for_each(|state| state.reset());
    }

    /// Get the outputs of all managed agg states, without group key prefix.
    /// Possibly need to read/sync from state table if the state not cached in memory.
    /// This method is idempotent, i.e. it can be called multiple times and the outputs are
    /// guaranteed to be the same.
    pub async fn get_outputs(
        &mut self,
        storages: &[AggStateStorage<S>],
    ) -> StreamExecutorResult<OwnedRow> {
        // Row count doesn't need I/O, so the following statement is supposed to be fast.
        let row_count = self.states[self.row_count_index]
            .get_output(&storages[self.row_count_index], self.group_key.as_ref())
            .await?
            .as_ref()
            .map(|x| *x.as_int64() as usize)
            .expect("row count should not be None");
        if row_count == 0 {
            // Reset all states (in fact only value states will be reset).
            // This is important because for some agg calls (e.g. `sum`), if no row is applied,
            // they should output NULL, for some other calls (e.g. `sum0`), they should output 0.
            // FIXME(rc): Deciding whether to reset states according to `row_count` is not precisely
            // correct, see https://github.com/risingwavelabs/risingwave/issues/7412 for bug description.
            self.reset();
        }
        futures::future::try_join_all(
            self.states
                .iter_mut()
                .zip_eq_fast(storages)
                .map(|(state, storage)| state.get_output(storage, self.group_key.as_ref())),
        )
        .await
        .map(OwnedRow::new)
    }

    /// Build aggregation result change, according to previous and current agg outputs.
    /// The saved previous outputs will be updated to the latest outputs after this method.
    pub fn build_change(&mut self, curr_outputs: OwnedRow) -> Option<Record<OwnedRow>> {
        let prev_row_count = self.prev_row_count();
        let curr_row_count = curr_outputs[self.row_count_index]
            .as_ref()
            .map(|x| *x.as_int64() as usize)
            .expect("row count should not be None");

        trace!(
            "prev_row_count = {}, curr_row_count = {}",
            prev_row_count,
            curr_row_count
        );

        let change_type = Strtg::infer_change_type(
            prev_row_count,
            curr_row_count,
            self.prev_outputs.as_ref(),
            &curr_outputs,
        );

        change_type.map(|change_type| match change_type {
            RecordType::Insert => {
                let new_row = self.group_key().chain(&curr_outputs).into_owned_row();
                self.prev_outputs = Some(curr_outputs);
                Record::Insert { new_row }
            }
            RecordType::Delete => {
                let prev_outputs = self.prev_outputs.take();
                let old_row = self.group_key().chain(prev_outputs).into_owned_row();
                Record::Delete { old_row }
            }
            RecordType::Update => {
                let new_row = self.group_key().chain(&curr_outputs).into_owned_row();
                let prev_outputs = self.prev_outputs.replace(curr_outputs);
                let old_row = self.group_key().chain(prev_outputs).into_owned_row();
                Record::Update { old_row, new_row }
            }
        })
    }
}

// TODO(rc): split logic of `StreamChunkBuilder` to chunk builder and row merger.
/// A wrapper of [`StreamChunkBuilder`] that provides a more convenient API.
pub struct ChunkBuilder {
    inner: StreamChunkBuilder,
}

impl ChunkBuilder {
    pub fn new(capacity: usize, data_types: &[DataType]) -> Self {
        Self {
            inner: StreamChunkBuilder::new(
                capacity,
                data_types,
                (0..data_types.len()).map(|x| (x, x)).collect(),
                vec![],
            ),
        }
    }

    /// Append a row to the builder, return a chunk if the builder is full.
    #[must_use]
    pub fn append_row(&mut self, op: Op, row: impl Row) -> Option<StreamChunk> {
        self.inner.append_row_update(op, row)
    }

    /// Append a record to the builder, return a chunk if the builder is full.
    pub fn append_record(&mut self, record: Record<impl Row>) -> Option<StreamChunk> {
        match record {
            Record::Insert { new_row } => self.append_row(Op::Insert, new_row),
            Record::Delete { old_row } => self.append_row(Op::Delete, old_row),
            Record::Update { old_row, new_row } => {
                let _none = self.append_row(Op::UpdateDelete, old_row);
                self.append_row(Op::UpdateInsert, new_row)
            }
        }
    }

    /// Take remaining rows and build a chunk.
    pub fn take(&mut self) -> Option<StreamChunk> {
        self.inner.take()
    }
}
