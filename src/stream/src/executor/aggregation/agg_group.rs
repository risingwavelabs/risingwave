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
use std::sync::Arc;

use risingwave_common::array::stream_record::{Record, RecordType};
use risingwave_common::array::StreamChunk;
use risingwave_common::buffer::Bitmap;
use risingwave_common::catalog::Schema;
use risingwave_common::estimate_size::EstimateSize;
use risingwave_common::must_match;
use risingwave_common::row::{OwnedRow, Row, RowExt};
use risingwave_common::util::iter_util::ZipEqFast;
use risingwave_expr::aggregate::{AggCall, BoxedAggregateFunction};
use risingwave_storage::StateStore;

use super::agg_state::{AggState, AggStateStorage};
use crate::common::table::state_table::StateTable;
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

/// [`GroupKey`] wraps a concrete group key and handle its mapping to state table pk.
#[derive(Clone, Debug)]
pub struct GroupKey {
    row_prefix: OwnedRow,
    table_pk_projection: Arc<[usize]>,
}

impl GroupKey {
    pub fn new(row_prefix: OwnedRow, table_pk_projection: Option<Arc<[usize]>>) -> Self {
        let table_pk_projection =
            table_pk_projection.unwrap_or_else(|| (0..row_prefix.len()).collect());
        Self {
            row_prefix,
            table_pk_projection,
        }
    }

    pub fn len(&self) -> usize {
        self.row_prefix.len()
    }

    pub fn is_empty(&self) -> bool {
        self.row_prefix.is_empty()
    }

    /// Get the group key for state table row prefix.
    pub fn table_row(&self) -> &OwnedRow {
        &self.row_prefix
    }

    /// Get the group key for state table pk prefix.
    pub fn table_pk(&self) -> impl Row + '_ {
        (&self.row_prefix).project(&self.table_pk_projection)
    }

    /// Get the group key for LRU cache key prefix.
    pub fn cache_key(&self) -> impl Row + '_ {
        self.table_row()
    }
}

/// [`AggGroup`] manages agg states of all agg calls for one `group_key`.
pub struct AggGroup<S: StateStore, Strtg: Strategy> {
    /// Group key.
    group_key: Option<GroupKey>,

    /// Current managed states for all [`AggCall`]s.
    states: Vec<AggState>,

    /// Previous outputs of aggregate functions. Initializing with `None`.
    prev_outputs: Option<OwnedRow>,

    /// Index of row count agg call (`count(*)`) in the call list.
    row_count_index: usize,

    _phantom: PhantomData<(S, Strtg)>,
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
        group_key: Option<GroupKey>,
        agg_calls: &[AggCall],
        agg_funcs: &[BoxedAggregateFunction],
        storages: &[AggStateStorage<S>],
        intermediate_state_table: &StateTable<S>,
        pk_indices: &PkIndices,
        row_count_index: usize,
        extreme_cache_size: usize,
        input_schema: &Schema,
    ) -> StreamExecutorResult<Self> {
        let encoded_states = intermediate_state_table
            .get_row(group_key.as_ref().map(GroupKey::table_pk))
            .await?;
        if let Some(encoded_states) = &encoded_states {
            assert_eq!(encoded_states.len(), agg_calls.len());
        }

        let mut states = Vec::with_capacity(agg_calls.len());
        for (idx, (agg_call, agg_func)) in agg_calls.iter().zip_eq_fast(agg_funcs).enumerate() {
            let state = AggState::create(
                agg_call,
                agg_func,
                &storages[idx],
                encoded_states.as_ref().map(|outputs| &outputs[idx]),
                pk_indices,
                extreme_cache_size,
                input_schema,
            )?;
            states.push(state);
        }

        let mut this = Self {
            group_key,
            states,
            prev_outputs: None, // will be initialized later
            row_count_index,
            _phantom: PhantomData,
        };

        if encoded_states.is_some() {
            let (_, outputs) = this.get_outputs(storages, agg_funcs).await?;
            this.prev_outputs = Some(outputs);
        }

        Ok(this)
    }

    /// Create a group from encoded states for EOWC. The previous output is set to `None`.
    #[allow(clippy::too_many_arguments)]
    pub fn create_eowc(
        group_key: Option<GroupKey>,
        agg_calls: &[AggCall],
        agg_funcs: &[BoxedAggregateFunction],
        storages: &[AggStateStorage<S>],
        encoded_states: &OwnedRow,
        pk_indices: &PkIndices,
        row_count_index: usize,
        extreme_cache_size: usize,
        input_schema: &Schema,
    ) -> StreamExecutorResult<Self> {
        let mut states = Vec::with_capacity(agg_calls.len());
        for (idx, (agg_call, agg_func)) in agg_calls.iter().zip_eq_fast(agg_funcs).enumerate() {
            let state = AggState::create(
                agg_call,
                agg_func,
                &storages[idx],
                Some(&encoded_states[idx]),
                pk_indices,
                extreme_cache_size,
                input_schema,
            )?;
            states.push(state);
        }

        Ok(Self {
            group_key,
            states,
            prev_outputs: None,
            row_count_index,
            _phantom: PhantomData,
        })
    }

    pub fn group_key(&self) -> Option<&GroupKey> {
        self.group_key.as_ref()
    }

    pub fn group_key_row(&self) -> OwnedRow {
        self.group_key
            .as_ref()
            .map(GroupKey::table_row)
            .cloned()
            .unwrap_or_default()
    }

    fn prev_row_count(&self) -> usize {
        match &self.prev_outputs {
            Some(states) => states[self.row_count_index]
                .as_ref()
                .map(|x| {
                    TryInto::try_into(*x.as_int64()).expect("row count should be non-negative")
                })
                .unwrap_or(0),
            None => 0,
        }
    }

    /// Get current row count of this group.
    fn curr_row_count(&self) -> usize {
        let row_count_state = must_match!(
            self.states[self.row_count_index],
            AggState::Value(ref state) => state
        );
        let row_count = *row_count_state
            .as_datum()
            .as_ref()
            .expect("row count state should not be NULL")
            .as_int64();
        if row_count < 0 {
            tracing::error!(group = ?self.group_key_row(), "bad row count");
            panic!("row count should be non-negative")
        }
        row_count.try_into().unwrap()
    }

    pub(crate) fn is_uninitialized(&self) -> bool {
        self.prev_outputs.is_none()
    }

    /// Apply input chunk to all managed agg states.
    ///
    /// `mappings` contains the column mappings from input chunk to each agg call.
    /// `visibilities` contains the row visibility of the input chunk for each agg call.
    pub async fn apply_chunk(
        &mut self,
        chunk: &StreamChunk,
        calls: &[AggCall],
        funcs: &[BoxedAggregateFunction],
        visibilities: Vec<Bitmap>,
    ) -> StreamExecutorResult<()> {
        if self.curr_row_count() == 0 {
            tracing::trace!(group = ?self.group_key_row(), "first time see this group");
        }
        for (((state, call), func), visibility) in (self.states.iter_mut())
            .zip_eq_fast(calls)
            .zip_eq_fast(funcs)
            .zip_eq_fast(visibilities)
        {
            state.apply_chunk(chunk, call, func, visibility).await?;
        }

        if self.curr_row_count() == 0 {
            tracing::trace!(group = ?self.group_key_row(), "last time see this group");
        }

        Ok(())
    }

    /// Reset all in-memory states to their initial state, i.e. to reset all agg state structs to
    /// the status as if they are just created, no input applied and no row in state table.
    fn reset(&mut self, funcs: &[BoxedAggregateFunction]) {
        for (state, func) in self.states.iter_mut().zip_eq_fast(funcs) {
            state.reset(func);
        }
    }

    /// Encode intermediate states.
    pub fn encode_states(
        &self,
        funcs: &[BoxedAggregateFunction],
    ) -> StreamExecutorResult<OwnedRow> {
        let mut encoded_states = Vec::with_capacity(self.states.len());
        for (state, func) in self.states.iter().zip_eq_fast(funcs) {
            let encoded = match state {
                AggState::Value(s) => func.encode_state(s)?,
                // For minput state, we don't need to store it in state table.
                AggState::MaterializedInput(_) => None,
            };
            encoded_states.push(encoded);
        }
        let states = self
            .group_key()
            .map(GroupKey::table_row)
            .chain(OwnedRow::new(encoded_states))
            .into_owned_row();
        Ok(states)
    }

    /// Get the outputs of all managed agg states, without group key prefix.
    /// Possibly need to read/sync from state table if the state not cached in memory.
    /// This method is idempotent, i.e. it can be called multiple times and the outputs are
    /// guaranteed to be the same.
    async fn get_outputs(
        &mut self,
        storages: &[AggStateStorage<S>],
        funcs: &[BoxedAggregateFunction],
    ) -> StreamExecutorResult<(usize, OwnedRow)> {
        let row_count = self.curr_row_count();
        if row_count == 0 {
            // Reset all states (in fact only value states will be reset).
            // This is important because for some agg calls (e.g. `sum`), if no row is applied,
            // they should output NULL, for some other calls (e.g. `sum0`), they should output 0.
            // FIXME(rc): Deciding whether to reset states according to `row_count` is not precisely
            // correct, see https://github.com/risingwavelabs/risingwave/issues/7412 for bug description.
            self.reset(funcs);
        }
        futures::future::try_join_all(
            self.states
                .iter_mut()
                .zip_eq_fast(storages)
                .zip_eq_fast(funcs)
                .map(|((state, storage), func)| {
                    state.get_output(storage, func, self.group_key.as_ref())
                }),
        )
        .await
        .map(|row| (row_count, OwnedRow::new(row)))
    }

    /// Build aggregation result change, according to previous and current agg outputs.
    /// The saved previous outputs will be updated to the latest outputs after this method.
    pub async fn build_change(
        &mut self,
        storages: &[AggStateStorage<S>],
        funcs: &[BoxedAggregateFunction],
    ) -> StreamExecutorResult<Option<Record<OwnedRow>>> {
        let prev_row_count = self.prev_row_count();
        let (curr_row_count, curr_outputs) = self.get_outputs(storages, funcs).await?;

        let change_type = Strtg::infer_change_type(
            prev_row_count,
            curr_row_count,
            self.prev_outputs.as_ref(),
            &curr_outputs,
        );

        tracing::trace!(
            group = ?self.group_key_row(),
            prev_row_count,
            curr_row_count,
            change_type = ?change_type,
            "build change"
        );

        Ok(change_type.map(|change_type| match change_type {
            RecordType::Insert => {
                let new_row = self
                    .group_key()
                    .map(GroupKey::table_row)
                    .chain(&curr_outputs)
                    .into_owned_row();
                self.prev_outputs = Some(curr_outputs);
                Record::Insert { new_row }
            }
            RecordType::Delete => {
                let prev_outputs = self.prev_outputs.take();
                let old_row = self
                    .group_key()
                    .map(GroupKey::table_row)
                    .chain(prev_outputs)
                    .into_owned_row();
                Record::Delete { old_row }
            }
            RecordType::Update => {
                let new_row = self
                    .group_key()
                    .map(GroupKey::table_row)
                    .chain(&curr_outputs)
                    .into_owned_row();
                let prev_outputs = self.prev_outputs.replace(curr_outputs);
                let old_row = self
                    .group_key()
                    .map(GroupKey::table_row)
                    .chain(prev_outputs)
                    .into_owned_row();
                Record::Update { old_row, new_row }
            }
        }))
    }
}
