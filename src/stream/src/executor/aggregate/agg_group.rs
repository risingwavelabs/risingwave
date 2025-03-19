// Copyright 2025 RisingWave Labs
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

use risingwave_common::array::StreamChunk;
use risingwave_common::array::stream_record::{Record, RecordType};
use risingwave_common::bitmap::Bitmap;
use risingwave_common::catalog::Schema;
use risingwave_common::must_match;
use risingwave_common::row::{OwnedRow, Row, RowExt};
use risingwave_common::util::iter_util::ZipEqFast;
use risingwave_common_estimate_size::EstimateSize;
use risingwave_expr::aggregate::{AggCall, BoxedAggregateFunction};
use risingwave_pb::stream_plan::PbAggNodeVersion;
use risingwave_storage::StateStore;

use super::agg_state::{AggState, AggStateStorage};
use crate::common::table::state_table::StateTable;
use crate::consistency::consistency_panic;
use crate::executor::PkIndices;
use crate::executor::error::StreamExecutorResult;

#[derive(Debug)]
pub struct Context {
    group_key: Option<GroupKey>,
}

impl Context {
    pub fn group_key(&self) -> Option<&GroupKey> {
        self.group_key.as_ref()
    }

    pub fn group_key_row(&self) -> OwnedRow {
        self.group_key()
            .map(GroupKey::table_row)
            .cloned()
            .unwrap_or_default()
    }
}

fn row_count_of(ctx: &Context, row: Option<impl Row>, row_count_col: usize) -> usize {
    match row {
        Some(row) => {
            let mut row_count = row
                .datum_at(row_count_col)
                .expect("row count field should not be NULL")
                .into_int64();

            if row_count < 0 {
                consistency_panic!(group = ?ctx.group_key_row(), row_count, "row count should be non-negative");

                // NOTE: Here is the case that an inconsistent `DELETE` arrives at HashAgg executor, and there's no
                // corresponding group existing before (or has been deleted). In this case, previous row count should
                // be `0` and current row count be `-1` after handling the `DELETE`. To ignore the inconsistency, we
                // reset `row_count` to `0` here, so that `OnlyOutputIfHasInput` will return no change, so that the
                // inconsistent will be hidden from downstream. This won't prevent from incorrect results of existing
                // groups, but at least can prevent from downstream panicking due to non-existing keys.
                // See https://github.com/risingwavelabs/risingwave/issues/14031 for more information.
                row_count = 0;
            }
            row_count.try_into().unwrap()
        }
        None => 0,
    }
}

pub trait Strategy {
    /// Infer the change type of the aggregation result. Don't need to take the ownership of
    /// `prev_row` and `curr_row`.
    fn infer_change_type(
        ctx: &Context,
        prev_row: Option<&OwnedRow>,
        curr_row: &OwnedRow,
        row_count_col: usize,
    ) -> Option<RecordType>;
}

/// The strategy that always outputs the aggregation result no matter there're input rows or not.
pub struct AlwaysOutput;
/// The strategy that only outputs the aggregation result when there're input rows. If row count
/// drops to 0, the output row will be deleted.
pub struct OnlyOutputIfHasInput;

impl Strategy for AlwaysOutput {
    fn infer_change_type(
        ctx: &Context,
        prev_row: Option<&OwnedRow>,
        _curr_row: &OwnedRow,
        row_count_col: usize,
    ) -> Option<RecordType> {
        let prev_row_count = row_count_of(ctx, prev_row, row_count_col);
        match prev_row {
            None => {
                // First time to build changes, assert to ensure correctness.
                // Note that it's not true vice versa, i.e. `prev_row_count == 0` doesn't imply
                // `prev_outputs == None`.
                assert_eq!(prev_row_count, 0);

                // Generate output no matter whether current row count is 0 or not.
                Some(RecordType::Insert)
            }
            // NOTE(kwannoel): We always output, even if the update is a no-op.
            // e.g. the following will still be emitted downstream:
            // ```
            // U- 1
            // U+ 1
            // ```
            // This is to support `approx_percentile` via `row_merge`, which requires
            // both the lhs and rhs to always output updates per epoch, or not all.
            // Otherwise we are unable to construct a full row, if only one side updates,
            // as the `row_merge` executor is stateless.
            Some(_prev_outputs) => Some(RecordType::Update),
        }
    }
}

impl Strategy for OnlyOutputIfHasInput {
    fn infer_change_type(
        ctx: &Context,
        prev_row: Option<&OwnedRow>,
        curr_row: &OwnedRow,
        row_count_col: usize,
    ) -> Option<RecordType> {
        let prev_row_count = row_count_of(ctx, prev_row, row_count_col);
        let curr_row_count = row_count_of(ctx, Some(curr_row), row_count_col);

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
                if prev_row.expect("must exist previous row") == curr_row {
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
    /// Agg group context, containing the group key.
    ctx: Context,

    /// Current managed states for all [`AggCall`]s.
    states: Vec<AggState>,

    /// Previous intermediate states, stored in the intermediate state table.
    prev_inter_states: Option<OwnedRow>,

    /// Previous outputs, yielded to downstream.
    /// If `EOWC` is true, this field is not used.
    prev_outputs: Option<OwnedRow>,

    /// Index of row count agg call (`count(*)`) in the call list.
    row_count_index: usize,

    /// Whether the emit policy is EOWC.
    emit_on_window_close: bool,

    _phantom: PhantomData<(S, Strtg)>,
}

impl<S: StateStore, Strtg: Strategy> Debug for AggGroup<S, Strtg> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AggGroup")
            .field("group_key", &self.ctx.group_key)
            .field("prev_inter_states", &self.prev_inter_states)
            .field("prev_outputs", &self.prev_outputs)
            .field("row_count_index", &self.row_count_index)
            .field("emit_on_window_close", &self.emit_on_window_close)
            .finish()
    }
}

impl<S: StateStore, Strtg: Strategy> EstimateSize for AggGroup<S, Strtg> {
    fn estimated_heap_size(&self) -> usize {
        // TODO(rc): should include the size of `prev_inter_states` and `prev_outputs`
        self.states
            .iter()
            .map(|state| state.estimated_heap_size())
            .sum()
    }
}

impl<S: StateStore, Strtg: Strategy> AggGroup<S, Strtg> {
    /// Create [`AggGroup`] for the given [`AggCall`]s and `group_key`.
    /// For [`SimpleAggExecutor`], the `group_key` should be `None`.
    ///
    /// [`SimpleAggExecutor`]: crate::executor::aggregate::SimpleAggExecutor
    #[allow(clippy::too_many_arguments)]
    pub async fn create(
        version: PbAggNodeVersion,
        group_key: Option<GroupKey>,
        agg_calls: &[AggCall],
        agg_funcs: &[BoxedAggregateFunction],
        storages: &[AggStateStorage<S>],
        intermediate_state_table: &StateTable<S>,
        pk_indices: &PkIndices,
        row_count_index: usize,
        emit_on_window_close: bool,
        extreme_cache_size: usize,
        input_schema: &Schema,
    ) -> StreamExecutorResult<Self> {
        let inter_states = intermediate_state_table
            .get_row(group_key.as_ref().map(GroupKey::table_pk))
            .await?;
        if let Some(inter_states) = &inter_states {
            assert_eq!(inter_states.len(), agg_calls.len());
        }

        let mut states = Vec::with_capacity(agg_calls.len());
        for (idx, (agg_call, agg_func)) in agg_calls.iter().zip_eq_fast(agg_funcs).enumerate() {
            let state = AggState::create(
                version,
                agg_call,
                agg_func,
                &storages[idx],
                inter_states.as_ref().map(|s| &s[idx]),
                pk_indices,
                extreme_cache_size,
                input_schema,
            )?;
            states.push(state);
        }

        let mut this = Self {
            ctx: Context { group_key },
            states,
            prev_inter_states: inter_states,
            prev_outputs: None, // will be set below
            row_count_index,
            emit_on_window_close,
            _phantom: PhantomData,
        };

        if !this.emit_on_window_close && this.prev_inter_states.is_some() {
            let (outputs, _stats) = this.get_outputs(storages, agg_funcs).await?;
            this.prev_outputs = Some(outputs);
        }

        Ok(this)
    }

    /// Create a group from intermediate states for EOWC output.
    /// Will always produce `Insert` when building change.
    #[allow(clippy::too_many_arguments)]
    pub fn for_eowc_output(
        version: PbAggNodeVersion,
        group_key: Option<GroupKey>,
        agg_calls: &[AggCall],
        agg_funcs: &[BoxedAggregateFunction],
        storages: &[AggStateStorage<S>],
        inter_states: &OwnedRow,
        pk_indices: &PkIndices,
        row_count_index: usize,
        emit_on_window_close: bool,
        extreme_cache_size: usize,
        input_schema: &Schema,
    ) -> StreamExecutorResult<Self> {
        let mut states = Vec::with_capacity(agg_calls.len());
        for (idx, (agg_call, agg_func)) in agg_calls.iter().zip_eq_fast(agg_funcs).enumerate() {
            let state = AggState::create(
                version,
                agg_call,
                agg_func,
                &storages[idx],
                Some(&inter_states[idx]),
                pk_indices,
                extreme_cache_size,
                input_schema,
            )?;
            states.push(state);
        }

        Ok(Self {
            ctx: Context { group_key },
            states,
            prev_inter_states: None, // this doesn't matter
            prev_outputs: None,      // this will make sure the outputs change to be `Insert`
            row_count_index,
            emit_on_window_close,
            _phantom: PhantomData,
        })
    }

    pub fn group_key(&self) -> Option<&GroupKey> {
        self.ctx.group_key()
    }

    /// Get current row count of this group.
    fn curr_row_count(&self) -> usize {
        let row_count_state = must_match!(
            self.states[self.row_count_index],
            AggState::Value(ref state) => state
        );
        row_count_of(&self.ctx, Some([row_count_state.as_datum().clone()]), 0)
    }

    pub(crate) fn is_uninitialized(&self) -> bool {
        self.prev_inter_states.is_none()
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
            tracing::trace!(group = ?self.ctx.group_key_row(), "first time see this group");
        }
        for (((state, call), func), visibility) in (self.states.iter_mut())
            .zip_eq_fast(calls)
            .zip_eq_fast(funcs)
            .zip_eq_fast(visibilities)
        {
            state.apply_chunk(chunk, call, func, visibility).await?;
        }

        if self.curr_row_count() == 0 {
            tracing::trace!(group = ?self.ctx.group_key_row(), "last time see this group");
        }

        Ok(())
    }

    /// Reset all in-memory states to their initial state, i.e. to reset all agg state structs to
    /// the status as if they are just created, no input applied and no row in state table.
    fn reset(&mut self, funcs: &[BoxedAggregateFunction]) -> StreamExecutorResult<()> {
        for (state, func) in self.states.iter_mut().zip_eq_fast(funcs) {
            state.reset(func)?;
        }
        Ok(())
    }

    /// Get the encoded intermediate states of all managed agg states.
    fn get_inter_states(&self, funcs: &[BoxedAggregateFunction]) -> StreamExecutorResult<OwnedRow> {
        let mut inter_states = Vec::with_capacity(self.states.len());
        for (state, func) in self.states.iter().zip_eq_fast(funcs) {
            let encoded = match state {
                AggState::Value(s) => func.encode_state(s)?,
                // For minput state, we don't need to store it in state table.
                AggState::MaterializedInput(_) => None,
            };
            inter_states.push(encoded);
        }
        Ok(OwnedRow::new(inter_states))
    }

    /// Get the outputs of all managed agg states, without group key prefix.
    /// Possibly need to read/sync from state table if the state not cached in memory.
    /// This method is idempotent, i.e. it can be called multiple times and the outputs are
    /// guaranteed to be the same.
    async fn get_outputs(
        &mut self,
        storages: &[AggStateStorage<S>],
        funcs: &[BoxedAggregateFunction],
    ) -> StreamExecutorResult<(OwnedRow, AggStateCacheStats)> {
        let row_count = self.curr_row_count();
        if row_count == 0 {
            // Reset all states (in fact only value states will be reset).
            // This is important because for some agg calls (e.g. `sum`), if no row is applied,
            // they should output NULL, for some other calls (e.g. `sum0`), they should output 0.
            // This actually also prevents inconsistent negative row count from being worse.
            // FIXME(rc): Deciding whether to reset states according to `row_count` is not precisely
            // correct, see https://github.com/risingwavelabs/risingwave/issues/7412 for bug description.
            self.reset(funcs)?;
        }
        let mut stats = AggStateCacheStats::default();
        futures::future::try_join_all(
            self.states
                .iter_mut()
                .zip_eq_fast(storages)
                .zip_eq_fast(funcs)
                .map(|((state, storage), func)| {
                    state.get_output(storage, func, self.ctx.group_key())
                }),
        )
        .await
        .map(|outputs_and_stats| {
            outputs_and_stats
                .into_iter()
                .map(|(output, stat)| {
                    stats.merge(stat);
                    output
                })
                .collect::<Vec<_>>()
        })
        .map(|row| (OwnedRow::new(row), stats))
    }

    /// Build change for aggregation intermediate states, according to previous and current agg states.
    /// The change should be applied to the intermediate state table.
    ///
    /// The saved previous inter states will be updated to the latest states after calling this method.
    pub fn build_states_change(
        &mut self,
        funcs: &[BoxedAggregateFunction],
    ) -> StreamExecutorResult<Option<Record<OwnedRow>>> {
        let curr_inter_states = self.get_inter_states(funcs)?;
        let change_type = Strtg::infer_change_type(
            &self.ctx,
            self.prev_inter_states.as_ref(),
            &curr_inter_states,
            self.row_count_index,
        );

        tracing::trace!(
            group = ?self.ctx.group_key_row(),
            prev_inter_states = ?self.prev_inter_states,
            curr_inter_states = ?curr_inter_states,
            change_type = ?change_type,
            "build intermediate states change"
        );

        let Some(change_type) = change_type else {
            return Ok(None);
        };
        Ok(Some(match change_type {
            RecordType::Insert => {
                let new_row = self
                    .group_key()
                    .map(GroupKey::table_row)
                    .chain(&curr_inter_states)
                    .into_owned_row();
                self.prev_inter_states = Some(curr_inter_states);
                Record::Insert { new_row }
            }
            RecordType::Delete => {
                let prev_inter_states = self
                    .prev_inter_states
                    .take()
                    .expect("must exist previous intermediate states");
                let old_row = self
                    .group_key()
                    .map(GroupKey::table_row)
                    .chain(prev_inter_states)
                    .into_owned_row();
                Record::Delete { old_row }
            }
            RecordType::Update => {
                let new_row = self
                    .group_key()
                    .map(GroupKey::table_row)
                    .chain(&curr_inter_states)
                    .into_owned_row();
                let prev_inter_states = self
                    .prev_inter_states
                    .replace(curr_inter_states)
                    .expect("must exist previous intermediate states");
                let old_row = self
                    .group_key()
                    .map(GroupKey::table_row)
                    .chain(prev_inter_states)
                    .into_owned_row();
                Record::Update { old_row, new_row }
            }
        }))
    }

    /// Build aggregation result change, according to previous and current agg outputs.
    /// The change should be yielded to downstream.
    ///
    /// The saved previous outputs will be updated to the latest outputs after this method.
    ///
    /// Note that this method is very likely to cost more than `build_states_change`, because it
    /// needs to produce output for materialized input states which may involve state table read.
    pub async fn build_outputs_change(
        &mut self,
        storages: &[AggStateStorage<S>],
        funcs: &[BoxedAggregateFunction],
    ) -> StreamExecutorResult<(Option<Record<OwnedRow>>, AggStateCacheStats)> {
        let (curr_outputs, stats) = self.get_outputs(storages, funcs).await?;

        let change_type = Strtg::infer_change_type(
            &self.ctx,
            self.prev_outputs.as_ref(),
            &curr_outputs,
            self.row_count_index,
        );

        tracing::trace!(
            group = ?self.ctx.group_key_row(),
            prev_outputs = ?self.prev_outputs,
            curr_outputs = ?curr_outputs,
            change_type = ?change_type,
            "build outputs change"
        );

        let Some(change_type) = change_type else {
            return Ok((None, stats));
        };
        Ok((
            Some(match change_type {
                RecordType::Insert => {
                    let new_row = self
                        .group_key()
                        .map(GroupKey::table_row)
                        .chain(&curr_outputs)
                        .into_owned_row();
                    // Although we say the `prev_outputs` field is not used in EOWC mode, we still
                    // do the same here to keep the code simple. When it's actually running in EOWC
                    // mode, `build_outputs_change` will be called only once for each group.
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
            }),
            stats,
        ))
    }
}

/// Stats for agg state cache operations.
#[derive(Debug, Default)]
pub struct AggStateCacheStats {
    pub agg_state_cache_lookup_count: u64,
    pub agg_state_cache_miss_count: u64,
}

impl AggStateCacheStats {
    fn merge(&mut self, other: Self) {
        self.agg_state_cache_lookup_count += other.agg_state_cache_lookup_count;
        self.agg_state_cache_miss_count += other.agg_state_cache_miss_count;
    }
}
