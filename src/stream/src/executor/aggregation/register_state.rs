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

use itertools::Itertools;
use risingwave_common::array::stream_chunk::Ops;
use risingwave_common::array::*;
use risingwave_common::buffer::Bitmap;
use risingwave_common::types::Datum;
use risingwave_expr::expr::AggKind;
use risingwave_storage::table::streaming_table::state_table::StateTable;
use risingwave_storage::StateStore;

use super::agg_impl::AppendOnlyStreamingApproxCountDistinct;
use super::AggCall;
use crate::executor::StreamExecutorResult;

#[async_trait::async_trait]
pub trait AggRegister<S: StateStore>: Send + Sync + 'static {
    fn is_dirty(&self) -> bool;
    fn set_dirty(&mut self, flag: bool);

    async fn update_from_state_table(
        &mut self,
        state_table: &StateTable<S>,
    ) -> StreamExecutorResult<()>;

    async fn sync_state(&mut self, state_table: &mut StateTable<S>) -> StreamExecutorResult<()> {
        self.sync_state_impl(state_table).await?;
        self.set_dirty(false);
        Ok(())
    }

    async fn sync_state_impl(&self, state_table: &mut StateTable<S>) -> StreamExecutorResult<()>;

    fn apply_batch(
        &mut self,
        ops: Ops<'_>,
        visibility: Option<&Bitmap>,
        data: &[&ArrayImpl],
    ) -> StreamExecutorResult<()> {
        self.set_dirty(true);
        self.apply_batch_impl(ops, visibility, data)
    }

    fn apply_batch_impl(
        &mut self,
        ops: Ops<'_>,
        visibility: Option<&Bitmap>,
        data: &[&ArrayImpl],
    ) -> StreamExecutorResult<()>;

    /// Get the output of the state. Must flush before getting output.
    fn get_output(&mut self) -> StreamExecutorResult<Datum>;
}

/// Aggregation state as a set of registers to maintain medium result, in a standalone state table.
///
/// For example, in `single_phase_append_only_approx_count_distinct_agg`, 65536 buckets are stored
/// according to hash value, and the aggregation result is calculated from buckets when need to get
/// output.
pub struct RegisterState<S: StateStore> {
    /// Upstream column indices of agg arguments.
    arg_indices: Vec<usize>,

    /// The internal registers state.
    inner: Box<dyn AggRegister<S>>,
}

impl<S: StateStore> RegisterState<S> {
    pub fn is_dirty(&self) -> bool {
        self.inner.is_dirty()
    }

    pub async fn update_from_state_table(
        &mut self,
        state_table: &StateTable<S>,
    ) -> StreamExecutorResult<()> {
        self.inner.update_from_state_table(state_table).await
    }

    pub async fn sync_state(
        &mut self,
        state_table: &mut StateTable<S>,
    ) -> StreamExecutorResult<()> {
        self.inner.sync_state(state_table).await
    }

    /// Create an instance from [`AggCall`].
    pub fn new(agg_call: &AggCall, group_key: Option<&Row>) -> Self {
        Self {
            arg_indices: agg_call.args.val_indices().to_vec(),
            inner: match agg_call.kind {
                AggKind::SinglePhaseAppendOnlyApproxDistinct => {
                    Box::new(AppendOnlyStreamingApproxCountDistinct::new(group_key))
                }
                _ => panic!(
                    "Agg kind `{}` is not expected to have registers state",
                    agg_call.kind
                ),
            },
        }
    }

    /// Apply a chunk of data to the state.
    pub fn apply_chunk(
        &mut self,
        ops: Ops<'_>,
        visibility: Option<&Bitmap>,
        columns: &[&ArrayImpl],
    ) -> StreamExecutorResult<()> {
        let data = self
            .arg_indices
            .iter()
            .map(|col_idx| columns[*col_idx])
            .collect_vec();
        self.inner.apply_batch(ops, visibility, &data)
    }

    /// Get the output of the state.
    pub fn get_output(&mut self) -> StreamExecutorResult<Datum> {
        self.inner.get_output()
    }
}
