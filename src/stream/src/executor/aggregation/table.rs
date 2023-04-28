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

use itertools::Itertools;
use risingwave_common::array::stream_chunk::Ops;
use risingwave_common::array::*;
use risingwave_common::buffer::Bitmap;
use risingwave_common::row::OwnedRow;
use risingwave_common::types::Datum;
use risingwave_expr::agg::{AggCall, AggKind};
use risingwave_storage::StateStore;

use super::agg_impl::AppendOnlyStreamingApproxCountDistinct;
use crate::common::table::state_table::StateTable;
use crate::executor::StreamExecutorResult;

#[async_trait::async_trait]
pub trait TableStateImpl<S: StateStore>: Send + Sync + 'static {
    async fn update_from_state_table(
        &mut self,
        state_table: &StateTable<S>,
        group_key: Option<&OwnedRow>,
    ) -> StreamExecutorResult<()>;

    async fn flush_state_if_needed(
        &self,
        state_table: &mut StateTable<S>,
        group_key: Option<&OwnedRow>,
    ) -> StreamExecutorResult<()>;

    fn apply_batch(
        &mut self,
        ops: Ops<'_>,
        visibility: Option<&Bitmap>,
        data: &[&ArrayImpl],
    ) -> StreamExecutorResult<()>;

    /// Get the output of the state. Must flush before getting output.
    fn get_output(&mut self) -> StreamExecutorResult<Datum>;
}

/// Aggregation state as a single state table whose schema is deduced by frontend and backend with
/// implicit consensus.
///
/// For example, in `single_phase_append_only_approx_count_distinct_agg`, 65536 buckets are stored
/// according to hash value, and the aggregation result is calculated from buckets when need to get
/// output.
pub struct TableState<S: StateStore> {
    /// Upstream column indices of agg arguments.
    arg_indices: Vec<usize>,

    /// The internal table state.
    inner: Box<dyn TableStateImpl<S>>,
}

impl<S: StateStore> TableState<S> {
    /// Create an instance from [`AggCall`].
    pub async fn new(
        agg_call: &AggCall,
        state_table: &StateTable<S>,
        group_key: Option<&OwnedRow>,
    ) -> StreamExecutorResult<Self> {
        let mut this = Self {
            arg_indices: agg_call.args.val_indices().to_vec(),
            inner: match agg_call.kind {
                AggKind::ApproxCountDistinct => {
                    Box::new(AppendOnlyStreamingApproxCountDistinct::new())
                }
                _ => panic!(
                    "Agg kind `{}` is not expected to have table state",
                    agg_call.kind
                ),
            },
        };
        this.inner
            .update_from_state_table(state_table, group_key)
            .await?;
        Ok(this)
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

    /// Flush in-memory state to state table if needed.
    pub async fn flush_state_if_needed(
        &self,
        state_table: &mut StateTable<S>,
        group_key: Option<&OwnedRow>,
    ) -> StreamExecutorResult<()> {
        self.inner
            .flush_state_if_needed(state_table, group_key)
            .await
    }

    /// Get the output of the state.
    pub fn get_output(&mut self) -> StreamExecutorResult<Datum> {
        self.inner.get_output()
    }
}
