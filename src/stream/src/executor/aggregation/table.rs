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

use std::pin::pin;

use futures::StreamExt;
use risingwave_common::array::*;
use risingwave_common::estimate_size::EstimateSize;
use risingwave_common::row;
use risingwave_common::row::RowExt;
use risingwave_common::types::Datum;
use risingwave_expr::agg::{build, AggCall, BoxedAggState};
use risingwave_storage::StateStore;

use super::GroupKey;
use crate::common::table::state_table::StateTable;
use crate::executor::StreamExecutorResult;

/// Aggregation state as a single state table whose schema is deduced by frontend and backend with
/// implicit consensus.
///
/// For example, in `single_phase_append_only_approx_count_distinct_agg`, 65536 buckets are stored
/// according to hash value, and the aggregation result is calculated from buckets when need to get
/// output.
///
/// The aggregation state can only be stored as a single value for now.
#[derive(EstimateSize)]
pub struct TableState {
    /// The internal aggregation state.
    inner: BoxedAggState,
}

impl TableState {
    /// Create an instance from [`AggCall`].
    pub async fn new(
        agg_call: &AggCall,
        state_table: &StateTable<impl StateStore>,
        group_key: Option<&GroupKey>,
    ) -> StreamExecutorResult<Self> {
        let mut this = Self {
            inner: build(agg_call)?,
        };
        this.update_from_state_table(state_table, group_key).await?;
        Ok(this)
    }

    /// Apply a chunk of data to the state.
    pub async fn apply_chunk(&mut self, chunk: &StreamChunk) -> StreamExecutorResult<()> {
        self.inner.update(chunk).await?;
        Ok(())
    }

    /// Load the state from state table.
    async fn update_from_state_table(
        &mut self,
        state_table: &StateTable<impl StateStore>,
        group_key: Option<&GroupKey>,
    ) -> StreamExecutorResult<()> {
        let state_row = {
            let mut data_iter = pin!(
                state_table
                    .iter_with_pk_prefix(group_key.map(GroupKey::table_pk), Default::default())
                    .await?
            );
            if let Some(state_row) = data_iter.next().await {
                Some(state_row?)
            } else {
                None
            }
        };
        if let Some(state_row) = state_row {
            let state = state_row[group_key.map_or(0, GroupKey::len)].clone();
            self.inner.set(state);
        }
        Ok(())
    }

    /// Flush in-memory state to state table if needed.
    pub async fn flush_state_if_needed(
        &self,
        state_table: &mut StateTable<impl StateStore>,
        group_key: Option<&GroupKey>,
    ) -> StreamExecutorResult<()> {
        let state = self.inner.get();
        let current_row = group_key.map(GroupKey::table_row).chain(row::once(state));

        let state_row = {
            let mut data_iter = pin!(
                state_table
                    .iter_with_pk_prefix(group_key.map(GroupKey::table_pk), Default::default())
                    .await?
            );
            if let Some(state_row) = data_iter.next().await {
                Some(state_row?)
            } else {
                None
            }
        };
        match state_row {
            Some(state_row) => {
                state_table.update(state_row, current_row);
            }
            None => {
                state_table.insert(current_row);
            }
        }

        Ok(())
    }

    /// Get the output of the state.
    pub fn get_output(&mut self) -> StreamExecutorResult<Datum> {
        Ok(self.inner.get())
    }
}
