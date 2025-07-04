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

use anyhow::anyhow;
use risingwave_common::row;
use risingwave_common::row::OwnedRow;
use risingwave_common::types::{Datum, ScalarImpl};
use risingwave_common::util::epoch::EpochPair;
use risingwave_connector::source::cdc::external::CdcOffset;
use risingwave_storage::StateStore;

use crate::common::table::state_table::StateTable;
use crate::executor::StreamExecutorResult;

#[derive(Debug, Default)]
pub struct CdcStateRecord {
    // pub current_pk_pos: Option<OwnedRow>,
    pub is_finished: bool,
    // /// The last cdc offset that has been consumed by the cdc backfill executor
    // pub last_cdc_offset: Option<CdcOffset>,
    pub row_count: i64,
}

/// state schema: | `split_id` | `backfill_finished` | `row_count` | `cdc_offset` |
pub struct ParallelizedCdcBackfillState<S: StateStore> {
    state_table: StateTable<S>,
    cached_state: Vec<Datum>,
    state_len: usize,
}

impl<S: StateStore> ParallelizedCdcBackfillState<S> {
    pub fn new(state_table: StateTable<S>, state_len: usize) -> Self {
        Self {
            state_table,
            cached_state: vec![None; state_len],
            state_len,
        }
    }

    pub async fn init_epoch(&mut self, epoch: EpochPair) -> StreamExecutorResult<()> {
        self.state_table.init_epoch(epoch).await
    }

    /// Restore the backfill state from storage
    pub async fn restore_state(&mut self, split_id: i64) -> StreamExecutorResult<CdcStateRecord> {
        let key = Some(split_id.clone());
        match self
            .state_table
            .get_row(row::once(key.map(ScalarImpl::from)))
            .await?
        {
            Some(row) => {
                tracing::info!("restored cdc backfill state: {:?}", row);
                self.cached_state = row.into_inner().into_vec();
                let state = self.cached_state.as_slice();
                let state_len = state.len();
                // schema: | `split_id` | `backfill_finished` | `row_count` | `cdc_offset` |
                let row_count = match state[state_len - 2] {
                    Some(ScalarImpl::Int64(val)) => val,
                    _ => return Err(anyhow!("invalid backfill state: row_count").into()),
                };
                let is_finished = match state[state_len - 3] {
                    Some(ScalarImpl::Bool(val)) => val,
                    _ => return Err(anyhow!("invalid backfill state: backfill_finished").into()),
                };

                Ok(CdcStateRecord {
                    // current_pk_pos: None,
                    is_finished,
                    // last_cdc_offset: None,
                    row_count,
                })
            }
            None => {
                self.cached_state = vec![None; self.state_len];
                Ok(CdcStateRecord::default())
            }
        }
    }

    /// Modify the state of the corresponding split
    pub async fn mutate_state(
        &mut self,
        split_id: i64,
        _current_pk_pos: Option<OwnedRow>,
        _last_cdc_offset: Option<CdcOffset>,
        row_count: u64,
        is_finished: bool,
    ) -> StreamExecutorResult<()> {
        // schema: | `split_id` | `backfill_finished` | `row_count` | `cdc_offset` |
        let state = self.cached_state.as_mut_slice();
        let split_id = Some(ScalarImpl::from(split_id));
        let state_len = state.len();
        state[0].clone_from(&split_id);
        state[state_len - 3] = Some(is_finished.into());
        state[state_len - 2] = Some((row_count as i64).into());
        state[state_len - 1] = None;

        tracing::debug!(?state, "!!!mutate_state");

        match self.state_table.get_row(row::once(split_id)).await? {
            Some(prev_row) => {
                tracing::debug!(?prev_row, ?state, "!!!update");
                self.state_table
                    .update(prev_row, self.cached_state.as_slice());
            }
            None => {
                tracing::debug!(?state, "!!!insert");
                self.state_table.insert(self.cached_state.as_slice());
            }
        }
        Ok(())
    }

    /// Persist the state to storage
    pub async fn commit_state(&mut self, new_epoch: EpochPair) -> StreamExecutorResult<()> {
        tracing::debug!("!!!commit_state");
        self.state_table
            .commit_assert_no_update_vnode_bitmap(new_epoch)
            .await
    }
}
