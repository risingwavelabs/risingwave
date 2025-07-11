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
use risingwave_common::types::ScalarImpl;
use risingwave_common::util::epoch::EpochPair;
use risingwave_storage::StateStore;

use crate::common::table::state_table::StateTable;
use crate::executor::StreamExecutorResult;

#[derive(Debug, Default)]
pub struct CdcStateRecord {
    pub is_finished: bool,
    #[expect(dead_code)]
    pub row_count: i64,
}

/// state schema: | `split_id` | `backfill_finished` | `row_count` |
pub struct ParallelizedCdcBackfillState<S: StateStore> {
    state_table: StateTable<S>,
    state_len: usize,
}

impl<S: StateStore> ParallelizedCdcBackfillState<S> {
    pub fn new(state_table: StateTable<S>, state_len: usize) -> Self {
        Self {
            state_table,
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
                let state = row.into_inner().into_vec();
                let is_finished = match state[1] {
                    Some(ScalarImpl::Bool(val)) => val,
                    _ => return Err(anyhow!("invalid backfill state: backfill_finished").into()),
                };
                let row_count = match state[2] {
                    Some(ScalarImpl::Int64(val)) => val,
                    _ => return Err(anyhow!("invalid backfill state: row_count").into()),
                };

                Ok(CdcStateRecord {
                    is_finished,
                    row_count,
                })
            }
            None => Ok(CdcStateRecord::default()),
        }
    }

    /// Modify the state of the corresponding split
    pub async fn mutate_state(
        &mut self,
        split_id: i64,
        is_finished: bool,
        row_count: u64,
    ) -> StreamExecutorResult<()> {
        // schema: | `split_id` | `backfill_finished` |
        let mut state = vec![None; self.state_len];
        let split_id = Some(ScalarImpl::from(split_id));
        state[0].clone_from(&split_id);
        state[1] = Some(is_finished.into());
        state[2] = Some((row_count as i64).into());
        match self.state_table.get_row(row::once(split_id)).await? {
            Some(prev_row) => {
                self.state_table.update(prev_row, state.as_slice());
            }
            None => {
                self.state_table.insert(state.as_slice());
            }
        }
        Ok(())
    }

    /// Persist the state to storage
    pub async fn commit_state(&mut self, new_epoch: EpochPair) -> StreamExecutorResult<()> {
        self.state_table
            .commit_assert_no_update_vnode_bitmap(new_epoch)
            .await
    }
}
