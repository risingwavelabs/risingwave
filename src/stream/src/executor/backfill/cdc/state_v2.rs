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
use risingwave_common::types::{JsonbVal, ScalarImpl};
use risingwave_common::util::epoch::EpochPair;
use risingwave_connector::source::cdc::external::CdcOffset;
use risingwave_storage::StateStore;

use crate::common::table::state_table::StateTable;
use crate::executor::StreamExecutorResult;

#[derive(Debug, Default)]
pub struct CdcStateRecord {
    pub is_finished: bool,
    #[expect(dead_code)]
    pub row_count: i64,
    pub cdc_offset_low: Option<CdcOffset>,
    pub cdc_offset_high: Option<CdcOffset>,
}

/// state schema: | `split_id` | `backfill_finished` | `row_count` | `cdc_offset_low` | `cdc_offset_high` |
/// legacy state schema: | `split_id` | `backfill_finished` | `row_count` |
pub struct ParallelizedCdcBackfillState<S: StateStore> {
    state_table: StateTable<S>,
    state_len: usize,
    is_legacy_state: bool,
}

impl<S: StateStore> ParallelizedCdcBackfillState<S> {
    pub fn new(state_table: StateTable<S>) -> Self {
        let is_legacy_state = state_table.get_data_types().len() == 3;
        let state_len = if is_legacy_state { 3 } else { 5 };
        Self {
            state_table,
            state_len,
            is_legacy_state,
        }
    }

    pub async fn init_epoch(&mut self, epoch: EpochPair) -> StreamExecutorResult<()> {
        self.state_table.init_epoch(epoch).await
    }

    /// Restore the backfill state from storage
    pub async fn restore_state(&mut self, split_id: i64) -> StreamExecutorResult<CdcStateRecord> {
        let key = Some(split_id);
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
                let (cdc_offset_low, cdc_offset_high) = if !self.is_legacy_state {
                    let cdc_offset_low = match state[3] {
                        Some(ScalarImpl::Jsonb(ref jsonb)) => {
                            serde_json::from_value(jsonb.clone().take()).unwrap()
                        }
                        None => None,
                        _ => return Err(anyhow!("invalid backfill state: cdc_offset_low").into()),
                    };
                    let cdc_offset_high = match state[4] {
                        Some(ScalarImpl::Jsonb(ref jsonb)) => {
                            serde_json::from_value(jsonb.clone().take()).unwrap()
                        }
                        None => None,
                        _ => return Err(anyhow!("invalid backfill state: cdc_offset_high").into()),
                    };
                    (cdc_offset_low, cdc_offset_high)
                } else {
                    (None, None)
                };

                Ok(CdcStateRecord {
                    is_finished,
                    row_count,
                    cdc_offset_low,
                    cdc_offset_high,
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
        cdc_offset_low: Option<CdcOffset>,
        cdc_offset_high: Option<CdcOffset>,
    ) -> StreamExecutorResult<()> {
        // schema: | `split_id` | `backfill_finished` | `row_count` | `cdc_offset_low` | `cdc_offset_high` |
        let mut state = vec![None; self.state_len];
        let split_id = Some(ScalarImpl::from(split_id));
        state[0].clone_from(&split_id);
        state[1] = Some(is_finished.into());
        state[2] = Some((row_count as i64).into());
        if !self.is_legacy_state {
            state[3] = cdc_offset_low.map(|cdc_offset| {
                let json = serde_json::to_value(cdc_offset).unwrap();
                ScalarImpl::Jsonb(JsonbVal::from(json))
            });
            state[4] = cdc_offset_high.map(|cdc_offset| {
                let json = serde_json::to_value(cdc_offset).unwrap();
                ScalarImpl::Jsonb(JsonbVal::from(json))
            });
        }

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

    pub fn is_legacy_state(&self) -> bool {
        self.is_legacy_state
    }
}
