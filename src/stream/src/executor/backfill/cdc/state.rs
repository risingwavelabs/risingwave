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

use anyhow::anyhow;
use maplit::hashmap;
use risingwave_common::row;
use risingwave_common::row::{OwnedRow, Row};
use risingwave_common::types::{Datum, JsonbVal, ScalarImpl, ScalarRefImpl};
use risingwave_common::util::epoch::EpochPair;
use risingwave_connector::source::external::{CdcOffset, DebeziumOffset, DebeziumSourceOffset};
use risingwave_connector::source::{SplitId, SplitImpl, SplitMetaData};
use risingwave_storage::StateStore;
use serde_json::Value;

use crate::common::table::state_table::StateTable;
use crate::executor::{SourceStateTableHandler, StreamExecutorResult};

/// Depending on how the table is created, we have two scenarios for CDC Backfill:
/// 1. `CREATE TABLE xx WITH ("connector"= 'mysql-cdc', "database.name"='mydb', "table.name"='t1')`
/// In this case, the cdc backfill executor will wraps the source executor, and maintain its state
/// (a finish flag) in the source state table.
///
///
/// 2. `CREATE TABLE xx FROM source TABLE 'mydb.t1'`
/// In this case, we can have multiple Table jobs sharing a single cdc Source job.
/// The cdc backfill executor will be an instance of the `StreamScan` operator and has its own state table
/// schema: `table_id | backfill_finished | row_count | cdc_offset`
pub enum CdcBackfillStateImpl<S: StateStore> {
    SingleTable(SingleBackfillState<S>),
    MultiTable(MultiBackfillState<S>),
}

#[derive(Debug, Default)]
pub struct CdcStateRecord {
    pub current_pk_pos: Option<OwnedRow>,
    pub is_finished: bool,
    /// The last cdc offset that has been consumed by the cdc backfill executor
    pub last_cdc_offset: Option<CdcOffset>,
    pub row_count: i64,
}

impl<S: StateStore> CdcBackfillStateImpl<S> {
    pub fn init_epoch(&mut self, epoch: EpochPair) {
        match self {
            CdcBackfillStateImpl::SingleTable(state) => state.init_epoch(epoch),
            CdcBackfillStateImpl::MultiTable(state) => state.init_epoch(epoch),
        }
    }

    /// Restore the state of the corresponding split
    pub async fn restore_state(&mut self) -> StreamExecutorResult<CdcStateRecord> {
        match self {
            CdcBackfillStateImpl::SingleTable(state) => state.restore_state().await,
            CdcBackfillStateImpl::MultiTable(state) => state.restore_state().await,
        }
    }

    /// Modify the state of the corresponding split (currently only supports single split)
    pub async fn mutate_state(
        &mut self,
        current_pk_pos: Option<OwnedRow>,
        last_cdc_offset: Option<CdcOffset>,
        row_count: u64,
        is_finished: bool,
    ) -> StreamExecutorResult<()> {
        let record = CdcStateRecord {
            current_pk_pos,
            last_cdc_offset,
            row_count: row_count as _,
            is_finished,
        };
        match self {
            CdcBackfillStateImpl::SingleTable(state) => state.mutate_state(&record).await,
            CdcBackfillStateImpl::MultiTable(state) => state.mutate_state(&record).await,
        }
    }

    /// Persist the state to storage
    pub async fn commit_state(&mut self, new_epoch: EpochPair) -> StreamExecutorResult<()> {
        match self {
            CdcBackfillStateImpl::SingleTable(state) => state.commit_state(new_epoch).await,
            CdcBackfillStateImpl::MultiTable(state) => state.commit_state(new_epoch).await,
        }
    }
}

pub const BACKFILL_STATE_KEY_SUFFIX: &str = "_backfill";

pub struct MultiBackfillState<S: StateStore> {
    /// Id of the backfilling table, will be the key of the state
    split_id: String,
    state_table: StateTable<S>,

    cached_state: Vec<Datum>,
}

impl<S: StateStore> MultiBackfillState<S> {
    pub fn new(table_id: u32, state_table: StateTable<S>, state_len: usize) -> Self {
        Self {
            split_id: table_id.to_string(),
            state_table,
            cached_state: vec![None; state_len],
        }
    }

    pub fn init_epoch(&mut self, epoch: EpochPair) {
        self.state_table.init_epoch(epoch)
    }

    /// Restore the backfill state from storage
    pub async fn restore_state(&mut self) -> StreamExecutorResult<CdcStateRecord> {
        let key = Some(self.split_id.clone());
        match self
            .state_table
            .get_row(row::once(key.map(ScalarImpl::from)))
            .await?
        {
            Some(row) => {
                self.cached_state = row.into_inner().into_vec();
                let state = self.cached_state.as_slice();
                let state_len = state.len();
                // schema: | `split_id` | `pk...` | `backfill_finished` | `row_count` | `cdc_offset` |
                let cdc_offset = match state[state_len - 1] {
                    Some(ScalarImpl::Jsonb(ref jsonb)) => {
                        serde_json::from_value(jsonb.clone().take()).unwrap()
                    }
                    _ => return Err(anyhow!("invalid backfill state: cdc_offset").into()),
                };
                let row_count = match state[state_len - 2] {
                    Some(ScalarImpl::Int64(val)) => val,
                    _ => return Err(anyhow!("invalid backfill state: row_count").into()),
                };
                let is_finished = match state[state_len - 3] {
                    Some(ScalarImpl::Bool(val)) => val,
                    _ => return Err(anyhow!("invalid backfill state: backfill_finished").into()),
                };

                let current_pk_pos = state[1..state_len - 3].to_vec();
                Ok(CdcStateRecord {
                    current_pk_pos: Some(OwnedRow::new(current_pk_pos)),
                    is_finished,
                    last_cdc_offset: cdc_offset,
                    row_count,
                })
            }
            None => Ok(CdcStateRecord::default()),
        }
    }

    pub async fn mutate_state(&mut self, record: &CdcStateRecord) -> StreamExecutorResult<()> {
        // schema: | `split_id` | `pk...` | `backfill_finished` | `row_count` | `cdc_offset` |
        let state = self.cached_state.as_mut_slice();
        let split_id = Some(ScalarImpl::from(self.split_id.clone()));
        let state_len = state.len();
        state[0] = split_id.clone();
        if let Some(current_pk_pos) = &record.current_pk_pos {
            state[1..=current_pk_pos.len()].clone_from_slice(current_pk_pos.as_inner());
        }
        state[state_len - 3] = Some(record.is_finished.into());
        state[state_len - 2] = Some(record.row_count.into());
        state[state_len - 1] = record.last_cdc_offset.clone().map(|cdc_offset| {
            let json = serde_json::to_value(cdc_offset).unwrap();
            ScalarImpl::Jsonb(JsonbVal::from(json))
        });

        match self.state_table.get_row(row::once(split_id)).await? {
            Some(prev_row) => {
                self.state_table
                    .update(prev_row, self.cached_state.as_slice());
            }
            None => {
                self.state_table.insert(self.cached_state.as_slice());
            }
        }
        Ok(())
    }

    pub async fn commit_state(&mut self, new_epoch: EpochPair) -> StreamExecutorResult<()> {
        self.state_table.commit(new_epoch).await
    }
}

/// The state manager for backfilling a single table
pub struct SingleBackfillState<S: StateStore> {
    /// Stores the backfill done flag
    source_state_handler: SourceStateTableHandler<S>,
    cdc_table_id: u32,
    split_id: SplitId,
    cdc_split: SplitImpl,
}

impl<S: StateStore> SingleBackfillState<S> {
    pub fn new(
        source_state_handler: SourceStateTableHandler<S>,
        cdc_table_id: u32,
        split_id: SplitId,
        cdc_split: SplitImpl,
    ) -> Self {
        Self {
            source_state_handler,
            cdc_table_id,
            split_id,
            cdc_split,
        }
    }

    pub fn init_epoch(&mut self, epoch: EpochPair) {
        self.source_state_handler.init_epoch(epoch)
    }

    pub async fn restore_state(&self) -> StreamExecutorResult<CdcStateRecord> {
        let mut key = self.split_id.to_string();
        key.push_str(BACKFILL_STATE_KEY_SUFFIX);
        let is_finished = match self.source_state_handler.get(key.into()).await? {
            Some(row) => match row.datum_at(1) {
                Some(ScalarRefImpl::Jsonb(jsonb_ref)) => jsonb_ref.as_bool()?,
                _ => unreachable!("invalid backfill persistent state"),
            },
            None => false,
        };
        Ok(CdcStateRecord {
            is_finished,
            ..Default::default()
        })
    }

    pub async fn mutate_state(&mut self, state_item: &CdcStateRecord) -> StreamExecutorResult<()> {
        // skip if unfinished for single backfill
        if !state_item.is_finished {
            return Ok(());
        }

        // When single backfill is finished, we should persist two states:
        // 1) a finish flag to denote the backfill has done
        // 2) a consumed binlog offset to denote the last binlog offset
        // which will be committed to the state store upon next barrier.
        let mut key = self.split_id.to_string();
        key.push_str(BACKFILL_STATE_KEY_SUFFIX);
        // write backfill finished flag
        self.source_state_handler
            .set(
                key.into(),
                JsonbVal::from(Value::Bool(state_item.is_finished)),
            )
            .await?;

        if let SplitImpl::MysqlCdc(split) = &mut self.cdc_split
            && let Some(state) = split.mysql_split.as_mut()
        {
            let start_offset = state_item.last_cdc_offset.as_ref().map(|cdc_offset| {
                let source_offset = if let CdcOffset::MySql(o) = cdc_offset {
                    DebeziumSourceOffset {
                        file: Some(o.filename.clone()),
                        pos: Some(o.position),
                        ..Default::default()
                    }
                } else {
                    DebeziumSourceOffset::default()
                };

                let mut server = "RW_CDC_".to_string();
                server.push_str(self.cdc_table_id.to_string().as_str());
                DebeziumOffset {
                    source_partition: hashmap! {
                        "server".to_string() => server
                    },
                    source_offset,
                    // upstream heartbeat event would not emit to the cdc backfill executor,
                    // since we don't parse heartbeat event in the source parser.
                    is_heartbeat: false,
                }
            });

            // persist the last binlog offset into split state
            state.inner.start_offset = start_offset.map(|o| {
                let value = serde_json::to_value(o).unwrap();
                value.to_string()
            });
            state.inner.snapshot_done = true;
        }
        // write the last binlog offset that will be used upon recovery
        self.source_state_handler
            .set(self.split_id.clone(), self.cdc_split.encode_to_json())
            .await
    }

    pub async fn commit_state(&mut self, new_epoch: EpochPair) -> StreamExecutorResult<()> {
        self.source_state_handler
            .state_store
            .commit(new_epoch)
            .await
    }
}
