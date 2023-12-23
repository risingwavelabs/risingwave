// Copyright 2024 RisingWave Labs
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

use risingwave_common::row::{OwnedRow, Row};
use risingwave_common::types::{JsonbVal, ScalarImpl, ScalarRef, ScalarRefImpl};
use risingwave_common::util::epoch::EpochPair;
use risingwave_common::{bail, row};
use risingwave_connector::source::{SplitImpl, SplitMetaData};
use risingwave_pb::catalog::PbTable;
use risingwave_storage::StateStore;

use super::kafka_backfill_executor::{BackfillState, BackfillStates, SplitId};
use crate::common::table::state_table::StateTable;
use crate::executor::error::StreamExecutorError;
use crate::executor::StreamExecutorResult;

pub struct BackfillStateTableHandler<S: StateStore> {
    pub state_store: StateTable<S>,
}

impl<S: StateStore> BackfillStateTableHandler<S> {
    pub async fn from_table_catalog(table_catalog: &PbTable, store: S) -> Self {

        Self {
            state_store: StateTable::from_table_catalog(table_catalog, store, None).await,
        }
    }

    pub fn init_epoch(&mut self, epoch: EpochPair) {
        self.state_store.init_epoch(epoch);
    }

    fn string_to_scalar(rhs: impl Into<String>) -> ScalarImpl {
        ScalarImpl::Utf8(rhs.into().into_boxed_str())
    }

    pub(crate) async fn get(&self, key: &SplitId) -> StreamExecutorResult<Option<OwnedRow>> {
        self.state_store
            .get_row(row::once(Some(Self::string_to_scalar(key))))
            .await
            .map_err(StreamExecutorError::from)
    }

    pub async fn set(&mut self, key: SplitId, value: JsonbVal) -> StreamExecutorResult<()> {
        let row = [
            Some(Self::string_to_scalar(&key)),
            Some(ScalarImpl::Jsonb(value)),
        ];
        match self.get(&key).await? {
            Some(prev_row) => {
                self.state_store.update(prev_row, row);
            }
            None => {
                self.state_store.insert(row);
            }
        }
        Ok(())
    }

    pub async fn delete(&mut self, key: &SplitId) -> StreamExecutorResult<()> {
        if let Some(prev_row) = self.get(key).await? {
            self.state_store.delete(prev_row);
        }

        Ok(())
    }

    pub async fn set_states(&mut self, states: BackfillStates) -> StreamExecutorResult<()> {
        if states.is_empty() {
            bail!("states require not null");
        } else {
            for (split_id, state) in states {
                self.set(split_id, state.encode_to_json()).await?;
            }
        }
        Ok(())
    }

    /// `None` means no need to read from the split anymore (backfill finished)
    pub async fn try_recover_from_state_store(
        &mut self,
        mut stream_source_split: SplitImpl,
    ) -> StreamExecutorResult<(Option<SplitImpl>, BackfillState)> {
        Ok(
            match self.get(&stream_source_split.id().to_string()).await? {
                None => (Some(stream_source_split), BackfillState::Backfilling(None)),
                Some(row) => match row.datum_at(1) {
                    Some(ScalarRefImpl::Jsonb(jsonb_ref)) => {
                        let state = BackfillState::restore_from_json(jsonb_ref.to_owned_scalar())?;
                        let new_split = match &state {
                            BackfillState::Backfilling(None) => Some(stream_source_split),
                            BackfillState::Backfilling(Some(offset)) => {
                                stream_source_split.update_in_place(offset.clone())?;
                                Some(stream_source_split)
                            }
                            BackfillState::SourceCachingUp(_) => None,
                            BackfillState::Finished => None,
                        };
                        (new_split, state)
                    }
                    _ => unreachable!(),
                },
            },
        )
    }
}
