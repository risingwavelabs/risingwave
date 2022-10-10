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

use risingwave_common::util::epoch::{Epoch, INVALID_EPOCH};

use crate::storage::{MetaStore, MetaStoreError, MetaStoreResult, DEFAULT_COLUMN_FAMILY};

/// `BarrierManagerState` defines the necessary state of `GlobalBarrierManager`, this will be stored
/// persistently to meta store. Add more states when needed.
pub struct BarrierManagerState {
    /// The last sent `prev_epoch`
    pub in_flight_prev_epoch: Epoch,
}

const BARRIER_MANAGER_STATE_KEY: &[u8] = b"barrier_manager_state";

impl BarrierManagerState {
    pub async fn create<S>(store: &S) -> Self
    where
        S: MetaStore,
    {
        let in_flight_prev_epoch = match store
            .get_cf(DEFAULT_COLUMN_FAMILY, BARRIER_MANAGER_STATE_KEY)
            .await
        {
            Ok(byte_vec) => u64::from_be_bytes(byte_vec.as_slice().try_into().unwrap()).into(),
            Err(MetaStoreError::ItemNotFound(_)) => INVALID_EPOCH.into(),
            Err(e) => panic!("{:?}", e),
        };
        Self {
            in_flight_prev_epoch,
        }
    }

    pub async fn update_inflight_prev_epoch<S>(&self, store: &S) -> MetaStoreResult<()>
    where
        S: MetaStore,
    {
        store
            .put_cf(
                DEFAULT_COLUMN_FAMILY,
                BARRIER_MANAGER_STATE_KEY.to_vec(),
                self.in_flight_prev_epoch.0.to_be_bytes().to_vec(),
            )
            .await
            .map_err(Into::into)
    }
}
