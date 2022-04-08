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

use risingwave_common::error::Result;

use crate::manager::INVALID_EPOCH;
use crate::storage;
use crate::storage::{MetaStore, DEFAULT_COLUMN_FAMILY};

/// `BarrierManagerState` defines the necessary state of `GlobalBarrierManager`, this will be stored
/// persistently to meta store. Add more states when needed.
pub struct BarrierManagerState {
    pub prev_epoch: u64,
}

impl BarrierManagerState {
    pub async fn create<S>(store: &S) -> Self
    where
        S: MetaStore,
    {
        match store
            .get_cf(DEFAULT_COLUMN_FAMILY, b"barrier_manager_state")
            .await
        {
            Ok(byte_vec) => BarrierManagerState {
                prev_epoch: u64::from_be_bytes(byte_vec.as_slice().try_into().unwrap()),
            },
            Err(storage::Error::ItemNotFound(_)) => BarrierManagerState {
                prev_epoch: INVALID_EPOCH,
            },
            Err(e) => panic!("{:?}", e),
        }
    }

    pub async fn update<S>(&self, store: &S) -> Result<()>
    where
        S: MetaStore,
    {
        store
            .put_cf(
                DEFAULT_COLUMN_FAMILY,
                b"barrier_manager_state".to_vec(),
                self.prev_epoch.to_be_bytes().to_vec(),
            )
            .await
            .map_err(Into::into)
    }
}
