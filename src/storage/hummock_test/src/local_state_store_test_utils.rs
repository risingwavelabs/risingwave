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

use std::future::Future;

use risingwave_common::util::epoch::EpochPair;
use risingwave_storage::error::StorageResult;
use risingwave_storage::store::{InitOptions, StateStoreWriteEpochControl};

pub trait LocalStateStoreTestExt: StateStoreWriteEpochControl {
    fn init_for_test(&mut self, epoch: u64) -> impl Future<Output = StorageResult<()>> + Send + '_ {
        self.init(InitOptions::new(EpochPair::new_test_epoch(epoch)))
    }

    fn init_for_test_with_prev_epoch(
        &mut self,
        epoch: u64,
        prev_epoch: u64,
    ) -> impl Future<Output = StorageResult<()>> + Send + '_ {
        self.init(InitOptions::new(EpochPair::new(epoch, prev_epoch)))
    }
}
impl<T: StateStoreWriteEpochControl> LocalStateStoreTestExt for T {}
