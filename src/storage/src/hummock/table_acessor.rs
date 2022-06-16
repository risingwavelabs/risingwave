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

use risingwave_hummock_sdk::HummockSSTableId;

use crate::hummock::sstable_store::{SstableStoreRef, TableHolder};
use crate::hummock::HummockResult;
use crate::monitor::StoreLocalStatistic;

#[async_trait::async_trait]
pub trait TableAcessor: Clone + Sync + Send {
    async fn sstable(
        &self,
        sst_id: HummockSSTableId,
        stats: &mut StoreLocalStatistic,
    ) -> HummockResult<TableHolder>;
}

#[derive(Clone)]
pub struct StorageTableAcessor {
    store: SstableStoreRef,
}

impl StorageTableAcessor {
    pub fn new(store: SstableStoreRef) -> Self {
        Self { store }
    }
}

#[async_trait::async_trait]
impl TableAcessor for StorageTableAcessor {
    async fn sstable(
        &self,
        sst_id: HummockSSTableId,
        stats: &mut StoreLocalStatistic,
    ) -> HummockResult<TableHolder> {
        self.store.load_table(sst_id, stats, true).await
    }
}
