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

use std::cmp;
use std::collections::VecDeque;
use std::ops::DerefMut;
use std::sync::Arc;

use risingwave_hummock_sdk::HummockSstableId;
use risingwave_rpc_client::HummockMetaClient;
use tokio::sync::Mutex;

use crate::hummock::{HummockError, HummockResult};

pub type SstableIdManagerRef = Arc<SstableIdManager>;

/// 1. Caches SST ids fetched from meta.
/// 2. TODO #4037: Maintains watermark SST ids. It will be used by SST full GC.
pub struct SstableIdManager {
    // Lock order: `unused_sst_ids` before `max_fetched_sst_id`
    // TODO: optimization https://github.com/singularity-data/risingwave/pull/4308#discussion_r934411543
    unused_sst_ids: Mutex<VecDeque<HummockSstableId>>,
    // Newly fetched SST id should >= `max_fetched_sst_id`,
    // in order to ensure SST id served locally is monotonic increasing.
    max_fetched_sst_id: parking_lot::Mutex<HummockSstableId>,
    remote_fetch_number: u32,
    hummock_meta_client: Arc<dyn HummockMetaClient>,
}

impl SstableIdManager {
    pub fn new(hummock_meta_client: Arc<dyn HummockMetaClient>, remote_fetch_number: u32) -> Self {
        Self {
            unused_sst_ids: Default::default(),
            remote_fetch_number,
            hummock_meta_client,
            max_fetched_sst_id: parking_lot::Mutex::new(HummockSstableId::MIN),
        }
    }

    pub async fn get_next_sst_id(&self) -> HummockResult<HummockSstableId> {
        let mut guard = self.unused_sst_ids.lock().await;
        let unused_sst_ids = guard.deref_mut();
        if unused_sst_ids.is_empty() {
            let sst_id_range = self
                .hummock_meta_client
                .get_new_sst_ids(self.remote_fetch_number)
                .await
                .map_err(HummockError::meta_error)?;

            {
                let mut max_fetched_sst_id = self.max_fetched_sst_id.lock();
                assert!(
                    *max_fetched_sst_id <= sst_id_range.start_id,
                    "SST id moves backwards"
                );
                *max_fetched_sst_id = cmp::max(*max_fetched_sst_id, sst_id_range.end_id);
            }

            unused_sst_ids.append(&mut (sst_id_range.start_id..sst_id_range.end_id).collect());
        }
        unused_sst_ids
            .pop_front()
            .ok_or_else(|| HummockError::meta_error("get_new_sst_ids returns empty result"))
    }
}
