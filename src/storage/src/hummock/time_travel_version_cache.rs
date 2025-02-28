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
use std::pin::Pin;

use futures::FutureExt;
use futures::future::Shared;
use moka::sync::Cache;
use risingwave_hummock_sdk::HummockEpoch;

use crate::hummock::HummockResult;
use crate::hummock::local_version::pinned_version::PinnedVersion;

type InflightResult = Shared<Pin<Box<dyn Future<Output = HummockResult<PinnedVersion>> + Send>>>;

/// A naive cache to reduce number of RPC sent to meta node.
pub struct SimpleTimeTravelVersionCache {
    cache: Cache<(u32, HummockEpoch), InflightResult>,
}

impl SimpleTimeTravelVersionCache {
    pub fn new(capacity: u64) -> Self {
        let cache = Cache::builder().max_capacity(capacity).build();
        Self { cache }
    }

    pub async fn get_or_insert(
        &self,
        table_id: u32,
        epoch: HummockEpoch,
        fetch: impl Future<Output = HummockResult<PinnedVersion>> + Send + 'static,
    ) -> HummockResult<PinnedVersion> {
        self.cache
            .entry((table_id, epoch))
            .or_insert_with_if(
                || fetch.boxed().shared(),
                |inflight| {
                    if let Some(result) = inflight.peek() {
                        return result.is_err();
                    }
                    false
                },
            )
            .value()
            .clone()
            .await
    }
}
