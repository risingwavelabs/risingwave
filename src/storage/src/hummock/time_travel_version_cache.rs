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

use std::future::Future;

use moka::sync::Cache;
use risingwave_hummock_sdk::HummockEpoch;
use tokio::sync::Mutex;

use crate::hummock::local_version::pinned_version::PinnedVersion;
use crate::hummock::HummockResult;

/// A naive cache to reduce number of RPC sent to meta node.
pub struct SimpleTimeTravelVersionCache {
    inner: Mutex<SimpleTimeTravelVersionCacheInner>,
}

impl SimpleTimeTravelVersionCache {
    pub fn new() -> Self {
        Self {
            inner: Mutex::new(SimpleTimeTravelVersionCacheInner::new()),
        }
    }

    pub async fn get_or_insert(
        &self,
        epoch: HummockEpoch,
        fetch: impl Future<Output = HummockResult<PinnedVersion>>,
    ) -> HummockResult<PinnedVersion> {
        let mut guard = self.inner.lock().await;
        if let Some(v) = guard.get(&epoch) {
            return Ok(v);
        }
        let version = fetch.await?;
        guard.add(epoch, version);
        Ok(guard.get(&epoch).unwrap())
    }
}

struct SimpleTimeTravelVersionCacheInner {
    cache: Cache<HummockEpoch, PinnedVersion>,
}

impl SimpleTimeTravelVersionCacheInner {
    fn new() -> Self {
        let capacity = std::env::var("RW_HUMMOCK_TIME_TRAVEL_CACHE_SIZE")
            .unwrap_or_else(|_| "10".into())
            .parse()
            .unwrap();
        let cache = Cache::builder().max_capacity(capacity).build();
        Self { cache }
    }

    fn get(&self, epoch: &HummockEpoch) -> Option<PinnedVersion> {
        self.cache.get(epoch)
    }

    fn add(&mut self, epoch: HummockEpoch, version: PinnedVersion) {
        self.cache.insert(epoch, version);
    }
}
