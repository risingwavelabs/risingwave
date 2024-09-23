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
use std::pin::Pin;

use ahash::HashMap;
use futures::future::Shared;
use futures::FutureExt;
use moka::sync::Cache;
use parking_lot::{Mutex, RwLock};
use risingwave_hummock_sdk::HummockEpoch;

use crate::hummock::local_version::pinned_version::PinnedVersion;
use crate::hummock::HummockResult;

type InflightRequest = Shared<Pin<Box<dyn Future<Output = HummockResult<PinnedVersion>> + Send>>>;

/// A naive cache to reduce number of RPC sent to meta node.
pub struct SimpleTimeTravelVersionCache {
    inner: RwLock<SimpleTimeTravelVersionCacheInner>,
    request_registry: Mutex<HashMap<(u32, HummockEpoch), InflightRequest>>,
}

impl SimpleTimeTravelVersionCache {
    pub fn new() -> Self {
        Self {
            inner: RwLock::new(SimpleTimeTravelVersionCacheInner::new()),
            request_registry: Default::default(),
        }
    }

    pub async fn get_or_insert(
        &self,
        table_id: u32,
        epoch: HummockEpoch,
        fetch: impl Future<Output = HummockResult<PinnedVersion>> + Send + 'static,
    ) -> HummockResult<PinnedVersion> {
        // happy path: from cache
        if let Some(v) = self.inner.read().get(table_id, epoch) {
            return Ok(v);
        }
        // slow path: from RPC
        let fut = {
            let mut requests = self.request_registry.lock();
            let inflight = requests.get(&(table_id, epoch)).cloned();
            inflight.unwrap_or_else(|| {
                let request = fetch.boxed().shared();
                requests.insert((table_id, epoch), request.clone());
                request
            })
        };
        let result = fut.await;
        if let Ok(ref v) = result {
            self.inner.write().try_insert(table_id, epoch, v);
        }
        self.request_registry.lock().remove(&(table_id, epoch));
        result
    }
}

struct SimpleTimeTravelVersionCacheInner {
    cache: Cache<(u32, HummockEpoch), PinnedVersion>,
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

    /// Tries to get the value
    fn get(&self, table_id: u32, epoch: HummockEpoch) -> Option<PinnedVersion> {
        self.cache.get(&(table_id, epoch))
    }

    /// Inserts entry if key is not present.
    fn try_insert(&mut self, table_id: u32, epoch: HummockEpoch, version: &PinnedVersion) {
        if !self.cache.contains_key(&(table_id, epoch)) {
            self.cache.insert((table_id, epoch), version.clone())
        }
    }
}
