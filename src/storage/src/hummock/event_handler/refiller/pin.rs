// Copyright 2026 RisingWave Labs
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

use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;

use futures::future::try_join_all;
use risingwave_pb::id::TableId;
use tokio::sync::Semaphore;

use super::CacheRefillConfig;
use crate::hummock::SstableStoreRef;
use crate::hummock::pin_cache::PinCache;
use crate::hummock::pin_cache_refill::PinCacheRefillPlan;
use crate::hummock::pin_cache_refill::executor::PinCacheRefillExecutor;
use crate::hummock::table_cache_metadata::TableCacheMetadata;

/// Admits each event synchronously; the existing executor owns physical downloads.
/// There is no pending event queue or worker in the per-event integration.
pub(super) struct PinCacheRefiller {
    pub(super) executor: PinCacheRefillExecutor,
    cache: Arc<PinCache>,
    timeout: Duration,
}

impl PinCacheRefiller {
    pub fn new(
        config: &CacheRefillConfig,
        store: SstableStoreRef,
        concurrency: Arc<Semaphore>,
    ) -> Self {
        let cache = store
            .pin_cache()
            .expect("Pin refill requires a local backend")
            .clone();
        Self {
            executor: PinCacheRefillExecutor::new(store, concurrency),
            cache,
            timeout: config.timeout,
        }
    }

    /// Starts downloads now; the returned future only waits for this event's attempt.
    /// Admission and submission happen before the handler can change metadata.
    pub fn start_cache_refill(
        &self,
        plan: PinCacheRefillPlan,
        metadata: &TableCacheMetadata,
    ) -> impl Future<Output = ()> + Send + 'static + use<> {
        let downloads = plan
            .objects
            .into_values()
            .filter_map(|mut object| {
                object
                    .admitted_tables
                    .retain(|table| metadata.ownership().has_vnodes(table));
                if object.admitted_tables.is_empty()
                    || self.cache.get(object.source_sst.object_id).is_some()
                {
                    return None;
                }
                self.cache
                    .prepare_refill(object.source_sst.object_id)
                    .map(|token| self.executor.submit(&object, token))
            })
            .collect::<Vec<_>>();
        let refill = try_join_all(
            downloads
                .into_iter()
                .map(|download| async move { download.await.then_some(()).ok_or(()) }),
        );
        // Construct the timeout at submission, not when FIFO publication first polls us.
        let attempt = tokio::time::timeout(self.timeout, refill);
        async move {
            if attempt.await.is_err() {
                tracing::warn!("Pin cache refill timed out");
            }
        }
    }

    pub fn invalidate_refills(&self, changed_tables: &HashSet<TableId>) {
        // Copies may outlive their event timeout; always revoke them in the executor.
        self.executor.cancel_tables(changed_tables);
    }

    pub fn cancel_objects(
        &self,
        objects: impl IntoIterator<Item = risingwave_hummock_sdk::HummockSstableObjectId>,
    ) {
        self.executor.cancel_objects(objects);
    }
}

#[cfg(test)]
mod tests;
