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

use std::sync::Arc;

use futures::StreamExt;
use risingwave_common::util::iter_util::ZipEqFast;
use risingwave_hummock_sdk::HummockSstableObjectId;
use risingwave_object_store::object::{ObjectMetadataIter, ObjectResult};
use thiserror_ext::AsReport;

use super::{PinCache, PinCacheEntry, PinCacheState};
use crate::monitor::GLOBAL_PIN_CACHE_METRICS;

#[derive(PartialEq, Eq)]
pub(super) enum RecoveryState {
    Pending,
    Ready,
    Failed,
}

impl PinCacheState {
    pub(super) fn reconcile_recovered_files(&mut self) -> Vec<Arc<PinCacheEntry>> {
        if self.desired.is_none() {
            return vec![];
        }
        let mut stale = Vec::new();
        for (object_id, entry) in std::mem::take(&mut self.recovered_files) {
            if self.desired.as_ref().unwrap().get(&object_id) == Some(&entry.size)
                && !self.published.contains_key(&object_id)
                && !self.inflight.contains_key(&object_id)
            {
                self.publish(object_id, entry);
            } else {
                stale.push(entry);
            }
        }
        stale
    }
}

impl PinCache {
    pub(super) fn parse_object_id(path: &str) -> Option<HummockSstableObjectId> {
        if path.contains('/') {
            return None;
        }
        let (object_id, path_id) = path.strip_suffix(".sst")?.split_once('-')?;
        path_id.parse::<u64>().ok()?;
        object_id.parse::<u64>().ok().map(Into::into)
    }

    pub(crate) async fn wait_for_recovery(&self) {
        loop {
            let notified = self.recovery_notify.notified();
            if *self.recovery_state.read() != RecoveryState::Pending {
                return;
            }
            notified.await;
        }
    }

    pub(super) async fn recover_local_files(
        self: Arc<Self>,
        objects: ObjectResult<ObjectMetadataIter>,
    ) {
        let mut recovered_by_shard: [Vec<_>; super::PIN_CACHE_SHARDS] =
            std::array::from_fn(|_| Vec::new());
        let mut stale_objects = Vec::new();
        let mut recovery_failed = false;
        match objects {
            Ok(mut objects) => {
                while let Some(result) = objects.next().await {
                    match result {
                        Ok(metadata) => {
                            if metadata.key.is_empty() || metadata.key.ends_with('/') {
                                continue;
                            }
                            let entry = Arc::new(PinCacheEntry {
                                path: metadata.key,
                                size: metadata.total_size as u64,
                            });
                            self.gc.account_existing(&entry);
                            if let Some(object_id) = Self::parse_object_id(&entry.path) {
                                recovered_by_shard[Self::shard_index(object_id)]
                                    .push((object_id, entry));
                            } else {
                                stale_objects.push(entry);
                            }
                        }
                        Err(error) => {
                            recovery_failed = true;
                            tracing::warn!(
                                error = %error.as_report(),
                                "failed to inspect an object while recovering pin cache"
                            );
                        }
                    }
                }
            }
            Err(error) => {
                recovery_failed = true;
                tracing::warn!(
                    error = %error.as_report(),
                    "failed to list pin cache while recovering"
                );
            }
        }

        {
            let _update = self.membership_update.lock();
            for (shard, files) in self.shards.iter().zip_eq_fast(recovered_by_shard) {
                let mut state = shard.write();
                state.recovered_files.extend(files);
                stale_objects.extend(state.reconcile_recovered_files());
            }
            // New downloads may start only after the complete inventory is accounted and
            // every shard has reconciled its routes (or retained files for the initial snapshot).
            *self.recovery_state.write() = if recovery_failed {
                RecoveryState::Failed
            } else {
                RecoveryState::Ready
            };
        }
        GLOBAL_PIN_CACHE_METRICS
            .recovery_ready
            .set((!recovery_failed) as i64);
        if recovery_failed {
            GLOBAL_PIN_CACHE_METRICS.recovery_failures.inc();
        }
        self.recovery_notify.notify_waiters();
        self.gc.reclaim(stale_objects);
    }
}
