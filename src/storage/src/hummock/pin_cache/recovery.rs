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
use risingwave_hummock_sdk::HummockSstableObjectId;
use risingwave_object_store::object::{ObjectMetadataIter, ObjectResult};
use thiserror_ext::AsReport;

use super::{PinCache, PinCacheEntry, PinCacheState};
use crate::monitor::GLOBAL_PIN_CACHE_METRICS;

#[derive(Default, PartialEq, Eq)]
pub(super) enum RecoveryState {
    #[default]
    Pending,
    Ready,
    Failed,
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

    pub(super) fn reconcile_recovered_files(state: &mut PinCacheState) -> Vec<Arc<PinCacheEntry>> {
        let PinCacheState {
            desired,
            published,
            published_bytes,
            inflight,
            recovered_files,
            ..
        } = state;
        let Some(desired) = desired.as_ref() else {
            return vec![];
        };
        let mut stale = Vec::new();
        for (object_id, entry) in std::mem::take(recovered_files) {
            if desired
                .get(&object_id)
                .is_some_and(|desired_size| *desired_size == entry.size)
                && !published.contains_key(&object_id)
                && !inflight.contains_key(&object_id)
            {
                *published_bytes += entry.size;
                published.insert(object_id, entry);
            } else {
                stale.push(entry);
            }
        }
        stale
    }

    pub(crate) async fn wait_for_recovery(&self) {
        loop {
            let notified = self.recovery_notify.notified();
            if self.state.read().recovery_state != RecoveryState::Pending {
                return;
            }
            notified.await;
        }
    }

    pub(super) async fn recover_local_files(
        self: Arc<Self>,
        objects: ObjectResult<ObjectMetadataIter>,
    ) {
        let mut recovered_files = Vec::new();
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
                                recovered_files.push((object_id, entry));
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

        let reconciled_stale_objects = {
            let mut state = self.state.write();
            state.recovered_files = recovered_files;
            state.recovery_state = if recovery_failed {
                RecoveryState::Failed
            } else {
                RecoveryState::Ready
            };
            Self::reconcile_recovered_files(&mut state)
        };
        {
            let state = self.state.read();
            state.report_published();
            GLOBAL_PIN_CACHE_METRICS
                .recovery_ready
                .set((state.recovery_state == RecoveryState::Ready) as i64);
        }
        if recovery_failed {
            GLOBAL_PIN_CACHE_METRICS.recovery_failures.inc();
        }
        stale_objects.extend(reconciled_stale_objects);
        self.recovery_notify.notify_waiters();
        self.gc.reclaim(stale_objects);
    }
}
