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

use std::collections::{BTreeMap, HashSet};
use std::sync::{Arc, Weak};
use std::time::Duration;

use log::error;
use tokio::sync::Mutex;
use tokio_retry::strategy::jitter;

use super::SchedulerError;
use crate::meta_client::FrontendMetaClient;
use crate::scheduler::plan_fragmenter::QueryId;
use crate::scheduler::SchedulerError::PinSnapshot;
use crate::scheduler::SchedulerResult;

/// Cache of hummock snapshot in meta.
pub struct HummockSnapshotManager {
    core: Mutex<HummockSnapshotManagerCore>,
    meta_client: Arc<dyn FrontendMetaClient>,
}
pub type HummockSnapshotManagerRef = Arc<HummockSnapshotManager>;

impl HummockSnapshotManager {
    pub fn new(meta_client: Arc<dyn FrontendMetaClient>) -> Self {
        Self {
            core: Mutex::new(HummockSnapshotManagerCore::new()),
            meta_client,
        }
    }

    pub async fn get_epoch(&self, query_id: QueryId) -> SchedulerResult<u64> {
        let mut core_guard = self.core.lock().await;
        let last_pinned = core_guard.last_pinned;
        core_guard
            .epoch_to_query_ids
            .get_mut(&last_pinned)
            .unwrap()
            .insert(query_id);
        Ok(core_guard.last_pinned)
    }

    pub async fn unpin_snapshot(&self, epoch: u64, query_id: &QueryId) -> SchedulerResult<()> {
        let min_epoch = async {
            // Decrease the ref count of corresponding epoch. If all last pinned epoch is dropped,
            // mark as outdated.
            let mut core_guard = self.core.lock().await;
            let query_ids = core_guard.epoch_to_query_ids.get_mut(&epoch);
            if let Some(query_ids) = query_ids {
                query_ids.remove(query_id);
            }

            // Check the min epoch, if the epoch has no query running on it, this should be the min
            // epoch to be unpin.
            let mut min_epoch = None;
            if let Some((epoch, query_ids)) = core_guard.epoch_to_query_ids.first_key_value() {
                if query_ids.is_empty() {
                    min_epoch = Some(*epoch)
                }
            }
            // Remove the epoch from the map. Need to send RPC unpin_snapshot_before with this epoch
            // later.
            if let Some(min_epoch_inner) = min_epoch.as_ref() {
                core_guard.epoch_to_query_ids.remove(min_epoch_inner);
            }

            min_epoch
        };

        let need_to_request_meta = min_epoch.await;
        if let Some(epoch_inner) = need_to_request_meta {
            let meta_client = self.meta_client.clone();
            tokio::spawn(async move {
                tracing::info!("Unpin epoch {:?} with RPC", epoch_inner);
                let handle =
                    tokio::spawn(
                        async move { meta_client.unpin_snapshot_before(epoch_inner).await },
                    );
                if let Err(join_error) = handle.await && join_error.is_panic() {
                    error!("Request meta to unpin snapshot panic {:?}!", join_error);
                } else {
                    tracing::info!("Unpin epoch RPC succeed in frontend manager {:?}", epoch_inner);
                }
            });
        }
        Ok(())
    }

    /// Pin a epoch with retry.
    ///
    /// Return:
    ///   - `Some(Ok(u64))` if success
    ///   - `Some(Err(SchedulerError::PinSnapshot))` if exceed the retry limit
    ///   - `None` if meet the break condition
    async fn pin_epoch_with_retry(
        meta_client: Arc<dyn FrontendMetaClient>,
        last_pinned: u64,
        max_retry: usize,
        break_condition: impl Fn() -> bool,
    ) -> Option<Result<u64, SchedulerError>> {
        let max_retry_interval = Duration::from_secs(10);
        let mut retry_backoff = tokio_retry::strategy::ExponentialBackoff::from_millis(10)
            .max_delay(max_retry_interval)
            .map(jitter);
        let mut retry_count = 0;
        loop {
            if retry_count > max_retry {
                break Some(Err(PinSnapshot(last_pinned, max_retry.try_into().unwrap())));
            }
            if break_condition() {
                break None;
            }
            match meta_client.pin_snapshot(last_pinned).await {
                Ok(version) => {
                    break Some(Ok(version));
                }
                Err(err) => {
                    let retry_after = retry_backoff.next().unwrap_or(max_retry_interval);
                    tracing::warn!(
                        "Failed to pin epoch {:?}. Will retry after about {} milliseconds",
                        err,
                        retry_after.as_millis()
                    );
                    tokio::time::sleep(retry_after).await;
                }
            }
            retry_count += 1;
        }
    }

    async fn start_pin_worker(
        local_snapshot_manager_weak: Weak<HummockSnapshotManager>,
        meta_client: Arc<dyn FrontendMetaClient>,
    ) {
        let min_execute_interval = Duration::from_millis(100);
        let mut min_execute_interval_tick = tokio::time::interval(min_execute_interval);
        min_execute_interval_tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            min_execute_interval_tick.tick().await;
            let local_snapshot_manager = match local_snapshot_manager_weak.upgrade() {
                None => {
                    tracing::info!("Shutdown hummock pin worker");
                    return;
                }
                Some(local_snapshot_manager) => local_snapshot_manager,
            };

            let last_epoch = local_snapshot_manager.core.lock().await.last_pinned;

            match Self::pin_epoch_with_retry(meta_client.clone(), last_epoch, usize::MAX, || {
                // Should stop when the `local_version_manager` in this thread is the only
                // strong reference to the object.
                local_snapshot_manager_weak.strong_count() == 1
            })
            .await
            {
                Some(Ok(pinned_epoch)) => {
                    let mut core_guard = local_snapshot_manager.core.lock().await;
                    if core_guard.last_pinned < pinned_epoch {
                        core_guard.last_pinned = pinned_epoch;
                    }
                }
                Some(Err(_)) => {
                    unreachable!(
                        "since the max_retry is `usize::MAX`, this should never return `Err`"
                    );
                }
                None => {
                    tracing::info!("Shutdown snapshot pin worker");
                    return;
                }
            };
        }
    }
}

#[derive(Default)]
struct HummockSnapshotManagerCore {
    last_pinned: u64,
    /// Record the query ids that pin each snapshot.
    /// Send an `unpin_snapshot` RPC when a snapshot is not pinned any more.
    epoch_to_query_ids: BTreeMap<u64, HashSet<QueryId>>,
}

impl HummockSnapshotManagerCore {
    fn new() -> Self {
        Self {
            // Initialize by setting `is_outdated` to `true`.
            ..Default::default()
        }
    }
}
