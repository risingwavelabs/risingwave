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

use std::collections::hash_map::Entry;
use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::{Arc, Weak};

use futures::future::try_join_all;
use parking_lot::Mutex;
use risingwave_common::bitmap::Bitmap;
use risingwave_common::config::streaming::CacheRefillPolicy;
use risingwave_hummock_sdk::compaction_group::hummock_version_ext::SstDeltaInfo;
use risingwave_hummock_sdk::version::HummockVersionDelta;
use risingwave_pb::id::TableId;
use tokio::sync::{Notify, Semaphore, oneshot};
use tokio::task::JoinHandle;

use super::CacheRefillConfig;
use super::admission::load_sst_metadata;
use super::admission::pin::{iter_admitted_objects, select_owned_candidates};
use crate::hummock::SstableStoreRef;
use crate::hummock::event_handler::ReadOnlyRwLockRef;
use crate::hummock::local_version::pinned_version::PinnedVersion;
use crate::hummock::pin_cache_refill::executor::PinCacheRefillExecutor;
use crate::hummock::pin_cache_refill::{PinCacheRefillObject, PinCacheRefillPlan};
use crate::hummock::table_cache_metadata::TableCacheMetadata;

/// One unresolved event: target snapshot, withdrawable intent and publication completion.
struct Request {
    target: PinnedVersion,
    plan: PinCacheRefillPlan,
    completion: oneshot::Sender<bool>,
}

/// Batches queued intent independently of version publication. The executor owns all
/// admitted downloads. This worker selects batches, prepares locality and reports endpoints.
pub(super) struct PinCacheRefiller {
    pub(super) executor: Arc<PinCacheRefillExecutor>,
    // Selected requests stay in this queue until the attempt settles. Policy withdrawal
    // can narrow their original intent during metadata I/O without a second active-batch state.
    requests: Arc<Mutex<VecDeque<Request>>>,
    wake: Arc<Notify>,
    worker: JoinHandle<()>,
}

impl Drop for PinCacheRefiller {
    fn drop(&mut self) {
        // The worker owns the last queue reference while running. Abort drops its selected
        // inputs and queued senders together; clearing here could race its fixed-prefix drain.
        self.worker.abort();
    }
}

impl PinCacheRefiller {
    pub fn new(
        config: Arc<CacheRefillConfig>,
        store: SstableStoreRef,
        metadata: ReadOnlyRwLockRef<TableCacheMetadata>,
        concurrency: Arc<Semaphore>,
        meta_concurrency: Option<Arc<Semaphore>>,
    ) -> Self {
        let executor = Arc::new(PinCacheRefillExecutor::new(store.clone(), concurrency));
        let requests = Arc::new(Mutex::new(VecDeque::new()));
        let wake = Arc::new(Notify::new());
        let worker = tokio::spawn(Self::run(
            requests.clone(),
            wake.clone(),
            metadata,
            store,
            Arc::downgrade(&executor),
            config,
            meta_concurrency,
        ));
        Self {
            executor,
            requests,
            wake,
            worker,
        }
    }

    /// Captures receipt-time admitted Pin tables and enqueues intent without downloads.
    /// The consumer selects using current CN ownership;
    /// dropping this receiver does not cancel queued or submitted work.
    pub fn start_cache_refill(
        &self,
        deltas: &[SstDeltaInfo],
        target: PinnedVersion,
        version_deltas: Option<&[HummockVersionDelta]>,
        admitted_tables: &HashSet<TableId>,
    ) -> oneshot::Receiver<bool> {
        let plan = PinCacheRefillPlan::from_version_update(
            deltas,
            &target,
            version_deltas,
            admitted_tables,
        );
        let (completion, receiver) = oneshot::channel();
        self.requests.lock().push_back(Request {
            target,
            plan,
            completion,
        });
        self.wake.notify_one();
        receiver
    }

    async fn run(
        requests: Arc<Mutex<VecDeque<Request>>>,
        wake: Arc<Notify>,
        metadata: ReadOnlyRwLockRef<TableCacheMetadata>,
        store: SstableStoreRef,
        executor: Weak<PinCacheRefillExecutor>,
        config: Arc<CacheRefillConfig>,
        meta_concurrency: Option<Arc<Semaphore>>,
    ) {
        let cache = store
            .pin_cache()
            .expect("Pin refill requires a local backend");
        loop {
            let selected = {
                let metadata = metadata.read();
                let requests = requests.lock();
                if requests.is_empty() {
                    None
                } else {
                    let count = config.pin_cache_max_batch_size.max(1).min(requests.len());
                    let target = &requests[count - 1].target;
                    let candidates = select_owned_candidates(
                        requests
                            .iter()
                            .take(count)
                            .flat_map(|request| request.plan.objects.values())
                            .filter(|candidate| {
                                cache.get(candidate.source_sst.object_id).is_none()
                            }),
                        &metadata,
                    );
                    let mut plan = merge_pin_refill_candidates(candidates);
                    // A single event was already pruned against its target at receipt.
                    if count > 1 {
                        plan.retain_in_version(target, |table| metadata.is_pinned(table));
                    }
                    Some((count, target.id(), plan))
                }
            };
            let Some((count, target_id, plan)) = selected else {
                wake.notified().await;
                continue;
            };
            let attempt = async {
                let prepared = match load_sst_metadata(
                    &store,
                    plan.objects.into_values(),
                    |candidate| &candidate.source_sst,
                    meta_concurrency.as_deref(),
                )
                .await
                {
                    Ok(prepared) => prepared,
                    Err(error) => {
                        tracing::warn!(version_id = %target_id, error = %error,
                            "Pin metadata preparation failed; using remote fallback");
                        return Err(());
                    }
                };
                if prepared.is_empty() {
                    return Ok(());
                }
                let downloads = {
                    let metadata = metadata.read();
                    let requests = requests.lock();
                    // The selected prefix remains authoritative during preparation. RESET then
                    // SET cannot recreate tables removed from these captured plans.
                    let admitted = iter_admitted_objects(
                        prepared,
                        &metadata,
                        requests
                            .iter()
                            .take(count)
                            .flat_map(|request| request.plan.objects.values())
                            .flat_map(|candidate| candidate.admitted_tables.iter().copied()),
                    );
                    let Some(executor) = executor.upgrade() else {
                        return Err(());
                    };
                    admitted
                        .filter_map(|candidate| {
                            if cache.get(candidate.source_sst.object_id).is_some() {
                                return None;
                            }
                            cache
                                .prepare_refill(candidate.source_sst.object_id)
                                .map(|token| executor.submit(&candidate, token))
                        })
                        .collect::<Vec<_>>()
                };
                try_join_all(
                    downloads
                        .into_iter()
                        .map(|download| async move { download.await.then_some(()).ok_or(()) }),
                )
                .await
                .map(|_| ())
            };
            if tokio::time::timeout(config.timeout, attempt).await.is_err() {
                tracing::warn!(version_id = %target_id, "Pin cache refill timed out");
            }
            // Keep endpoint decisions even when preparation fails or times out before submission.
            // Only this worker removes requests; metadata notifications only narrow their plans.
            for (index, request) in requests.lock().drain(..count).enumerate() {
                let _ = request.completion.send(index + 1 == count);
            }
            tokio::task::yield_now().await;
        }
    }

    #[cfg(test)]
    pub(super) fn has_unfinished_requests(&self) -> bool {
        !self.requests.lock().is_empty()
    }

    // All metadata notifications run under the handler's write guard, serializing with admission.
    pub fn on_policies_changed(
        &self,
        previous: &HashMap<TableId, CacheRefillPolicy>,
        metadata: &TableCacheMetadata,
    ) {
        let changed = previous
            .keys()
            .chain(metadata.table_cache_refill_policies.keys())
            .filter(|table| {
                previous
                    .get(*table)
                    .copied()
                    .unwrap_or(metadata.default_policy)
                    == CacheRefillPolicy::Pin
                    && !metadata.is_pinned(table)
            })
            .copied()
            .collect();
        self.invalidate_refills(&changed, metadata);
    }

    pub fn on_serving_mapping_changed(
        &self,
        previous: &HashMap<TableId, Bitmap>,
        metadata: &TableCacheMetadata,
    ) {
        let changed = previous
            .keys()
            .filter(|table| metadata.is_pinned(table))
            .copied()
            .filter(|table| {
                metadata.role.for_serving()
                    && previous.get(table).is_some_and(|bitmap| {
                        bitmap
                            .iter_ones()
                            .any(|vnode| !metadata.contains_vnode(table, vnode))
                    })
            })
            .collect();
        self.invalidate_refills(&changed, metadata);
    }

    pub fn on_streaming_vnodes_changed(
        &self,
        table: TableId,
        previous: Option<&Bitmap>,
        metadata: &TableCacheMetadata,
    ) {
        // Gaining vnodes or moving them between local lanes does not revoke admission.
        // Losing any local vnode conservatively cancels dependent object copies; no per-job
        // bitmap or metadata reload is needed to invalidate a stale locality decision.
        if metadata.is_pinned(&table)
            && metadata.role.for_streaming()
            && previous.is_some_and(|bitmap| {
                bitmap
                    .iter_ones()
                    .any(|vnode| !metadata.contains_vnode(&table, vnode))
            })
        {
            self.invalidate_refills(&[table].into(), metadata);
        }
    }

    /// Preparation uses current ownership at submission; policy withdrawal permanently narrows intent.
    fn invalidate_refills(&self, changed_tables: &HashSet<TableId>, metadata: &TableCacheMetadata) {
        if changed_tables.is_empty() {
            return;
        }
        if changed_tables
            .iter()
            .any(|table| !metadata.is_pinned(table))
        {
            for request in &mut *self.requests.lock() {
                request
                    .plan
                    .retain_tables(|table| metadata.is_pinned(table));
            }
        }
        // Executor work can outlive a batch timeout, so it is always invalidated directly.
        self.executor.cancel_tables(changed_tables);
    }

    pub fn cancel_objects(
        &self,
        objects: impl IntoIterator<Item = risingwave_hummock_sdk::HummockSstableObjectId>,
    ) {
        self.executor.cancel_objects(objects);
    }
}

// Coalesce owned candidates before pruning the selected endpoint and loading metadata.
fn merge_pin_refill_candidates(candidates: Vec<PinCacheRefillObject>) -> PinCacheRefillPlan {
    let mut plan = PinCacheRefillPlan::default();
    for candidate in candidates {
        match plan.objects.entry(candidate.source_sst.object_id) {
            Entry::Occupied(mut entry) => {
                entry
                    .get_mut()
                    .admitted_tables
                    .extend(candidate.admitted_tables);
            }
            Entry::Vacant(entry) => {
                entry.insert(candidate);
            }
        }
    }
    plan
}

#[cfg(test)]
mod tests;
