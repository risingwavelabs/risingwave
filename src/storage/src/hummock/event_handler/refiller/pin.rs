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

use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::{Arc, Weak};

use futures::future::try_join_all;
use parking_lot::Mutex;
use risingwave_common::bitmap::Bitmap;
use risingwave_hummock_sdk::HummockVersionId;
use risingwave_hummock_sdk::compaction_group::hummock_version_ext::SstDeltaInfo;
use risingwave_hummock_sdk::version::HummockVersionDelta;
use risingwave_pb::id::TableId;
use tokio::sync::{Notify, Semaphore, oneshot};
use tokio::task::JoinHandle;

use super::CacheRefillConfig;
use super::admission::pin::{iter_admitted_objects, load_metadata, select_owned_candidates};
use crate::hummock::SstableStoreRef;
use crate::hummock::event_handler::ReadOnlyRwLockRef;
use crate::hummock::local_version::pinned_version::PinnedVersion;
use crate::hummock::pin_cache_refill::PinCacheRefillPlan;
use crate::hummock::pin_cache_refill::executor::PinCacheRefillExecutor;
use crate::hummock::table_cache_metadata::TableCacheMetadata;

/// One unresolved event: version ID, withdrawable intent and its publication completion.
struct Request {
    version_id: HummockVersionId,
    plan: PinCacheRefillPlan,
    completion: oneshot::Sender<()>,
}

/// Prepares one event at a time independently of Foyer and version publication.
/// The executor owns admitted downloads; this worker owns metadata preparation and completion.
pub(super) struct PinCacheRefiller {
    pub(super) executor: Arc<PinCacheRefillExecutor>,
    // The selected request stays queued until its attempt settles, so policy withdrawal
    // can narrow its original intent during metadata I/O without another active-request record.
    requests: Arc<Mutex<VecDeque<Request>>>,
    wake: Arc<Notify>,
    worker: JoinHandle<()>,
}

impl Drop for PinCacheRefiller {
    fn drop(&mut self) {
        // The worker owns the last queue reference while running. Abort drops its selected
        // inputs and queued senders together; clearing here could race its completion dequeue.
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
        target: &PinnedVersion,
        version_deltas: Option<&[HummockVersionDelta]>,
        admitted_tables: &HashSet<TableId>,
    ) -> oneshot::Receiver<()> {
        let plan = PinCacheRefillPlan::from_version_update(
            deltas,
            target,
            version_deltas,
            admitted_tables,
        );
        let (completion, receiver) = oneshot::channel();
        self.requests.lock().push_back(Request {
            version_id: target.id(),
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
                requests.front().map(|request| {
                    let candidates = select_owned_candidates(
                        request.plan.objects.values().filter(|candidate| {
                            cache.get(candidate.source_sst.object_id).is_none()
                        }),
                        &metadata,
                    );
                    (request.version_id, candidates)
                })
            };
            let Some((target_id, candidates)) = selected else {
                wake.notified().await;
                continue;
            };
            let attempt = async {
                let prepared =
                    match load_metadata(&store, meta_concurrency.as_deref(), candidates).await {
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
                    // The selected request remains authoritative during preparation. RESET then
                    // SET cannot recreate tables removed from its captured plan.
                    let admitted = iter_admitted_objects(
                        prepared,
                        &metadata,
                        requests
                            .front()
                            .expect("only the worker removes requests")
                            .plan
                            .objects
                            .values()
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
            // Failure and timeout permit remote fallback. Only this worker removes requests.
            let request = requests
                .lock()
                .pop_front()
                .expect("selected request remains queued");
            let _ = request.completion.send(());
            tokio::task::yield_now().await;
        }
    }

    #[cfg(test)]
    pub(super) fn has_unfinished_requests(&self) -> bool {
        !self.requests.lock().is_empty()
    }

    // All metadata notifications run under the handler's write guard, serializing with admission.
    pub fn on_pinned_tables_changed(
        &self,
        previous: &HashSet<TableId>,
        metadata: &TableCacheMetadata,
    ) {
        let changed = previous
            .difference(metadata.pinned_tables())
            .copied()
            .collect();
        self.invalidate_refills(&changed, metadata.pinned_tables());
    }

    pub fn on_serving_mapping_changed(
        &self,
        previous: &HashMap<TableId, Bitmap>,
        metadata: &TableCacheMetadata,
    ) {
        let changed = metadata
            .pinned_tables()
            .iter()
            .copied()
            .filter(|table| {
                metadata.role.for_serving()
                    && previous.get(table).is_some_and(|bitmap| {
                        bitmap
                            .iter_ones()
                            .any(|vnode| !metadata.ownership().contains_vnode(table, vnode))
                    })
            })
            .collect();
        self.invalidate_refills(&changed, metadata.pinned_tables());
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
                    .any(|vnode| !metadata.ownership().contains_vnode(&table, vnode))
            })
        {
            self.invalidate_refills(&[table].into(), metadata.pinned_tables());
        }
    }

    /// Preparation uses current ownership at submission; policy withdrawal permanently narrows intent.
    fn invalidate_refills(
        &self,
        changed_tables: &HashSet<TableId>,
        pinned_tables: &HashSet<TableId>,
    ) {
        if changed_tables.is_empty() {
            return;
        }
        if changed_tables
            .iter()
            .any(|table| !pinned_tables.contains(table))
        {
            for request in &mut *self.requests.lock() {
                request.plan.retain_tables(pinned_tables);
            }
        }
        // Executor work can outlive an event timeout, so it is always invalidated directly.
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
