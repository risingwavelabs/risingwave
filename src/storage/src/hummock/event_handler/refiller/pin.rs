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
use std::panic::AssertUnwindSafe;
use std::sync::Arc;
use std::time::Duration;

use futures::FutureExt;
use futures::future::{AbortHandle, Abortable, BoxFuture, try_join_all};
use parking_lot::Mutex;
use risingwave_common::bitmap::Bitmap;
use risingwave_common::util::iter_util::ZipEqFast;
use risingwave_common::util::panic::FutureCatchUnwindExt;
use risingwave_hummock_sdk::compaction_group::hummock_version_ext::SstDeltaInfo;
use risingwave_hummock_sdk::version::HummockVersionDelta;
use risingwave_pb::id::TableId;
use thiserror_ext::AsReport;
use tokio::sync::{Semaphore, oneshot};
use tokio::task::JoinHandle;

use super::metadata::meta_cache_refill;
use crate::hummock::SstableStoreRef;
use crate::hummock::event_handler::ReadOnlyRwLockRef;
use crate::hummock::local_version::pinned_version::PinnedVersion;
use crate::hummock::pin_cache::PinCacheRefillToken;
use crate::hummock::pin_cache_refill::executor::PinCacheRefillExecutor;
use crate::hummock::pin_cache_refill::{PinCacheRefillObject, PinCacheRefillPlan};
use crate::hummock::refill_locality::{block_vnode_range, vnode_range_overlaps_bitmap};
use crate::hummock::table_cache_metadata::TableCacheMetadata;
use crate::monitor::GLOBAL_CACHE_REFILL_METRICS;

struct Request {
    target: PinnedVersion,
    plan: PinCacheRefillPlan,
    completion: oneshot::Sender<bool>,
}

struct RefillBatch {
    abort: AbortHandle,
    // Original tokens revoke metadata preparation before any executor Work exists.
    candidates: Arc<Vec<(PinCacheRefillObject, PinCacheRefillToken)>>,
}

#[derive(Default)]
struct PinRefillPlanStats {
    objects: u64,
    bytes: u64,
}

impl PinRefillPlanStats {
    fn add_plan(&mut self, plan: &PinCacheRefillPlan) {
        self.objects += plan.objects.len() as u64;
        self.bytes += plan
            .objects
            .values()
            .map(|object| object.source_sst.file_size)
            .sum::<u64>();
    }
}

/// Owns Pin candidate batching and execution; the handler owns policy and membership.
/// A spawned task drains queued batches independently of publication conditions, then exits.
pub(super) struct PinCacheRefiller {
    metadata: ReadOnlyRwLockRef<TableCacheMetadata>,
    pub(super) executor: Arc<PinCacheRefillExecutor>,
    state: Arc<Mutex<PinRefillState>>,
    worker: Option<JoinHandle<()>>,
}

/// Shared only with the Pin worker. Admission updates and cancellation are atomic with
/// batch startup; this lock is never held across an await or acquired by the executor.
struct PinRefillState {
    pending: VecDeque<Request>,
    active: Option<RefillBatch>,
    store: SstableStoreRef,
    executor: std::sync::Weak<PinCacheRefillExecutor>,
    meta_concurrency: Option<Arc<Semaphore>>,
    max_batch_size: usize,
    timeout: Duration,
}

impl Drop for PinCacheRefiller {
    fn drop(&mut self) {
        if let Some(worker) = &self.worker {
            worker.abort();
        }
        let mut state = self.state.lock();
        if let Some(batch) = state.active.take() {
            batch.abort.abort();
        }
        state.pending.clear();
    }
}

impl PinCacheRefiller {
    pub fn new(
        store: SstableStoreRef,
        metadata: ReadOnlyRwLockRef<TableCacheMetadata>,
        concurrency: Arc<Semaphore>,
        meta_concurrency: Option<Arc<Semaphore>>,
        max_batch_size: usize,
        timeout: Duration,
    ) -> Self {
        let executor = Arc::new(PinCacheRefillExecutor::new(store.clone(), concurrency));
        let state = Arc::new(Mutex::new(PinRefillState {
            pending: VecDeque::new(),
            active: None,
            store,
            executor: Arc::downgrade(&executor),
            meta_concurrency,
            max_batch_size: max_batch_size.max(1),
            timeout,
        }));
        Self {
            metadata,
            executor,
            state,
            worker: None,
        }
    }

    /// The caller holds the CN metadata guard through initial admission. Subsequent
    /// batches acquire the same shared read view before locking batch state.
    pub fn submit(
        &mut self,
        deltas: &[SstDeltaInfo],
        after: PinnedVersion,
        raw_deltas: Option<&[HummockVersionDelta]>,
        metadata: &TableCacheMetadata,
    ) -> impl Future<Output = bool> + Send + 'static + use<> {
        let plan = PinCacheRefillPlan::from_version_update(
            deltas,
            &after,
            raw_deltas,
            metadata.pinned_tables(),
        );
        let (completion, receiver) = oneshot::channel();
        let work = {
            let mut state = self.state.lock();
            state.pending.push_back(Request {
                target: after,
                plan,
                completion,
            });
            state.activate_pending(metadata)
        };
        if let Some(mut work) = work {
            let state = self.state.clone();
            let metadata = self.metadata.clone();
            self.worker = Some(tokio::spawn(async move {
                loop {
                    if AssertUnwindSafe(work).rw_catch_unwind().await.is_err() {
                        tracing::warn!("Pin cache refill batch panicked; using remote fallback");
                    }
                    let next = {
                        let metadata = metadata.read();
                        let mut state = state.lock();
                        state.active = None;
                        state.activate_pending(&metadata)
                    };
                    let Some(next) = next else {
                        // A later submit can start a new task. Do not touch its state again.
                        break;
                    };
                    work = next;
                    // Empty commits can finish without an I/O await. Let other work run.
                    tokio::task::yield_now().await;
                }
            }));
        }
        // Only unselected requests can lose their sender: selected batches own a completion guard.
        async move { receiver.await.unwrap_or(true) }
    }

    #[cfg(test)]
    pub(super) fn is_idle(&self) -> bool {
        let state = self.state.lock();
        state.active.is_none() && state.pending.is_empty()
    }

    /// Metadata changes revoke affected attempts and only narrow queued admission.
    pub fn on_metadata_changed(&self, tables: &HashSet<TableId>, metadata: &TableCacheMetadata) {
        if tables.is_empty() {
            return;
        }
        let mut state = self.state.lock();
        state.cancel_tables(tables);
        for request in &mut state.pending {
            request.plan.retain_tables(metadata.pinned_tables());
        }
    }

    pub fn cancel_objects(
        &self,
        objects: impl IntoIterator<Item = risingwave_hummock_sdk::HummockSstableObjectId>,
    ) {
        self.executor.cancel_objects(objects);
    }
}

impl PinRefillState {
    fn activate_pending(
        &mut self,
        metadata: &TableCacheMetadata,
    ) -> Option<BoxFuture<'static, ()>> {
        if self.active.is_some() || self.pending.is_empty() {
            return None;
        }
        let count = self.max_batch_size.min(self.pending.len());
        let target = self.pending[count - 1].target.clone();
        let last_version = target.id();
        let mut plans = Vec::with_capacity(count);
        let mut completions = Vec::with_capacity(count);
        let cache = self
            .store
            .pin_cache()
            .expect("Pin refill requires a local backend");
        for request in self.pending.drain(..count) {
            plans.push(request.plan);
            completions.push(request.completion);
        }
        let plan = merge_pin_refill_plans(plans, &target, metadata.pinned_tables());
        let candidates = Arc::new(
            plan.objects
                .into_values()
                .filter_map(|mut object| {
                    object
                        .admitted_tables
                        .retain(|table| metadata.ownership().has_vnodes(table));
                    if object.admitted_tables.is_empty()
                        || cache.get(object.source_sst.object_id).is_some()
                    {
                        return None;
                    }
                    cache
                        .prepare_refill(object.source_sst.object_id)
                        .map(|token| (object, token))
                })
                .collect::<Vec<_>>(),
        );
        let owned_vnodes = candidates
            .iter()
            .flat_map(|(object, _)| object.admitted_tables.iter().copied())
            .collect::<HashSet<_>>()
            .into_iter()
            .filter_map(|table| {
                metadata
                    .ownership()
                    .owned_vnodes(&table)
                    .map(|bitmap| (table, bitmap.into_owned()))
            })
            .collect();
        let preparation = candidates.clone();
        let store = self.store.clone();
        let meta_concurrency = self.meta_concurrency.clone();
        let executor = self.executor.clone();
        let timeout = self.timeout;
        let (abort, registration) = AbortHandle::new_pair();
        // Only the selected target is a publication endpoint, even on fallback. Build the guard
        // before the future so cancellation or panic cannot promote subsumed intermediate targets.
        let completions = scopeguard::guard(completions, move |completions| {
            for (index, completion) in completions.into_iter().enumerate() {
                let _ = completion.send(index + 1 == count);
            }
        });
        let work = async move {
            let _completions = completions;
            if matches!(
                Abortable::new(
                    tokio::time::timeout(
                        timeout,
                        refill_pin_cache(
                            store,
                            meta_concurrency,
                            preparation,
                            owned_vnodes,
                            executor
                        ),
                    ),
                    registration,
                )
                .await,
                Ok(Err(_))
            ) {
                tracing::warn!(version_id = %last_version, "Pin cache refill timed out");
            }
        }
        .boxed();
        self.active = Some(RefillBatch { abort, candidates });
        Some(work)
    }

    fn cancel_tables(&self, tables: &HashSet<TableId>) {
        if tables.is_empty() {
            return;
        }
        if let Some(batch) = &self.active {
            let cache = self.store.pin_cache().unwrap();
            let mut affected = false;
            for (object, token) in batch.candidates.iter() {
                if !object.admitted_tables.is_disjoint(tables) {
                    affected = true;
                    if cache.prepare_refill(object.source_sst.object_id) == Some(*token) {
                        cache.revoke_refill(object.source_sst.object_id);
                    }
                }
            }
            if affected {
                batch.abort.abort();
            }
        }
        if let Some(executor) = self.executor.upgrade() {
            executor.cancel_tables(tables);
        }
    }
}

// Candidate composition is independent of backend registration and download preparation.
// The caller keeps all selected events unpublished until the final target can be published.
fn merge_pin_refill_plans(
    plans: Vec<PinCacheRefillPlan>,
    target: &PinnedVersion,
    pinned_tables: &HashSet<TableId>,
) -> PinCacheRefillPlan {
    let mut pin_plan = PinCacheRefillPlan::default();
    let mut input_pin_plan = PinRefillPlanStats::default();
    let mut retained_pin_plan = PinRefillPlanStats::default();
    for mut plan in plans {
        input_pin_plan.add_plan(&plan);
        plan.retain_tables(pinned_tables);
        retained_pin_plan.add_plan(&plan);
        for (object, object_plan) in plan.objects {
            match pin_plan.objects.entry(object) {
                Entry::Occupied(mut entry) => {
                    entry
                        .get_mut()
                        .admitted_tables
                        .extend(object_plan.admitted_tables);
                }
                Entry::Vacant(entry) => {
                    entry.insert(object_plan);
                }
            }
        }
    }
    pin_plan.retain_in_version(target, pinned_tables);
    // Input counts event-scoped candidates; retained reflects the current pin policy.
    // Merged counts unique physical candidates before metadata admission, not download jobs.
    let mut merged_pin_plan = PinRefillPlanStats::default();
    merged_pin_plan.add_plan(&pin_plan);
    for (stage, stats) in [
        ("input", &input_pin_plan),
        ("retained", &retained_pin_plan),
        ("merged", &merged_pin_plan),
    ] {
        GLOBAL_CACHE_REFILL_METRICS
            .refill_total
            .with_label_values(&["pin_plan", stage])
            .inc_by(stats.objects);
        GLOBAL_CACHE_REFILL_METRICS
            .refill_bytes
            .with_label_values(&["pin_plan", stage])
            .inc_by(stats.bytes);
    }
    pin_plan
}

// Metadata preparation owns one batch context; only admitted objects become download jobs.
async fn refill_pin_cache(
    store: SstableStoreRef,
    meta_concurrency: Option<Arc<Semaphore>>,
    candidates: Arc<Vec<(PinCacheRefillObject, PinCacheRefillToken)>>,
    ownership: HashMap<TableId, Bitmap>,
    executor: std::sync::Weak<PinCacheRefillExecutor>,
) -> Result<(), ()> {
    let holders = meta_cache_refill(
        &store,
        meta_concurrency.as_deref(),
        candidates.iter().map(|(object, _)| &object.source_sst),
    )
    .await
    .map_err(|error| {
        tracing::warn!(error = %error.as_report(), "pin refill metadata failed");
    })?;
    let mut completions = Vec::new();
    for ((object, token), sst) in candidates.iter().zip_eq_fast(holders) {
        let owned = sst
            .meta
            .block_metas
            .iter()
            .enumerate()
            .any(|(index, block)| {
                let table = block.table_id();
                object.admitted_tables.contains(&table)
                    && ownership.get(&table).is_some_and(|bitmap| {
                        vnode_range_overlaps_bitmap(block_vnode_range(&sst, index), bitmap)
                    })
            });
        drop(sst);
        if owned {
            // A preparation waiter must not keep the executor owner alive.
            let executor = executor.upgrade().ok_or(())?;
            completions.push(executor.submit(object, *token));
        }
    }
    // Admission is complete; downloads retain table IDs, not the bitmap context.
    drop(ownership);
    try_join_all(
        completions
            .into_iter()
            .map(|completion| async move { completion.await.then_some(()).ok_or(()) }),
    )
    .await
    .map(|_| ())
}

#[cfg(test)]
mod tests;
