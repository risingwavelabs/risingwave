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

use std::collections::{BTreeSet, HashMap, HashSet};
use std::panic::AssertUnwindSafe;
use std::sync::{Arc, LazyLock};
use std::time::Duration;

use futures::future::{Shared, try_join_all};
use futures::stream::FuturesUnordered;
use futures::{FutureExt, StreamExt};
use parking_lot::Mutex;
use prometheus::{IntCounterVec, register_int_counter_vec_with_registry};
use risingwave_common::bitmap::Bitmap;
use risingwave_common::monitor::GLOBAL_METRICS_REGISTRY;
use risingwave_common::util::panic::FutureCatchUnwindExt;
use risingwave_hummock_sdk::HummockSstableObjectId;
use risingwave_hummock_sdk::sstable_info::SstableInfo;
use risingwave_pb::id::TableId;
use thiserror_ext::AsReport;
use tokio::sync::{Semaphore, oneshot, watch};
use tokio::time::Instant;

use self::PinCacheRefillAttemptOutcome::{
    AlreadyPublished, CapacityRejected, NotOwned, Obsolete, Published,
};
use super::PinCacheRefillPlan;
use crate::hummock::pin_cache::{
    PinCache, PinCacheDownloadError, PinCacheReadHandle, PinCacheRefillToken,
};
use crate::hummock::refill_locality::{
    RefillOwnership, block_vnode_range, vnode_range_overlaps_bitmap,
};
use crate::hummock::{HummockError, HummockResult, Sstable, SstableStoreRef};
use crate::monitor::StoreLocalStatistic;

/// Ownership rejection can retire only the publication that was checked. A stale backend
/// token says nothing about ownership of the currently published route.
enum PinCacheRefillAttemptOutcome {
    Published,
    AlreadyPublished,
    Obsolete,
    CapacityRejected,
    NotOwned(Option<PinCacheReadHandle>),
}

async fn refill_pin_cache_object(
    store: &SstableStoreRef,
    projections: &[SstableInfo],
    ownership: &HashMap<TableId, Bitmap>,
    token: PinCacheRefillToken,
) -> Result<PinCacheRefillAttemptOutcome, PinCacheRefillError> {
    let Some(cache) = store.pin_cache() else {
        return Ok(PinCacheRefillAttemptOutcome::Obsolete);
    };
    // Work may have been revoked while waiting for the executor's permit. Skip its metadata
    // read as well as its download. Publication also rejects revocation during the download.
    if cache.prepare_refill(token.object_id()) != Some(token) {
        return Ok(PinCacheRefillAttemptOutcome::Obsolete);
    }
    let route = cache.get(token.object_id());
    if !pin_cache_object_is_owned(store, projections, ownership)
        .await
        .map_err(|error| PinCacheRefillError {
            phase: "ownership_meta",
            error,
        })?
    {
        return Ok(PinCacheRefillAttemptOutcome::NotOwned(route));
    }
    // Ownership validation may await remote metadata. Do not start a whole-SST transfer for
    // work revoked during that wait; only the final publication check belongs to the backend.
    if cache.prepare_refill(token.object_id()) != Some(token) {
        return Ok(PinCacheRefillAttemptOutcome::Obsolete);
    }
    // Check the current route after ownership validation. The captured route above is only
    // for withdrawing that specific publication if the ownership check rejects it.
    if cache.get(token.object_id()).is_some() {
        return Ok(PinCacheRefillAttemptOutcome::AlreadyPublished);
    }
    let download = match cache
        .download(
            token.object_id(),
            projections[0].file_size,
            store.store(),
            store.get_sst_data_path(token.object_id()),
        )
        .await
    {
        Ok(download) => download,
        Err(PinCacheDownloadError::CapacityRejected) => {
            return Ok(PinCacheRefillAttemptOutcome::CapacityRejected);
        }
        Err(PinCacheDownloadError::Io(error)) => {
            return Err(PinCacheRefillError {
                phase: "object_copy",
                error: error.into(),
            });
        }
    };
    Ok(if cache.publish(token, download) {
        PinCacheRefillAttemptOutcome::Published
    } else {
        PinCacheRefillAttemptOutcome::Obsolete
    })
}

struct PinCacheRefillError {
    phase: &'static str,
    error: HummockError,
}

async fn pin_cache_object_is_owned(
    store: &SstableStoreRef,
    projections: &[SstableInfo],
    ownership: &HashMap<TableId, Bitmap>,
) -> HummockResult<bool> {
    let Some(info) = projections.first() else {
        return Ok(false);
    };
    let mut stats = StoreLocalStatistic::default();
    let sst = store.sstable(info, &mut stats).await;
    stats.discard();
    let sst = sst?;
    Ok(owns_object(&sst, ownership))
}

/// The executor has already restricted ownership to this object's admitted tables.
/// A matching block admits the complete physical object.
fn owns_object(sst: &Sstable, ownership: &HashMap<TableId, Bitmap>) -> bool {
    sst.meta
        .block_metas
        .iter()
        .enumerate()
        .any(|(index, block)| {
            let table = block.table_id();
            ownership.get(&table).is_some_and(|bitmap| {
                vnode_range_overlaps_bitmap(block_vnode_range(sst, index), bitmap)
            })
        })
}

static REFILL_OUTCOMES: LazyLock<IntCounterVec> = LazyLock::new(|| {
    register_int_counter_vec_with_registry!(
        "pin_cache_refill_total",
        "Pin whole-object refill attempt outcomes",
        &["result"],
        &GLOBAL_METRICS_REGISTRY
    )
    .unwrap()
});

fn attempt_result_label<E, P>(
    result: &Result<Result<PinCacheRefillAttemptOutcome, E>, P>,
) -> &'static str {
    match result {
        Ok(Ok(Published)) => "published",
        Ok(Ok(AlreadyPublished)) => "already_published",
        Ok(Ok(CapacityRejected)) => "capacity_rejected",
        Ok(Ok(Obsolete | NotOwned(_))) => "obsolete",
        Ok(Err(_)) => "error",
        Err(_) => "panic",
    }
}

#[derive(Clone, Copy)]
enum Status {
    // Both initial attempts and retries wait here until their deadline.
    Queued(Instant),
    Running,
}

impl Status {
    fn deadline(self) -> Option<Instant> {
        match self {
            Self::Queued(at) => Some(at),
            Self::Running => None,
        }
    }
}

type Completion = Shared<oneshot::Receiver<bool>>;

struct Work {
    projections: Arc<[SstableInfo]>,
    cache_token: PinCacheRefillToken,
    admitted_tables: HashSet<TableId>,
    ownership: Arc<HashMap<TableId, Bitmap>>,
    completion: Completion,
    completion_sender: Option<oneshot::Sender<bool>>,
    attempts: u32,
    status: Status,
}

impl Work {
    // Only the first result belongs to admission tickets. Later retries cannot turn
    // a failed admission into a successful one after its version has already degraded.
    fn finish(&mut self, ready: bool) {
        if let Some(sender) = self.completion_sender.take() {
            let _ = sender.send(ready);
        }
    }

    fn reproject(
        &mut self,
        cache: &Arc<PinCache>,
        ownership: RefillOwnership<'_>,
        pinned_tables: &HashSet<TableId>,
    ) -> bool {
        let object = self.cache_token.object_id();
        // Do not revive revoked work or invalidate a publication from a new admission.
        if cache.prepare_refill(object) != Some(self.cache_token) {
            return false;
        }
        // Policy revocation is permanent for this admission. Temporary vnode changes
        // must not shrink this set, and later SET cannot restore a revoked table.
        self.admitted_tables
            .retain(|table| pinned_tables.contains(table));
        let projected: Vec<_> = self
            .admitted_tables
            .iter()
            .filter_map(|table| ownership.owned_vnodes(table).map(|bitmap| (*table, bitmap)))
            .collect();
        if projected.is_empty() {
            cache.revoke_refill(object);
            if let Some(route) = cache.get(object) {
                route.invalidate();
            }
            return false;
        }
        if projected.len() == self.ownership.len()
            && projected
                .iter()
                .all(|(table, bitmap)| self.ownership.get(table) == Some(bitmap.as_ref()))
        {
            return true;
        }
        let ownership = Arc::new(
            projected
                .into_iter()
                .map(|(table, bitmap)| (table, bitmap.into_owned()))
                .collect(),
        );
        cache.revoke_refill(object);
        let Some(cache_token) = cache.prepare_refill(object) else {
            return false;
        };
        let (sender, receiver) = oneshot::channel();
        self.cache_token = cache_token;
        self.completion = receiver.shared();
        // Dropping the previous sender fails all tickets for the superseded admission.
        self.completion_sender = Some(sender);
        self.ownership = ownership;
        self.status = Status::Queued(Instant::now());
        self.attempts = 0;
        true
    }
}

#[derive(Default)]
struct State {
    objects: HashMap<HummockSstableObjectId, Work>,
    schedule: BTreeSet<(Instant, HummockSstableObjectId)>,
    stopped: bool,
}

impl State {
    fn remove(&mut self, object: HummockSstableObjectId) -> Option<Work> {
        let work = self.objects.remove(&object)?;
        if let Some(at) = work.status.deadline() {
            self.schedule.remove(&(at, object));
        }
        Some(work)
    }

    fn insert(&mut self, object: HummockSstableObjectId, work: Work) {
        self.remove(object);
        if let Some(at) = work.status.deadline() {
            self.schedule.insert((at, object));
        }
        self.objects.insert(object, work);
    }
}

/// A bounded object executor, not a version queue. Version deadlines only drop tickets;
/// this worker continues owning open uploads and retries live-owned I/O failures.
/// Capacity rejection completes the attempt without retrying or waiting for GC.
pub(super) struct PinCacheRefillExecutor {
    // Lock order: executor state -> PinCache shard. PinCache never calls back into this lock.
    // Keep admission checks and route revocation atomic; never hold state across await.
    state: Arc<Mutex<State>>,
    wake: watch::Sender<()>,
    store: SstableStoreRef,
}

pub(crate) struct Ticket {
    // Shared receivers outlive completed work, but do not keep the executor alive.
    completions: Vec<Completion>,
}

impl Ticket {
    pub(crate) async fn wait(self) -> bool {
        try_join_all(self.completions.into_iter().map(|completion| async move {
            match completion.await {
                Ok(true) => Ok(()),
                Ok(false) | Err(_) => Err(()),
            }
        }))
        .await
        .is_ok()
    }
}

impl PinCacheRefillExecutor {
    pub(super) fn new(store: SstableStoreRef, concurrency: Arc<Semaphore>) -> Self {
        for result in [
            "published",
            "already_published",
            "capacity_rejected",
            "obsolete",
            "error",
            "panic",
        ] {
            let _ = REFILL_OUTCOMES.with_label_values(&[result]);
        }
        let state = Arc::new(Mutex::new(State::default()));
        let (wake, receiver) = watch::channel(());
        let limit = concurrency.available_permits().max(1);
        // Capture cleanup before spawning, so even an unpolled driver cancels its tickets.
        let cleanup = scopeguard::guard(state.clone(), |state| {
            let mut state = state.lock();
            state.stopped = true;
            state.objects.clear();
            state.schedule.clear();
        });
        let worker_store = store.clone();
        let worker_state = state.clone();
        tokio::spawn(async move {
            let _cleanup = cleanup;
            Self::run(worker_store, worker_state, receiver, concurrency, limit).await;
        });
        Self { state, wake, store }
    }

    pub(super) fn submit(
        &self,
        plan: PinCacheRefillPlan,
        ownership: RefillOwnership<'_>,
    ) -> Ticket {
        let mut completions = Vec::new();
        let Some(cache) = self.store.pin_cache() else {
            return Ticket { completions };
        };
        // Resolve each object's admitted tables against current ownership before locking,
        // so a large submission does not block completions while allocating maps.
        let objects = plan
            .objects
            .into_iter()
            .filter_map(|(object, projections)| {
                let mut admitted_tables: HashSet<_> = projections
                    .iter()
                    .flat_map(|info| info.table_ids.iter().copied())
                    .filter(|table| plan.admitted_tables.contains(table))
                    .collect();
                let ownership: HashMap<_, _> = admitted_tables
                    .iter()
                    .filter_map(|table| {
                        ownership
                            .owned_vnodes(table)
                            .map(|bitmap| (*table, bitmap.into_owned()))
                    })
                    .collect();
                if ownership.is_empty() {
                    return None;
                }
                admitted_tables.retain(|table| ownership.contains_key(table));
                let ownership = Arc::new(ownership);
                Some((object, projections, admitted_tables, ownership))
            })
            .collect::<Vec<_>>();
        let mut state = self.state.lock();
        if state.stopped {
            // An already cancelled receiver makes a stopped executor fail immediately.
            return Ticket {
                completions: vec![oneshot::channel().1.shared()],
            };
        }
        for (object, projections, admitted_tables, ownership) in objects {
            let Some(current_token) = cache.prepare_refill(object) else {
                continue;
            };
            if let Some(work) = state.objects.get(&object)
                && work.cache_token == current_token
                && work.projections.as_ref() == projections
                && work.ownership == ownership
            {
                // Failure is sticky for this admission attempt. A new identical ticket
                // degrades immediately while debt retries; it must not restart its deadline.
                completions.push(work.completion.clone());
                continue;
            }
            // A new ownership/projection generation must not publish an older in-flight token.
            cache.revoke_refill(object);
            let Some(cache_token) = cache.prepare_refill(object) else {
                state.remove(object);
                continue;
            };
            let (sender, receiver) = oneshot::channel();
            let completion = receiver.shared();
            state.insert(
                object,
                Work {
                    projections: projections.into(),
                    cache_token,
                    admitted_tables,
                    ownership,
                    completion: completion.clone(),
                    completion_sender: Some(sender),
                    attempts: 0,
                    status: Status::Queued(Instant::now()),
                },
            );
            completions.push(completion);
        }
        drop(state);
        self.wake.send_replace(());
        Ticket { completions }
    }

    /// Removes only the work whose backend membership has already been withdrawn.
    /// Running attempts retain their physical slots until they observe the revoked token.
    pub(super) fn remove_objects(&self, objects: &[HummockSstableObjectId]) {
        let mut state = self.state.lock();
        for &object in objects {
            state.remove(object);
        }
        drop(state);
        self.wake.send_replace(());
    }

    pub(super) fn reproject(
        &self,
        ownership: RefillOwnership<'_>,
        pinned_tables: &HashSet<TableId>,
    ) {
        let Some(cache) = self.store.pin_cache() else {
            return;
        };
        let mut state = self.state.lock();
        if state.stopped {
            return;
        }
        let State {
            objects, schedule, ..
        } = &mut *state;
        objects.retain(|object, work| {
            let old_deadline = work.status.deadline();
            let keep = work.reproject(cache, ownership, pinned_tables);
            let new_deadline = if keep { work.status.deadline() } else { None };
            if old_deadline != new_deadline {
                if let Some(at) = old_deadline {
                    schedule.remove(&(at, *object));
                }
                if let Some(at) = new_deadline {
                    schedule.insert((at, *object));
                }
            }
            keep
        });
        drop(state);
        self.wake.send_replace(());
    }

    async fn run(
        store: SstableStoreRef,
        state: Arc<Mutex<State>>,
        mut receiver: watch::Receiver<()>,
        concurrency: Arc<Semaphore>,
        limit: usize,
    ) {
        let mut tasks = FuturesUnordered::new();
        // This worker owns per-object download serialization, including across admission changes.
        // A replacement Work may already be queued while its revoked attempt is still doing I/O.
        let mut running = HashSet::new();
        loop {
            let (work, next_retry) = {
                let mut state = state.lock();
                let mut jobs = Vec::new();
                while running.len() + jobs.len() < limit {
                    let Some(&(at, object)) = state.schedule.first() else {
                        break;
                    };
                    if at > Instant::now() {
                        break;
                    }
                    state.schedule.pop_first();
                    if running.contains(&object) {
                        // Completion of the old generation re-enables this queued object.
                        continue;
                    }
                    let mut work = state.remove(object).unwrap();
                    work.status = Status::Running;
                    jobs.push((
                        object,
                        Arc::clone(&work.projections),
                        work.ownership.clone(),
                        work.cache_token,
                    ));
                    state.insert(object, work);
                }
                (jobs, state.schedule.first().map(|&(at, _)| at))
            };
            for (object, projections, ownership, cache_token) in work {
                running.insert(object);
                let store = store.clone();
                let concurrency = concurrency.clone();
                tasks.push(async move {
                    // A panicking I/O task is a failed admission, not a permanently lost ticket.
                    let result = AssertUnwindSafe(async {
                        let _permit = concurrency
                            .acquire_owned()
                            .await
                            .expect("refill concurrency stays open");
                        refill_pin_cache_object(&store, &projections, &ownership, cache_token).await
                    })
                    .rw_catch_unwind()
                    .await;
                    (object, cache_token, result)
                });
            }
            tokio::select! {
                change = receiver.changed() => if change.is_err() { break; },
                _ = tokio::time::sleep_until(next_retry.unwrap_or_else(Instant::now)), if next_retry.is_some() && running.len() < limit => {},
                Some((object, cache_token, result)) = tasks.next(), if !tasks.is_empty() => {
                    running.remove(&object);
                    REFILL_OUTCOMES
                        .with_label_values(&[attempt_result_label(&result)])
                        .inc();
                    let mut state = state.lock();
                    if state.objects.get(&object).is_some_and(|work| work.cache_token == cache_token) {
                        let mut work = state.remove(object).unwrap();
                        let retry = match result {
                            Ok(Ok(Published | AlreadyPublished | Obsolete)) => {
                                work.finish(true);
                                false
                            },
                            Ok(Ok(NotOwned(route))) => {
                                // Both the admission and the publication must still match.
                                if let Some(route) = route {
                                    route.invalidate();
                                }
                                work.finish(true);
                                false
                            },
                            Ok(Ok(CapacityRejected)) => {
                                // Cache pressure must not keep a refill admission alive. Report
                                // fallback to the version gate and let version application retire
                                // old files; a later explicit submission may attempt this object again.
                                work.finish(false);
                                false
                            },
                            Ok(Err(PinCacheRefillError { phase, error })) => {
                                tracing::warn!(
                                    object_id = object.as_raw_id(),
                                    admission = ?cache_token,
                                    object_size = work.projections[0].file_size,
                                    attempt = work.attempts + 1,
                                    phase,
                                    error = %error.as_report(),
                                    "pin refill failed; retaining retry debt"
                                );
                                true
                            },
                            Err(_) => {
                                tracing::warn!(
                                    object_id = object.as_raw_id(),
                                    admission = ?cache_token,
                                    object_size = work.projections[0].file_size,
                                    attempt = work.attempts + 1,
                                    "pin refill panicked; retaining retry debt"
                                );
                                true
                            }
                        };
                        if retry {
                            work.attempts = work.attempts.saturating_add(1);
                            work.status = Status::Queued(
                                Instant::now()
                                    + Duration::from_millis(
                                        100 * (1_u64 << work.attempts.min(8)),
                                    ),
                            );
                            work.finish(false);
                            state.insert(object, work);
                        }
                    }
                    // An old-generation transfer may have occupied this object's slot while
                    // its replacement was queued. Re-enable that replacement now.
                    if let Some(at) = state.objects.get(&object).and_then(|work| work.status.deadline()) {
                        state.schedule.insert((at, object));
                    }
                    drop(state);
                }
            }
        }
    }
}

#[cfg(test)]
mod tests;
