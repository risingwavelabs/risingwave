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
use prometheus::{
    Histogram, HistogramVec, IntCounterVec, IntGaugeVec, exponential_buckets, histogram_opts,
    register_histogram_vec_with_registry, register_histogram_with_registry,
    register_int_counter_vec_with_registry, register_int_gauge_vec_with_registry,
};
use risingwave_common::bitmap::Bitmap;
use risingwave_common::monitor::GLOBAL_METRICS_REGISTRY;
use risingwave_common::util::panic::FutureCatchUnwindExt;
use risingwave_hummock_sdk::HummockSstableObjectId;
use risingwave_hummock_sdk::sstable_info::SstableInfo;
use risingwave_pb::id::TableId;
use thiserror_ext::AsReport;
use tokio::sync::{Semaphore, oneshot, watch};
use tokio::time::Instant;

use super::PinCacheRefillAttemptOutcome::{
    AlreadyPublished, CapacityRejected, NotOwned, Obsolete, Published,
};
use super::{
    PinCacheRefillAttemptOutcome, PinCacheRefillError, PinCacheRefillPlan, refill_pin_cache_object,
};
use crate::hummock::SstableStoreRef;
use crate::hummock::pin_cache::{PinCache, PinCacheRefillToken};

static REFILL_OUTCOMES: LazyLock<IntCounterVec> = LazyLock::new(|| {
    register_int_counter_vec_with_registry!(
        "pin_cache_refill_total",
        "Pin whole-object refill attempt outcomes",
        &["result"],
        &GLOBAL_METRICS_REGISTRY
    )
    .unwrap()
});

static REFILL_ATTEMPT_DURATION: LazyLock<HistogramVec> = LazyLock::new(|| {
    register_histogram_vec_with_registry!(
        histogram_opts!(
            "pin_cache_refill_attempt_duration_seconds",
            "End-to-end duration of one Pin whole-object refill attempt",
            exponential_buckets(0.01, 2.0, 17).unwrap(),
        ),
        &["result"],
        &GLOBAL_METRICS_REGISTRY
    )
    .unwrap()
});

static REFILL_ATTEMPT_BYTES: LazyLock<IntCounterVec> = LazyLock::new(|| {
    register_int_counter_vec_with_registry!(
        "pin_cache_refill_attempt_object_bytes",
        "Planned object bytes of Pin refill attempts by result",
        &["result"],
        &GLOBAL_METRICS_REGISTRY
    )
    .unwrap()
});

static REFILL_PERMIT_WAIT_DURATION: LazyLock<Histogram> = LazyLock::new(|| {
    register_histogram_with_registry!(
        histogram_opts!(
            "pin_cache_refill_permit_wait_duration_seconds",
            "Time a Pin whole-object refill attempt waits for shared refill concurrency",
            exponential_buckets(0.001, 2.0, 18).unwrap(),
        ),
        &GLOBAL_METRICS_REGISTRY
    )
    .unwrap()
});

static REFILL_FAILURES: LazyLock<IntCounterVec> = LazyLock::new(|| {
    register_int_counter_vec_with_registry!(
        "pin_cache_refill_failure_total",
        "Pin whole-object refill failures by phase",
        &["phase"],
        &GLOBAL_METRICS_REGISTRY
    )
    .unwrap()
});

static BACKLOG: LazyLock<[IntGaugeVec; 2]> = LazyLock::new(|| {
    ["objects", "bytes"].map(|unit| {
        register_int_gauge_vec_with_registry!(
            format!("pin_cache_refill_{unit}"),
            "Unfinished Pin refill work by state",
            &["state"],
            &GLOBAL_METRICS_REGISTRY
        )
        .unwrap()
    })
});

fn report_backlog(current: [[i64; 2]; 3], previous: &mut [[i64; 2]; 3]) {
    for (index, label) in ["pending", "inflight", "debt"].into_iter().enumerate() {
        for unit in 0..2 {
            BACKLOG[unit]
                .with_label_values(&[label])
                .add(current[index][unit] - previous[index][unit]);
        }
    }
    *previous = current;
}

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

struct RefillAttemptGuard {
    token: PinCacheRefillToken,
    object_size: u64,
    started_at: Instant,
    finished: bool,
}

impl RefillAttemptGuard {
    fn new(token: PinCacheRefillToken, object_size: u64) -> Self {
        Self {
            token,
            object_size,
            started_at: Instant::now(),
            finished: false,
        }
    }

    fn finish(&mut self, result: &'static str) -> Duration {
        let elapsed = self.started_at.elapsed();
        REFILL_ATTEMPT_DURATION
            .with_label_values(&[result])
            .observe(elapsed.as_secs_f64());
        REFILL_ATTEMPT_BYTES
            .with_label_values(&[result])
            .inc_by(self.object_size);
        self.finished = true;
        elapsed
    }
}

impl Drop for RefillAttemptGuard {
    fn drop(&mut self) {
        if self.finished {
            return;
        }
        let elapsed = self.started_at.elapsed();
        REFILL_OUTCOMES.with_label_values(&["cancelled"]).inc();
        REFILL_ATTEMPT_DURATION
            .with_label_values(&["cancelled"])
            .observe(elapsed.as_secs_f64());
        REFILL_ATTEMPT_BYTES
            .with_label_values(&["cancelled"])
            .inc_by(self.object_size);
        tracing::warn!(
            object_id = self.token.object_id().as_raw_id(),
            admission = ?self.token,
            object_size = self.object_size,
            elapsed_ms = elapsed.as_millis(),
            "pin refill attempt was cancelled before completion"
        );
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
        ownership: &HashMap<TableId, Bitmap>,
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
        let projected = self
            .admitted_tables
            .iter()
            .filter_map(|table| ownership.get_key_value(table));
        let count = projected.clone().count();
        if count == 0 {
            cache.revoke_refill(object);
            if let Some(route) = cache.get(object) {
                route.invalidate();
            }
            return false;
        }
        if count == self.ownership.len()
            && projected
                .clone()
                .all(|(table, bitmap)| self.ownership.get(table) == Some(bitmap))
        {
            return true;
        }
        let ownership = Arc::new(
            projected
                .map(|(&table, bitmap)| (table, bitmap.clone()))
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

    // Retry debt is an observation of queued work, not a separate scheduling state.
    fn backlog_index(&self) -> usize {
        match self.status {
            Status::Queued(_) if self.attempts == 0 => 0,
            Status::Running => 1,
            Status::Queued(_) => 2,
        }
    }
}

#[derive(Default)]
struct State {
    objects: HashMap<HummockSstableObjectId, Work>,
    schedule: BTreeSet<(Instant, HummockSstableObjectId)>,
    backlog: [[i64; 2]; 3],
    stopped: bool,
}

impl State {
    fn remove(&mut self, object: HummockSstableObjectId) -> Option<Work> {
        let work = self.objects.remove(&object)?;
        if let Some(at) = work.status.deadline() {
            self.schedule.remove(&(at, object));
        }
        self.backlog[work.backlog_index()][0] -= 1;
        self.backlog[work.backlog_index()][1] -= work.projections[0].file_size as i64;
        Some(work)
    }

    fn insert(&mut self, object: HummockSstableObjectId, work: Work) {
        self.remove(object);
        if let Some(at) = work.status.deadline() {
            self.schedule.insert((at, object));
        }
        self.backlog[work.backlog_index()][0] += 1;
        self.backlog[work.backlog_index()][1] += work.projections[0].file_size as i64;
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
            "cancelled",
        ] {
            let _ = REFILL_OUTCOMES.with_label_values(&[result]);
            let _ = REFILL_ATTEMPT_DURATION.with_label_values(&[result]);
            let _ = REFILL_ATTEMPT_BYTES.with_label_values(&[result]);
        }
        for phase in ["ownership_meta", "object_copy", "panic"] {
            let _ = REFILL_FAILURES.with_label_values(&[phase]);
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
            state.backlog = [[0; 2]; 3];
        });
        let worker_store = store.clone();
        let worker_state = state.clone();
        tokio::spawn(async move {
            let _cleanup = cleanup;
            Self::run(worker_store, worker_state, receiver, concurrency, limit).await;
        });
        Self { state, wake, store }
    }

    pub(super) fn submit(&self, plan: PinCacheRefillPlan) -> Ticket {
        let mut completions = Vec::new();
        let Some(cache) = self.store.pin_cache() else {
            return Ticket { completions };
        };
        // Ownership projections depend only on the immutable plan. Build them before locking
        // the executor so a large submission does not block completions while allocating maps.
        let objects = plan
            .objects
            .into_iter()
            .map(|(object, projections)| {
                let admitted_tables: HashSet<_> = projections
                    .iter()
                    .flat_map(|info| info.table_ids.iter().copied())
                    .filter(|table| plan.ownership.contains_key(table))
                    .collect();
                let ownership = Arc::new(
                    plan.ownership
                        .iter()
                        .filter(|(table, _)| admitted_tables.contains(*table))
                        .map(|(&table, bitmap)| (table, bitmap.clone()))
                        .collect::<HashMap<_, _>>(),
                );
                (object, projections, admitted_tables, ownership)
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
        ownership: &HashMap<TableId, Bitmap>,
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
            objects,
            schedule,
            backlog,
            ..
        } = &mut *state;
        objects.retain(|object, work| {
            let old_deadline = work.status.deadline();
            let old_bucket = work.backlog_index();
            let bytes = work.projections[0].file_size as i64;
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
            let new_bucket = keep.then(|| work.backlog_index());
            if Some(old_bucket) != new_bucket {
                backlog[old_bucket][0] -= 1;
                backlog[old_bucket][1] -= bytes;
                if let Some(bucket) = new_bucket {
                    backlog[bucket][0] += 1;
                    backlog[bucket][1] += bytes;
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
        let mut running = HashMap::new();
        let mut backlog = scopeguard::guard([[0; 2]; 3], |mut previous| {
            report_backlog([[0; 2]; 3], &mut previous)
        });
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
                    if running.contains_key(&object) {
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
                let mut current = state.backlog;
                // Superseded uploads still occupy slots even when their replacement is queued.
                current[1] = [
                    (running.len() + jobs.len()) as i64,
                    running.values().copied().sum::<u64>() as i64
                        + jobs
                            .iter()
                            .map(|(_, infos, _, _)| infos[0].file_size as i64)
                            .sum::<i64>(),
                ];
                report_backlog(current, &mut backlog);
                (jobs, state.schedule.first().map(|&(at, _)| at))
            };
            for (object, projections, ownership, cache_token) in work {
                let object_size = projections[0].file_size;
                running.insert(object, object_size);
                let store = store.clone();
                let concurrency = concurrency.clone();
                tasks.push(async move {
                    let mut attempt = RefillAttemptGuard::new(cache_token, object_size);
                    // A panicking I/O task is a failed admission, not a permanently lost ticket.
                    let result = AssertUnwindSafe(async {
                        let permit_wait_started_at = Instant::now();
                        let _permit = concurrency
                            .acquire_owned()
                            .await
                            .expect("refill concurrency stays open");
                        REFILL_PERMIT_WAIT_DURATION
                            .observe(permit_wait_started_at.elapsed().as_secs_f64());
                        refill_pin_cache_object(&store, &projections, &ownership, cache_token).await
                    })
                    .rw_catch_unwind()
                    .await;
                    let elapsed = attempt.finish(attempt_result_label(&result));
                    (object, cache_token, result, elapsed)
                });
            }
            tokio::select! {
                change = receiver.changed() => if change.is_err() { break; },
                _ = tokio::time::sleep_until(next_retry.unwrap_or_else(Instant::now)), if next_retry.is_some() && running.len() < limit => {},
                Some((object, cache_token, result, elapsed)) = tasks.next(), if !tasks.is_empty() => {
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
                                REFILL_FAILURES.with_label_values(&[phase]).inc();
                                tracing::warn!(
                                    object_id = object.as_raw_id(),
                                    admission = ?cache_token,
                                    object_size = work.projections[0].file_size,
                                    attempt = work.attempts + 1,
                                    elapsed_ms = elapsed.as_millis(),
                                    phase,
                                    error = %error.as_report(),
                                    "pin refill failed; retaining retry debt"
                                );
                                true
                            },
                            Err(_) => {
                                REFILL_FAILURES.with_label_values(&["panic"]).inc();
                                tracing::warn!(
                                    object_id = object.as_raw_id(),
                                    admission = ?cache_token,
                                    object_size = work.projections[0].file_size,
                                    attempt = work.attempts + 1,
                                    elapsed_ms = elapsed.as_millis(),
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
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::task::{Context, Poll};

    use futures::task::{ArcWake, waker_ref};
    use risingwave_common::hash::VirtualNode;
    use risingwave_common::util::epoch::test_epoch;
    use risingwave_hummock_sdk::key::{FullKey, TableKey};

    use super::*;
    use crate::hummock::iterator::test_utils::{iterator_test_table_key_of, mock_sstable_store};
    use crate::hummock::pin_cache_refill::PinCacheRefillController;
    use crate::hummock::pin_cache_refill::tests::version_with_ssts;
    use crate::hummock::test_utils::{default_builder_opt_for_test, gen_test_sstable};
    use crate::hummock::value::HummockValue;

    #[tokio::test]
    async fn test_policy_reset_cannot_revive_shared_object_admission_but_vnode_changes_can() {
        for (reset_policy, migrate_first) in [(false, false), (true, false), (true, true)] {
            let tables = [TableId::from(233), TableId::from(234)];
            let store = mock_sstable_store().await;
            let (_, info) = crate::hummock::test_utils::gen_test_sstable_with_table_ids(
                default_builder_opt_for_test(),
                874,
                tables.into_iter().map(|table| {
                    (
                        FullKey::new(
                            table,
                            TableKey(iterator_test_table_key_of(0)),
                            test_epoch(1),
                        ),
                        HummockValue::put(vec![1]),
                    )
                }),
                store.clone(),
                tables.map(|table| table.as_raw_id()).to_vec(),
            )
            .await;
            let object = info.object_id;
            let cache = PinCache::new(mock_sstable_store().await.store(), u64::MAX, 1, 2, [])
                .await
                .unwrap();
            let store = Arc::new(
                Arc::into_inner(store)
                    .unwrap()
                    .with_pin_cache(cache.clone()),
            );
            let version = version_with_ssts(std::slice::from_ref(&info));
            let concurrency = Arc::new(Semaphore::new(1));
            let mut controller =
                PinCacheRefillController::new(store, version.clone(), concurrency.clone());
            let permit = concurrency.acquire().await.unwrap();
            let ownership = HashMap::from([
                (tables[0], Bitmap::ones(VirtualNode::COUNT_FOR_TEST)),
                (
                    tables[1],
                    Bitmap::from_indices(VirtualNode::COUNT_FOR_TEST, [255]),
                ),
            ]);
            let changes =
                controller.replace_pinned_tables(tables.into(), std::slice::from_ref(&version));
            cache.register_objects(changes.inserted);
            controller.unregister_objects(changes.removed);
            controller.update_ownership(ownership.clone());
            let ticket = controller.submit(PinCacheRefillPlan {
                objects: [(object, vec![info])].into(),
                ownership: Arc::new(ownership.clone()),
            });
            tokio::time::timeout(Duration::from_secs(1), async {
                while !matches!(
                    controller.executor.state.lock().objects[&object].status,
                    Status::Running
                ) {
                    tokio::task::yield_now().await;
                }
            })
            .await
            .unwrap();

            // B keeps the physical object and its work alive, but none of O's B blocks are local.
            let without_a = HashMap::from([(tables[1], ownership[&tables[1]].clone())]);
            if migrate_first {
                // RESET must also take effect when ownership is unchanged by the policy update.
                controller.update_ownership(without_a.clone());
            }
            if reset_policy {
                let changes = controller
                    .replace_pinned_tables([tables[1]].into(), std::slice::from_ref(&version));
                cache.register_objects(changes.inserted);
                controller.unregister_objects(changes.removed);
            }
            controller.update_ownership(without_a);
            if reset_policy {
                let changes =
                    controller.replace_pinned_tables(tables.into(), std::slice::from_ref(&version));
                cache.register_objects(changes.inserted);
                controller.unregister_objects(changes.removed);
            }
            controller.update_ownership(ownership);
            assert!(cache.is_registered(object));
            assert!(
                !tokio::time::timeout(Duration::from_secs(1), ticket.wait())
                    .await
                    .unwrap()
            );
            drop(permit);
            tokio::time::timeout(Duration::from_secs(2), async {
                while !controller.executor.state.lock().objects.is_empty() {
                    tokio::task::yield_now().await;
                }
            })
            .await
            .unwrap();
            assert_eq!(
                cache.get(object).is_some(),
                !reset_policy,
                "only vnode changes may reuse an admitted table; SET cannot undo RESET"
            );
        }
    }

    #[tokio::test]
    async fn test_cached_object_skips_download_but_still_checks_ownership() {
        let store = mock_sstable_store().await;
        let (_, info) = gen_test_sstable(
            default_builder_opt_for_test(),
            873,
            std::iter::once((
                FullKey::new(
                    TableId::default(),
                    TableKey(iterator_test_table_key_of(0)),
                    test_epoch(1),
                ),
                HummockValue::put(vec![1]),
            )),
            store.clone(),
        )
        .await;
        let object = info.object_id;
        // Exactly one file fits: a second download would return CapacityRejected.
        let cache = PinCache::new(
            mock_sstable_store().await.store(),
            info.file_size,
            1,
            2,
            [(object, info.file_size)],
        )
        .await
        .unwrap();
        let store = Arc::new(
            Arc::into_inner(store)
                .unwrap()
                .with_pin_cache(cache.clone()),
        );
        let token = cache.prepare_refill(object).unwrap();
        let ownership = [(
            TableId::default(),
            Bitmap::ones(VirtualNode::COUNT_FOR_TEST),
        )]
        .into();
        let projections = [info];
        assert!(matches!(
            refill_pin_cache_object(&store, &projections, &ownership, token).await,
            Ok(Published)
        ));
        assert!(matches!(
            refill_pin_cache_object(&store, &projections, &ownership, token).await,
            Ok(AlreadyPublished)
        ));
        assert!(matches!(
            refill_pin_cache_object(&store, &projections, &HashMap::new(), token).await,
            Ok(NotOwned(Some(_)))
        ));
    }

    #[tokio::test]
    async fn test_capacity_rejection_finishes_without_retry_debt() {
        let store = mock_sstable_store().await;
        let (_, info) = gen_test_sstable(
            default_builder_opt_for_test(),
            874,
            std::iter::once((
                FullKey::new(
                    TableId::default(),
                    TableKey(iterator_test_table_key_of(0)),
                    test_epoch(1),
                ),
                HummockValue::put(vec![1]),
            )),
            store.clone(),
        )
        .await;
        let object = info.object_id;
        let cache = PinCache::new(
            mock_sstable_store().await.store(),
            info.file_size - 1,
            1,
            2,
            [(object, info.file_size)],
        )
        .await
        .unwrap();
        let store = Arc::new(
            Arc::into_inner(store)
                .unwrap()
                .with_pin_cache(cache.clone()),
        );
        let executor = PinCacheRefillExecutor::new(store, Arc::new(Semaphore::new(1)));
        let ticket = executor.submit(PinCacheRefillPlan {
            objects: [(object, vec![info])].into(),
            ownership: Arc::new(
                [(
                    TableId::default(),
                    Bitmap::ones(VirtualNode::COUNT_FOR_TEST),
                )]
                .into(),
            ),
        });
        assert!(
            !tokio::time::timeout(Duration::from_secs(1), ticket.wait())
                .await
                .unwrap()
        );
        assert!(cache.get(object).is_none());
        assert!(cache.is_registered(object));
        let state = executor.state.lock();
        assert!(state.objects.is_empty() && state.schedule.is_empty());
        assert_eq!(state.backlog, [[0; 2]; 3]);
    }

    #[tokio::test]
    async fn test_failed_admission_is_sticky_for_identical_tickets_and_reset_clears_debt() {
        let store = mock_sstable_store().await;
        let cache = PinCache::new(mock_sstable_store().await.store(), u64::MAX, 1, 2, [])
            .await
            .unwrap();
        let object = HummockSstableObjectId::from(870);
        let table = TableId::from(233);
        cache.register_objects([(object, 1)]);
        let store = Arc::new(
            Arc::into_inner(store)
                .unwrap()
                .with_pin_cache(cache.clone()),
        );
        let executor = PinCacheRefillExecutor::new(store.clone(), Arc::new(Semaphore::new(1)));
        // Missing remote metadata causes a real failed admission, not an injected ready state.
        let plan = PinCacheRefillPlan {
            objects: [(
                object,
                vec![
                    risingwave_hummock_sdk::sstable_info::SstableInfoInner {
                        object_id: object,
                        file_size: 1,
                        table_ids: vec![table],
                        ..Default::default()
                    }
                    .into(),
                ],
            )]
            .into(),
            ownership: Arc::new([(table, Bitmap::ones(VirtualNode::COUNT_FOR_TEST))].into()),
        };
        // Revoked attempts must skip the missing metadata; live attempts must still report it.
        let revoked = cache.prepare_refill(object).unwrap();
        cache.revoke_refill(object);
        assert!(matches!(
            refill_pin_cache_object(&store, &plan.objects[&object], &plan.ownership, revoked).await,
            Ok(Obsolete)
        ));
        let first = executor.submit(plan.clone());
        let completion = first.completions[0].clone();
        assert!(
            !tokio::time::timeout(Duration::from_secs(1), first.wait())
                .await
                .unwrap()
        );
        // I/O failure must run again after backoff while the original ticket stays failed.
        tokio::time::timeout(Duration::from_secs(2), async {
            while executor.state.lock().objects[&object].attempts < 2 {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap();
        let next = executor.submit(plan);
        assert!(completion.ptr_eq(&next.completions[0]));
        assert!(
            !next.wait().await,
            "new identical tickets do not reset failed admission"
        );
        assert_eq!(executor.state.lock().backlog, [[0, 0], [0, 0], [1, 1]]);
        cache.unregister_objects([object]);
        executor.remove_objects(&[object]);
        let state = executor.state.lock();
        assert!(state.objects.is_empty() && state.schedule.is_empty());
        assert_eq!(state.backlog, [[0; 2]; 3]);
    }

    #[tokio::test]
    async fn test_revoked_work_is_not_reused_after_membership_replacement() {
        for resubmit in [false, true] {
            let store = mock_sstable_store().await;
            let cache = PinCache::new(mock_sstable_store().await.store(), u64::MAX, 1, 2, [])
                .await
                .unwrap();
            let (_, info) = gen_test_sstable(
                default_builder_opt_for_test(),
                872,
                std::iter::once((
                    FullKey::new(
                        TableId::default(),
                        TableKey(iterator_test_table_key_of(0)),
                        test_epoch(1),
                    ),
                    HummockValue::put(vec![1]),
                )),
                store.clone(),
            )
            .await;
            let object = info.object_id;
            let size = info.file_size;
            cache.register_objects([(object, size)]);
            let store = Arc::new(
                Arc::into_inner(store)
                    .unwrap()
                    .with_pin_cache(cache.clone()),
            );
            let concurrency = Arc::new(Semaphore::new(2));
            let executor = PinCacheRefillExecutor::new(store.clone(), concurrency.clone());
            let permit = concurrency.acquire_many(2).await.unwrap();
            let plan = PinCacheRefillPlan {
                objects: [(object, vec![info])].into(),
                ownership: Arc::new(
                    [(
                        TableId::default(),
                        Bitmap::ones(VirtualNode::COUNT_FOR_TEST),
                    )]
                    .into(),
                ),
            };
            let ticket = executor.submit(plan.clone());
            tokio::time::timeout(Duration::from_secs(1), async {
                while !matches!(
                    executor.state.lock().objects[&object].status,
                    Status::Running
                ) {
                    tokio::task::yield_now().await;
                }
            })
            .await
            .unwrap();
            // The same SST can become needed again before its old attempt leaves the permit wait.
            cache.unregister_objects([object]);
            cache.register_objects([(object, size)]);
            if resubmit {
                let replacement = executor.submit(plan.clone());
                assert!(!ticket.completions[0].ptr_eq(&replacement.completions[0]));
                // Leave a second executor slot available: the running-object check, not the
                // global concurrency limit, must keep the replacement queued.
                tokio::time::timeout(Duration::from_secs(1), async {
                    while !executor.state.lock().schedule.is_empty() {
                        tokio::task::yield_now().await;
                    }
                })
                .await
                .unwrap();
                assert!(matches!(
                    executor.state.lock().objects[&object].status,
                    Status::Queued(_)
                ));
                executor.reproject(&plan.ownership, &plan.ownership.keys().copied().collect());
                assert!(
                    executor.state.lock().schedule.is_empty(),
                    "unchanged ownership must not requeue work waiting for an older transfer"
                );
                drop(permit);
                assert!(!ticket.wait().await);
                assert!(
                    tokio::time::timeout(Duration::from_secs(1), replacement.wait())
                        .await
                        .unwrap()
                );
            } else {
                // Reprojection may drop old work, but must not revoke the new admission.
                let token = cache.prepare_refill(object).unwrap();
                executor.reproject(&plan.ownership, &plan.ownership.keys().copied().collect());
                assert!(executor.state.lock().objects.is_empty());
                assert_eq!(cache.prepare_refill(object), Some(token));
                drop(permit);
                assert!(!ticket.wait().await);
                let replacement = executor.submit(plan);
                assert!(
                    tokio::time::timeout(Duration::from_secs(1), replacement.wait())
                        .await
                        .unwrap()
                );
            }
            assert!(
                cache.get(object).is_some(),
                "old work cannot revoke the current route"
            );
        }
    }

    #[cfg(not(madsim))]
    #[test]
    fn test_driver_shutdown_finishes_pending_and_future_tickets() {
        for (poll_driver, drop_owner) in [(false, false), (true, false), (true, true)] {
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap();
            let store = runtime.block_on(async {
                let store = mock_sstable_store().await;
                let cache = PinCache::new(
                    mock_sstable_store().await.store(),
                    u64::MAX,
                    1,
                    2,
                    [(HummockSstableObjectId::from(880), 1)],
                )
                .await
                .unwrap();
                Arc::new(Arc::into_inner(store).unwrap().with_pin_cache(cache))
            });
            let plan = PinCacheRefillPlan {
                objects: [(
                    880.into(),
                    vec![
                        risingwave_hummock_sdk::sstable_info::SstableInfoInner {
                            object_id: 880.into(),
                            file_size: 1,
                            table_ids: vec![TableId::default()],
                            ..Default::default()
                        }
                        .into(),
                    ],
                )]
                .into(),
                ownership: Arc::new(
                    [(
                        TableId::default(),
                        Bitmap::ones(VirtualNode::COUNT_FOR_TEST),
                    )]
                    .into(),
                ),
            };
            let (executor, ticket) = {
                let _entered = runtime.enter();
                let executor = PinCacheRefillExecutor::new(store, Arc::new(Semaphore::new(0)));
                let ticket = executor.submit(plan.clone());
                (executor, ticket)
            };
            if poll_driver {
                runtime.block_on(async {
                    tokio::time::timeout(Duration::from_secs(1), async {
                        while !matches!(
                            executor.state.lock().objects[&HummockSstableObjectId::from(880)]
                                .status,
                            Status::Running
                        ) {
                            tokio::task::yield_now().await;
                        }
                    })
                    .await
                    .unwrap();
                });
            }
            if drop_owner {
                runtime.block_on(async {
                    drop(executor);
                    assert!(
                        !tokio::time::timeout(Duration::from_secs(1), ticket.wait())
                            .await
                            .unwrap(),
                        "a ticket must not keep its driver alive after the owner exits"
                    );
                });
                continue;
            }
            drop(runtime);
            assert!(executor.state.lock().stopped);
            assert!(
                !ticket
                    .wait()
                    .now_or_never()
                    .expect("shutdown must finish pending tickets")
            );
            assert!(
                !executor
                    .submit(plan)
                    .wait()
                    .now_or_never()
                    .expect("a stopped driver cannot accept work")
            );
        }
    }

    #[tokio::test]
    async fn test_pin_executor_uses_bounded_parallelism_and_targeted_removal() {
        let store = mock_sstable_store().await;
        let cache = PinCache::new(mock_sstable_store().await.store(), u64::MAX, 1, 2, [])
            .await
            .unwrap();
        let mut objects = HashMap::new();
        for id in 850..853 {
            let (_, info) = gen_test_sstable(
                default_builder_opt_for_test(),
                id,
                std::iter::once((
                    FullKey::new(
                        TableId::default(),
                        TableKey(iterator_test_table_key_of(0)),
                        test_epoch(1),
                    ),
                    HummockValue::put(vec![1]),
                )),
                store.clone(),
            )
            .await;
            objects.insert(info.object_id, vec![info]);
        }
        cache.register_objects(objects.iter().map(|(&id, infos)| (id, infos[0].file_size)));
        let store = Arc::new(
            Arc::into_inner(store)
                .unwrap()
                .with_pin_cache(cache.clone()),
        );
        let concurrency = Arc::new(Semaphore::new(2));
        let controller =
            PinCacheRefillController::new(store, version_with_ssts(&[]), concurrency.clone());
        let executor = &controller.executor;
        // Occupy the shared data budget with Foyer work while Pin queues its attempts.
        let foyer_permits = concurrency.acquire_many(2).await.unwrap();
        let ownership: Arc<HashMap<_, _>> = Arc::new(
            [(
                TableId::default(),
                Bitmap::ones(VirtualNode::COUNT_FOR_TEST),
            )]
            .into(),
        );
        let all = executor.submit(PinCacheRefillPlan {
            objects: objects.clone(),
            ownership: ownership.clone(),
        });
        let mut tickets: HashMap<_, _> = objects
            .into_iter()
            .map(|(object, infos)| {
                (
                    object,
                    executor.submit(PinCacheRefillPlan {
                        objects: [(object, infos)].into(),
                        ownership: ownership.clone(),
                    }),
                )
            })
            .collect();
        tokio::time::timeout(Duration::from_secs(1), async {
            loop {
                let counts = {
                    let state = executor.state.lock();
                    (
                        state
                            .objects
                            .values()
                            .filter(|work| matches!(work.status, Status::Running))
                            .count(),
                        state
                            .objects
                            .values()
                            .filter(|work| matches!(work.status, Status::Queued(_)))
                            .count(),
                    )
                };
                if counts == (2, 1) && concurrency.available_permits() == 0 {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        let (removed, survivor) = {
            let state = executor.state.lock();
            let mut running = state
                .objects
                .iter()
                .filter(|(_, work)| matches!(work.status, Status::Running));
            let removed_running = *running.next().unwrap().0;
            let survivor = *running.next().unwrap().0;
            let queued = *state
                .objects
                .iter()
                .find(|(_, work)| matches!(work.status, Status::Queued(_)))
                .unwrap()
                .0;
            ([removed_running, queued], survivor)
        };
        struct WakeCount(AtomicUsize);
        impl ArcWake for WakeCount {
            fn wake_by_ref(arc_self: &Arc<Self>) {
                arc_self.0.fetch_add(1, Ordering::Relaxed);
            }
        }
        let survivor_wakes = Arc::new(WakeCount(AtomicUsize::new(0)));
        let removed_wakes = Arc::new(WakeCount(AtomicUsize::new(0)));
        let survivor_ticket = tickets.remove(&survivor).unwrap().wait();
        let removed_ticket = tickets.remove(&removed[0]).unwrap().wait();
        tokio::pin!(survivor_ticket, removed_ticket);
        let survivor_waker = waker_ref(&survivor_wakes);
        let removed_waker = waker_ref(&removed_wakes);
        assert!(
            survivor_ticket
                .as_mut()
                .poll(&mut Context::from_waker(&survivor_waker))
                .is_pending()
        );
        assert!(
            removed_ticket
                .as_mut()
                .poll(&mut Context::from_waker(&removed_waker))
                .is_pending()
        );
        let survivor_token = cache.prepare_refill(survivor);
        controller.unregister_objects([]);
        controller.unregister_objects(removed);
        assert!(removed_wakes.0.load(Ordering::Relaxed) > 0);
        assert_eq!(
            survivor_wakes.0.load(Ordering::Relaxed),
            0,
            "one admission must not wake unrelated tickets"
        );
        assert_eq!(
            removed_ticket
                .as_mut()
                .poll(&mut Context::from_waker(&removed_waker)),
            Poll::Ready(false)
        );
        assert!(
            !tokio::time::timeout(Duration::from_secs(1), all.wait())
                .await
                .unwrap(),
            "a failed member must finish the batch ticket while its survivor is blocked"
        );
        assert!(!cache.is_registered(removed[0]));
        assert!(
            !tokio::time::timeout(
                Duration::from_secs(1),
                tickets.remove(&removed[1]).unwrap().wait()
            )
            .await
            .unwrap(),
            "withdrawal must finish queued tickets without waiting for a permit"
        );
        assert!(!cache.is_registered(removed[1]));
        assert_eq!(cache.prepare_refill(survivor), survivor_token);
        drop(foyer_permits);
        assert!(
            tokio::time::timeout(Duration::from_secs(1), survivor_ticket)
                .await
                .unwrap()
        );
        assert!(cache.get(survivor).is_some());
        assert!(
            executor.state.lock().objects.is_empty(),
            "completed work must not become another live-set"
        );
    }
}
