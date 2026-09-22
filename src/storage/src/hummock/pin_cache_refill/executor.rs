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
use std::sync::atomic::{AtomicBool, AtomicU8, Ordering};
use std::sync::{Arc, LazyLock};
use std::time::Duration;

use futures::StreamExt;
use futures::stream::FuturesUnordered;
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
use tokio::sync::{Notify, Semaphore, watch};
use tokio::time::Instant;

use super::{
    PinCacheRefillError, PinCacheRefillPlan, pin_cache_object_is_owned, refill_pin_cache_object,
};
use crate::hummock::SstableStoreRef;
use crate::hummock::pin_cache::PinCacheRefillOutcome;

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
    result: &Result<Result<PinCacheRefillOutcome, E>, P>,
) -> &'static str {
    match result {
        Ok(Ok(PinCacheRefillOutcome::Published)) => "published",
        Ok(Ok(PinCacheRefillOutcome::AlreadyPublished)) => "already_published",
        Ok(Ok(PinCacheRefillOutcome::InProgress)) => "in_progress",
        Ok(Ok(PinCacheRefillOutcome::CapacityRejected)) => "capacity_rejected",
        Ok(Ok(PinCacheRefillOutcome::Obsolete)) => "obsolete",
        Ok(Err(_)) => "error",
        Err(_) => "panic",
    }
}

struct RefillAttemptGuard {
    object: HummockSstableObjectId,
    generation: u64,
    object_size: u64,
    started_at: Instant,
    finished: bool,
}

impl RefillAttemptGuard {
    fn new(object: HummockSstableObjectId, generation: u64, object_size: u64) -> Self {
        Self {
            object,
            generation,
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
            object_id = self.object.as_raw_id(),
            generation = self.generation,
            object_size = self.object_size,
            elapsed_ms = elapsed.as_millis(),
            "pin refill attempt was cancelled before completion"
        );
    }
}

#[derive(Clone, Copy)]
enum Status {
    Queued(Instant),
    Running,
    Failed(Instant),
}

impl Status {
    fn deadline(self) -> Option<Instant> {
        match self {
            Self::Queued(at) | Self::Failed(at) => Some(at),
            Self::Running => None,
        }
    }

    fn index(self) -> usize {
        match self {
            Self::Queued(_) => 0,
            Self::Running => 1,
            Self::Failed(_) => 2,
        }
    }
}

struct Work {
    projections: Vec<SstableInfo>,
    admitted_tables: HashSet<TableId>,
    ownership: Arc<HashMap<TableId, Bitmap>>,
    generation: u64,
    completion: Arc<AtomicU8>,
    attempts: u32,
    status: Status,
}

#[derive(Default)]
struct State {
    objects: HashMap<HummockSstableObjectId, Work>,
    schedule: BTreeSet<(Instant, HummockSstableObjectId)>,
    backlog: [[i64; 2]; 3],
    generation: u64,
    recovered_route_epoch: u64,
}

impl State {
    fn remove(&mut self, object: HummockSstableObjectId) -> Option<Work> {
        let work = self.objects.remove(&object)?;
        if let Some(at) = work.status.deadline() {
            self.schedule.remove(&(at, object));
        }
        self.backlog[work.status.index()][0] -= 1;
        self.backlog[work.status.index()][1] -= work.projections[0].file_size as i64;
        Some(work)
    }

    fn insert(&mut self, object: HummockSstableObjectId, work: Work) {
        self.remove(object);
        if let Some(at) = work.status.deadline() {
            self.schedule.insert((at, object));
        }
        self.backlog[work.status.index()][0] += 1;
        self.backlog[work.status.index()][1] += work.projections[0].file_size as i64;
        self.objects.insert(object, work);
    }
}

/// A bounded object executor, not a version queue. Version deadlines only drop tickets;
/// this worker continues owning open uploads and retries live-owned failed admissions.
#[derive(Clone)]
pub(super) struct PinCacheRefillExecutor {
    state: Arc<Mutex<State>>,
    wake: watch::Sender<()>,
    changed: Arc<Notify>,
    alive: Arc<AtomicBool>,
    store: SstableStoreRef,
    concurrency: Arc<Semaphore>,
    limit: usize,
}

pub(crate) struct Ticket {
    executor: PinCacheRefillExecutor,
    // Cells outlive successful work records: 0 pending, 1 ready, 2 failed/revoked.
    completions: Vec<Arc<AtomicU8>>,
}

impl Ticket {
    pub(crate) async fn wait(self) -> bool {
        loop {
            let notified = self.executor.changed.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            if !self.executor.alive.load(Ordering::Acquire) {
                return false;
            }
            let mut ready = true;
            for completion in &self.completions {
                match completion.load(Ordering::Acquire) {
                    0 => ready = false,
                    1 => {}
                    _ => return false,
                }
            }
            if ready {
                return true;
            }
            notified.await;
        }
    }
}

impl PinCacheRefillExecutor {
    pub(super) fn new(store: SstableStoreRef, concurrency: Arc<Semaphore>) -> Self {
        for result in [
            "published",
            "already_published",
            "in_progress",
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
        let changed = Arc::new(Notify::new());
        let alive = Arc::new(AtomicBool::new(true));
        let (wake, receiver) = watch::channel(());
        let limit = concurrency.available_permits().max(1);
        tokio::spawn(Self::run(
            store.clone(),
            state.clone(),
            changed.clone(),
            alive.clone(),
            receiver,
            concurrency.clone(),
            limit,
        ));
        Self {
            state,
            wake,
            changed,
            alive,
            store,
            concurrency,
            limit,
        }
    }

    pub(super) fn cancel_recovered_route_reconcile(&self) {
        self.state.lock().recovered_route_epoch += 1;
    }

    /// Recovery knows which complete files exist, but only the refiller knows worker ownership.
    /// Validate recovered routes without downloading a missing object or creating refill debt.
    pub(super) fn reconcile_recovered_routes(
        &self,
        unplanned_objects: Vec<HummockSstableObjectId>,
        validation_plan: Option<PinCacheRefillPlan>,
    ) {
        let Some(cache) = self.store.pin_cache().cloned() else {
            return;
        };
        let epoch = {
            let mut state = self.state.lock();
            state.recovered_route_epoch += 1;
            state.recovered_route_epoch
        };
        let state = self.state.clone();
        let store = self.store.clone();
        let concurrency = self.concurrency.clone();
        let changed = self.changed.clone();
        let limit = self.limit;
        tokio::spawn(async move {
            cache.wait_for_recovery().await;
            if state.lock().recovered_route_epoch != epoch {
                return;
            }

            for object in unplanned_objects {
                let mut guard = state.lock();
                if guard.recovered_route_epoch != epoch {
                    return;
                }
                if let Some(work) = guard.remove(object) {
                    work.completion.store(2, Ordering::Release);
                }
                cache.revoke_inflight(object);
                if let Some(route) = cache.get(object) {
                    route.invalidate();
                }
            }
            changed.notify_waiters();
            let Some(PinCacheRefillPlan {
                objects, ownership, ..
            }) = validation_plan
            else {
                return;
            };

            futures::stream::iter(objects)
                .for_each_concurrent(limit, |(object, projections)| {
                    let cache = cache.clone();
                    let store = store.clone();
                    let ownership = ownership.clone();
                    let concurrency = concurrency.clone();
                    let state = state.clone();
                    async move {
                        if state.lock().recovered_route_epoch != epoch {
                            return;
                        }
                        let Some(route) = cache.get(object) else {
                            return;
                        };
                        let _permit = concurrency
                            .acquire_owned()
                            .await
                            .expect("refill concurrency stays open");
                        if state.lock().recovered_route_epoch != epoch {
                            return;
                        }
                        let owned = pin_cache_object_is_owned(&store, &projections, &ownership)
                            .await
                            .unwrap_or_else(|error| {
                                tracing::warn!(
                                    object_id = object.as_raw_id(),
                                    error = %error.as_report(),
                                    "failed to validate recovered pin-cache route"
                                );
                                false
                            });
                        let state = state.lock();
                        if state.recovered_route_epoch == epoch && !owned {
                            route.invalidate();
                        }
                    }
                })
                .await;
        });
    }

    pub(super) fn submit(&self, plan: PinCacheRefillPlan) -> Ticket {
        let mut completions = Vec::new();
        let Some(cache) = self.store.pin_cache() else {
            return Ticket {
                executor: self.clone(),
                completions,
            };
        };
        let mut state = self.state.lock();
        for (object, projections) in plan.objects {
            if !cache.is_needed(object) {
                continue;
            }
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
            if let Some(work) = state.objects.get(&object)
                && work.projections == projections
                && work.ownership == ownership
            {
                // Failure is sticky for this admission attempt. A new identical ticket
                // degrades immediately while debt retries; it must not restart its deadline.
                completions.push(work.completion.clone());
                continue;
            }
            if let Some(previous) = state.objects.get(&object) {
                previous.completion.store(2, Ordering::Release);
            }
            // A new ownership/projection generation must not publish an older in-flight token.
            cache.revoke_inflight(object);
            state.generation += 1;
            let generation = state.generation;
            let completion = Arc::new(AtomicU8::new(0));
            state.insert(
                object,
                Work {
                    projections,
                    admitted_tables,
                    ownership,
                    generation,
                    completion: completion.clone(),
                    attempts: 0,
                    status: Status::Queued(Instant::now()),
                },
            );
            completions.push(completion);
        }
        drop(state);
        self.wake.send_replace(());
        self.changed.notify_waiters();
        Ticket {
            executor: self.clone(),
            completions,
        }
    }

    pub(super) fn reproject(&self, ownership: Arc<HashMap<TableId, Bitmap>>) {
        let Some(cache) = self.store.pin_cache() else {
            return;
        };
        let mut state = self.state.lock();
        state.generation += 1;
        let generation = state.generation;
        let mut objects = std::mem::take(&mut state.objects);
        state.schedule.clear();
        state.backlog = [[0; 2]; 3];
        objects.retain(|object, work| {
            let ownership = Arc::new(
                ownership
                    .iter()
                    .filter(|(table, _)| work.admitted_tables.contains(*table))
                    .map(|(&table, bitmap)| (table, bitmap.clone()))
                    .collect::<HashMap<_, _>>(),
            );
            if !cache.is_needed(*object)
                || !work.projections.iter().any(|sst| {
                    sst.table_ids
                        .iter()
                        .any(|table| ownership.contains_key(table))
                })
            {
                cache.revoke_inflight(*object);
                work.completion.store(2, Ordering::Release);
                if let Some(route) = cache.get(*object) {
                    route.invalidate();
                }
                return false;
            }
            if work.ownership != ownership {
                cache.revoke_inflight(*object);
                work.completion.store(2, Ordering::Release);
                work.completion = Arc::new(AtomicU8::new(0));
                work.ownership = ownership;
                work.generation = generation;
                work.status = Status::Queued(Instant::now());
                work.attempts = 0;
            }
            true
        });
        for (object, work) in objects {
            state.insert(object, work);
        }
        drop(state);
        self.wake.send_replace(());
        self.changed.notify_waiters();
    }

    async fn run(
        store: SstableStoreRef,
        state: Arc<Mutex<State>>,
        changed: Arc<Notify>,
        alive: Arc<AtomicBool>,
        mut receiver: watch::Receiver<()>,
        concurrency: Arc<Semaphore>,
        limit: usize,
    ) {
        let _alive = scopeguard::guard((), |_| {
            alive.store(false, Ordering::Release);
            changed.notify_waiters();
        });
        let mut tasks = FuturesUnordered::new();
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
                        work.generation,
                        work.projections.clone(),
                        work.ownership.clone(),
                        store
                            .pin_cache()
                            .and_then(|cache| cache.refill_generation(object)),
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
                            .map(|(_, _, infos, _, _)| infos[0].file_size as i64)
                            .sum::<i64>(),
                ];
                report_backlog(current, &mut backlog);
                (jobs, state.schedule.first().map(|&(at, _)| at))
            };
            for (object, generation, projections, ownership, cache_generation) in work {
                let object_size = projections[0].file_size;
                running.insert(object, object_size);
                let store = store.clone();
                let concurrency = concurrency.clone();
                tasks.push(async move {
                    let mut attempt = RefillAttemptGuard::new(object, generation, object_size);
                    // A panicking I/O task is a failed admission, not a permanently lost ticket.
                    let result = AssertUnwindSafe(async {
                        let permit_wait_started_at = Instant::now();
                        let _permit = concurrency
                            .acquire_owned()
                            .await
                            .expect("refill concurrency stays open");
                        REFILL_PERMIT_WAIT_DURATION
                            .observe(permit_wait_started_at.elapsed().as_secs_f64());
                        if let Some(generation) = cache_generation {
                            refill_pin_cache_object(&store, &projections, &ownership, generation)
                                .await
                        } else {
                            Ok(PinCacheRefillOutcome::Obsolete)
                        }
                    })
                    .rw_catch_unwind()
                    .await;
                    let elapsed = attempt.finish(attempt_result_label(&result));
                    (object, generation, result, elapsed)
                });
            }
            tokio::select! {
                change = receiver.changed() => if change.is_err() { break; },
                _ = tokio::time::sleep_until(next_retry.unwrap_or_else(Instant::now)), if next_retry.is_some() && running.len() < limit => {},
                Some((object, generation, result, elapsed)) = tasks.next(), if !tasks.is_empty() => {
                    running.remove(&object);
                    REFILL_OUTCOMES
                        .with_label_values(&[attempt_result_label(&result)])
                        .inc();
                    let mut state = state.lock();
                    if state.objects.get(&object).is_some_and(|work| work.generation == generation) {
                        let mut work = state.remove(object).unwrap();
                        let retry = match result {
                            Ok(Ok(PinCacheRefillOutcome::Published | PinCacheRefillOutcome::AlreadyPublished)) => {
                                let _ = work.completion.compare_exchange(0, 1, Ordering::AcqRel, Ordering::Acquire);
                                false
                            },
                            Ok(Ok(PinCacheRefillOutcome::Obsolete)) => {
                                // The matching generation has rechecked block geometry. Retire
                                // old ownership's whole file so scale-out can reclaim capacity.
                                if let Some(route) = store.pin_cache().and_then(|cache| cache.get(object)) {
                                    route.invalidate();
                                }
                                let _ = work.completion.compare_exchange(0, 1, Ordering::AcqRel, Ordering::Acquire);
                                false
                            },
                            Ok(Ok(
                                PinCacheRefillOutcome::InProgress
                                | PinCacheRefillOutcome::CapacityRejected,
                            )) => true,
                            Ok(Err(PinCacheRefillError { phase, error })) => {
                                REFILL_FAILURES.with_label_values(&[phase]).inc();
                                tracing::warn!(
                                    object_id = object.as_raw_id(),
                                    generation,
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
                                    generation,
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
                            work.status = Status::Failed(
                                Instant::now()
                                    + Duration::from_millis(
                                        100 * (1_u64 << work.attempts.min(8)),
                                    ),
                            );
                            work.completion.store(2, Ordering::Release);
                            state.insert(object, work);
                        }
                    }
                    // An old-generation transfer may have occupied this object's slot while
                    // its replacement was queued. Re-enable that replacement now.
                    if let Some(at) = state.objects.get(&object).and_then(|work| work.status.deadline()) {
                        state.schedule.insert((at, object));
                    }
                    drop(state);
                    changed.notify_waiters();
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use risingwave_common::hash::VirtualNode;
    use risingwave_common::util::epoch::test_epoch;
    use risingwave_hummock_sdk::key::{FullKey, TableKey};

    use super::*;
    use crate::hummock::iterator::test_utils::{iterator_test_table_key_of, mock_sstable_store};
    use crate::hummock::pin_cache::PinCache;
    use crate::hummock::test_utils::{default_builder_opt_for_test, gen_test_sstable};
    use crate::hummock::value::HummockValue;

    #[tokio::test]
    async fn test_failed_admission_is_sticky_for_identical_tickets_and_reset_clears_debt() {
        let store = mock_sstable_store().await;
        let cache = PinCache::new(mock_sstable_store().await.store(), u64::MAX);
        let object = HummockSstableObjectId::from(870);
        let table = TableId::from(233);
        cache.replace_desired_objects([(object, 1)]);
        store.set_pin_cache(cache.clone());
        let executor = PinCacheRefillExecutor::new(store, Arc::new(Semaphore::new(1)));
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
        let first = executor.submit(plan.clone());
        let completion = first.completions[0].clone();
        assert!(
            !tokio::time::timeout(Duration::from_secs(1), first.wait())
                .await
                .unwrap()
        );
        let next = executor.submit(plan);
        assert!(Arc::ptr_eq(&completion, &next.completions[0]));
        assert!(
            !next.wait().await,
            "new identical tickets do not reset failed admission"
        );
        assert_eq!(executor.state.lock().backlog, [[0, 0], [0, 0], [1, 1]]);
        cache.replace_desired_objects([]);
        executor.reproject(Arc::default());
        let state = executor.state.lock();
        assert!(state.objects.is_empty() && state.schedule.is_empty());
        assert_eq!(state.backlog, [[0; 2]; 3]);
    }

    #[tokio::test]
    async fn test_pin_executor_uses_bounded_parallelism() {
        let store = mock_sstable_store().await;
        let cache = PinCache::new(mock_sstable_store().await.store(), u64::MAX);
        let gate = Arc::new(tokio::sync::Semaphore::new(0));
        cache.set_refill_gate_for_test(gate.clone());
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
        cache.replace_desired_objects(objects.iter().map(|(&id, infos)| (id, infos[0].file_size)));
        store.set_pin_cache(cache.clone());
        let concurrency = Arc::new(Semaphore::new(2));
        let executor = PinCacheRefillExecutor::new(store, concurrency.clone());
        // Model one simultaneous Foyer download using the same data budget.
        let foyer_permit = concurrency.acquire().await.unwrap();
        let ticket = executor.submit(PinCacheRefillPlan {
            objects,
            ownership: Arc::new(
                [(
                    TableId::default(),
                    Bitmap::ones(VirtualNode::COUNT_FOR_TEST),
                )]
                .into(),
            ),
        });
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
        drop(foyer_permit);
        gate.add_permits(3);
        assert!(
            tokio::time::timeout(Duration::from_secs(1), ticket.wait())
                .await
                .unwrap()
        );
        assert!(
            executor.state.lock().objects.is_empty(),
            "completed work must not become another live-set"
        );
    }
}
