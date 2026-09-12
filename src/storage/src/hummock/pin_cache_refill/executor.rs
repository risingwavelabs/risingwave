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

use std::collections::{HashMap, HashSet};
use std::panic::AssertUnwindSafe;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, LazyLock};
use std::time::Duration;

use futures::stream::FuturesUnordered;
use futures::{FutureExt, StreamExt};
use parking_lot::Mutex;
use prometheus::{IntCounterVec, register_int_counter_vec_with_registry};
use risingwave_common::bitmap::Bitmap;
use risingwave_common::monitor::GLOBAL_METRICS_REGISTRY;
use risingwave_hummock_sdk::HummockSstableObjectId;
use risingwave_hummock_sdk::sstable_info::SstableInfo;
use risingwave_pb::id::TableId;
use thiserror_ext::AsReport;
use tokio::sync::{Notify, watch};
use tokio::time::Instant;

use super::{PinCacheRefillPlan, refill_pin_cache_object};
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

#[derive(Clone, Copy)]
enum Status {
    Queued,
    Running,
    Ready,
    Unowned,
    Failed(Instant),
}

struct Work {
    projections: Vec<SstableInfo>,
    admitted_tables: HashSet<TableId>,
    ownership: Arc<HashMap<TableId, Bitmap>>,
    generation: u64,
    attempts: u32,
    status: Status,
}

#[derive(Default)]
struct State {
    objects: HashMap<HummockSstableObjectId, Work>,
    generation: u64,
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
}

pub(crate) struct Ticket {
    executor: PinCacheRefillExecutor,
    generations: HashMap<HummockSstableObjectId, u64>,
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
            let ready = {
                let state = self.executor.state.lock();
                let mut ready = true;
                for (object, generation) in &self.generations {
                    let Some(work) = state.objects.get(object) else {
                        return false;
                    };
                    if work.generation != *generation
                        || (work.attempts > 0
                            && !matches!(work.status, Status::Ready | Status::Unowned))
                    {
                        return false;
                    }
                    ready &= matches!(work.status, Status::Ready | Status::Unowned);
                }
                ready
            };
            if ready {
                return true;
            }
            notified.await;
        }
    }
}

impl PinCacheRefillExecutor {
    pub(super) fn new(store: SstableStoreRef, concurrency: usize) -> Self {
        let state = Arc::new(Mutex::new(State::default()));
        let changed = Arc::new(Notify::new());
        let alive = Arc::new(AtomicBool::new(true));
        let (wake, receiver) = watch::channel(());
        tokio::spawn(Self::run(
            store.clone(),
            state.clone(),
            changed.clone(),
            alive.clone(),
            receiver,
            concurrency.max(1),
        ));
        Self {
            state,
            wake,
            changed,
            alive,
            store,
        }
    }

    pub(super) fn submit(&self, plan: PinCacheRefillPlan) -> Ticket {
        let mut generations = HashMap::new();
        let Some(cache) = self.store.pin_cache() else {
            return Ticket {
                executor: self.clone(),
                generations,
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
                && (!matches!(work.status, Status::Ready) || cache.get(object).is_some())
            {
                generations.insert(object, work.generation);
                continue;
            }
            // A new ownership/projection generation must not publish an older in-flight token.
            cache.revoke_inflight(object);
            state.generation += 1;
            let generation = state.generation;
            state.objects.insert(
                object,
                Work {
                    projections,
                    admitted_tables,
                    ownership,
                    generation,
                    attempts: 0,
                    status: Status::Queued,
                },
            );
            generations.insert(object, generation);
        }
        drop(state);
        self.wake.send_replace(());
        self.changed.notify_waiters();
        Ticket {
            executor: self.clone(),
            generations,
        }
    }

    pub(super) fn reproject(&self, ownership: Arc<HashMap<TableId, Bitmap>>) {
        let Some(cache) = self.store.pin_cache() else {
            return;
        };
        let mut state = self.state.lock();
        state.generation += 1;
        let generation = state.generation;
        state.objects.retain(|object, work| {
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
                if let Some(route) = cache.get(*object) {
                    route.invalidate();
                }
                return false;
            }
            if work.ownership != ownership {
                cache.revoke_inflight(*object);
                work.ownership = ownership.clone();
                work.generation = generation;
                work.status = Status::Queued;
                work.attempts = 0;
            }
            true
        });
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
        concurrency: usize,
    ) {
        let _alive = scopeguard::guard((), |_| {
            alive.store(false, Ordering::Release);
            changed.notify_waiters();
        });
        let mut tasks = FuturesUnordered::new();
        let mut running = HashSet::new();
        let mut tick = tokio::time::interval(Duration::from_millis(100));
        tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            let (work, retry_pending) = {
                let mut state = state.lock();
                // Debt is bounded by still-needed admitted objects, not by the number of errors.
                state.objects.retain(|object, _| {
                    store
                        .pin_cache()
                        .is_some_and(|cache| cache.is_needed(*object))
                });
                let retry_pending = state
                    .objects
                    .values()
                    .any(|work| matches!(work.status, Status::Failed(_)));
                let work = state
                    .objects
                    .iter_mut()
                    .filter(|(object, work)| {
                        !running.contains(*object)
                            && match work.status {
                                Status::Queued => true,
                                Status::Failed(at) => at <= Instant::now(),
                                _ => false,
                            }
                    })
                    .take(concurrency - running.len())
                    .map(|(&object, work)| {
                        work.status = Status::Running;
                        (
                            object,
                            work.generation,
                            work.projections.clone(),
                            work.ownership.clone(),
                            store
                                .pin_cache()
                                .and_then(|cache| cache.refill_generation(object)),
                        )
                    })
                    .collect::<Vec<_>>();
                (work, retry_pending)
            };
            for (object, generation, projections, ownership, cache_generation) in work {
                running.insert(object);
                let store = store.clone();
                tasks.push(async move {
                    // A panicking I/O task is a failed admission, not a permanently lost ticket.
                    let result = AssertUnwindSafe(async {
                        if let Some(generation) = cache_generation {
                            refill_pin_cache_object(&store, &projections, &ownership, generation)
                                .await
                        } else {
                            Ok(PinCacheRefillOutcome::Obsolete)
                        }
                    })
                    .catch_unwind()
                    .await;
                    (object, generation, result)
                });
            }
            tokio::select! {
                change = receiver.changed() => if change.is_err() { break; },
                _ = tick.tick(), if retry_pending => {},
                Some((object, generation, result)) = tasks.next(), if !tasks.is_empty() => {
                    running.remove(&object);
                    let outcome = match &result {
                        Ok(Ok(PinCacheRefillOutcome::Published)) => "published",
                        Ok(Ok(PinCacheRefillOutcome::AlreadyPublished)) => "already_published",
                        Ok(Ok(PinCacheRefillOutcome::InProgress)) => "in_progress",
                        Ok(Ok(PinCacheRefillOutcome::CapacityRejected)) => "capacity_rejected",
                        Ok(Ok(PinCacheRefillOutcome::Obsolete)) => "obsolete",
                        _ => "error",
                    };
                    REFILL_OUTCOMES.with_label_values(&[outcome]).inc();
                    let mut state = state.lock();
                    if let Some(work) = state.objects.get_mut(&object) && work.generation == generation {
                        work.status = match result {
                            Ok(Ok(PinCacheRefillOutcome::Published | PinCacheRefillOutcome::AlreadyPublished)) => Status::Ready,
                            Ok(Ok(PinCacheRefillOutcome::Obsolete)) => {
                                // The matching generation has rechecked block geometry. Retire
                                // old ownership's whole file so scale-out can reclaim capacity.
                                if let Some(route) = store.pin_cache().and_then(|cache| cache.get(object)) {
                                    route.invalidate();
                                }
                                Status::Unowned
                            },
                            other => {
                                if let Ok(Err(error)) = &other {
                                    tracing::warn!(object_id = object.as_raw_id(), error = %error.as_report(), "pin refill failed; retaining retry debt");
                                }
                                work.attempts = work.attempts.saturating_add(1);
                                Status::Failed(Instant::now() + Duration::from_millis(100 * (1_u64 << work.attempts.min(8))))
                            }
                        };
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
        let executor = PinCacheRefillExecutor::new(store, 2);
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
                            .filter(|work| matches!(work.status, Status::Queued))
                            .count(),
                    )
                };
                if counts == (2, 1) {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        gate.add_permits(3);
        assert!(
            tokio::time::timeout(Duration::from_secs(1), ticket.wait())
                .await
                .unwrap()
        );
        assert_eq!(
            executor
                .state
                .lock()
                .objects
                .values()
                .filter(|work| matches!(work.status, Status::Ready))
                .count(),
            3
        );
    }
}
