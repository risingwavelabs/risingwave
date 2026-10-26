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

use std::sync::atomic::{AtomicUsize, Ordering};
use std::task::{Context, Poll};
use std::time::Duration;

use futures::FutureExt;
use futures::task::{ArcWake, waker_ref};
use risingwave_hummock_sdk::sstable_info::SstableInfoInner;

use super::*;
use crate::hummock::iterator::test_utils::mock_sstable_store;
use crate::hummock::pin_cache::PinCache;
use crate::hummock::pin_cache::test_utils::in_memory_object_store;
use crate::hummock::test_utils::gen_test_sstable_with_table_vnodes;

// Fixture setup only: registration and publication remain explicit in each test.
async fn with_pin_cache(store: SstableStoreRef, capacity: u64) -> (SstableStoreRef, Arc<PinCache>) {
    let cache = PinCache::new(in_memory_object_store(), capacity, 1, [])
        .await
        .unwrap();
    let store = Arc::new(
        Arc::into_inner(store)
            .unwrap()
            .with_pin_cache(cache.clone()),
    );
    (store, cache)
}

// Keep asynchronous admission assertions bounded without hiding their expected outcome.
async fn wait(waiter: impl Future<Output = bool>) -> bool {
    tokio::time::timeout(Duration::from_secs(1), waiter)
        .await
        .unwrap()
}

async fn wait_finished(executor: &PinCacheRefillExecutor) {
    tokio::time::timeout(Duration::from_secs(1), async {
        while !executor.work.lock().is_empty() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn test_pin_refill_capacity_and_cached_submission() {
    for fits in [true, false] {
        let table = TableId::from(233);
        let store = mock_sstable_store().await;
        let (_, info) =
            gen_test_sstable_with_table_vnodes(&store, 873, &[(table, 0), (table, 128)]).await;
        let object = info.object_id;
        // Exact fit versus one byte short exercises physical file capacity accounting.
        let (store, cache) = with_pin_cache(store, info.file_size - u64::from(!fits)).await;
        cache.register_objects([object]);
        let candidate = PinCacheRefillObject {
            source_sst: info,
            admitted_tables: [table].into(),
        };
        let token = cache.prepare_refill(object).unwrap();
        let executor = PinCacheRefillExecutor::new(store.clone(), Arc::new(Semaphore::new(1)));
        let completion = executor.submit(&candidate, token);
        if fits {
            // Even the only, never-polled waiter must not own or cancel its download.
            drop(completion);
            wait_finished(&executor).await;
        } else {
            assert!(!wait(completion).await);
        }
        assert_eq!(cache.get(object).is_some(), fits);
        assert!(cache.is_registered(object));
        if fits {
            let path = store.get_sst_data_path(object);
            let bytes = store.store().read(&path, ..).await.unwrap();
            store.store().delete(&path).await.unwrap();
            store.clear_meta_cache().await.unwrap();
            let token = cache.prepare_refill(object);
            // A defensive repeat job skips the cached file without remote I/O.
            assert_eq!(
                executor.submit(&candidate, token.unwrap()).now_or_never(),
                Some(true)
            );
            assert_eq!(cache.prepare_refill(object), token);
            assert_eq!(cache.get(object).unwrap().read(..).await.unwrap(), bytes);
        }
        assert!(executor.work.lock().is_empty());
    }
}

#[tokio::test]
async fn test_failed_attempt_finishes_and_defensive_resubmission_can_succeed() {
    let table = TableId::from(233);
    let store = mock_sstable_store().await;
    let (_, info) = gen_test_sstable_with_table_vnodes(&store, 870, &[(table, 0)]).await;
    let object = info.object_id;
    let path = store.get_sst_data_path(object);
    let bytes = store.store().read(&path, ..).await.unwrap();
    store.store().delete(&path).await.unwrap();
    let (store, cache) = with_pin_cache(store, u64::MAX).await;
    cache.register_objects([object]);
    let executor = PinCacheRefillExecutor::new(store.clone(), Arc::new(Semaphore::new(1)));
    let candidate = PinCacheRefillObject {
        source_sst: info,
        admitted_tables: [table].into(),
    };
    // Revocation must skip I/O even when the remote file is unavailable.
    let revoked = cache.prepare_refill(object).unwrap();
    cache.revoke_refill(object);
    assert!(matches!(
        refill_pin_cache_object(&store, candidate.source_sst.file_size, revoked).await,
        Ok(Obsolete)
    ));
    let token = cache.prepare_refill(object).unwrap();
    let first = executor.submit(&candidate, token);
    let duplicate = executor.submit(&candidate, token);
    assert!(!wait(first).await);
    assert!(!wait(duplicate).await);
    assert!(
        executor.work.lock().is_empty(),
        "failure leaves no retry debt"
    );
    store.store().upload(&path, bytes).await.unwrap();
    assert!(
        cache.get(object).is_none(),
        "restoring the remote file does not submit work"
    );
    // Explicit resubmission is API defense, not evidence of a repeated production delta.
    assert!(wait(executor.submit(&candidate, token)).await);
    assert!(cache.get(object).is_some());
}

#[tokio::test]
async fn test_cancelled_attempt_preserves_new_registration_without_restarting() {
    let table = TableId::default();
    let (store, cache) = with_pin_cache(mock_sstable_store().await, u64::MAX).await;
    let (_, info) = gen_test_sstable_with_table_vnodes(&store, 872, &[(table, 0)]).await;
    let object = info.object_id;
    cache.register_objects([object]);
    let concurrency = Arc::new(Semaphore::new(1));
    let executor = PinCacheRefillExecutor::new(store, concurrency.clone());
    let permit = concurrency.acquire().await.unwrap();
    let candidate = PinCacheRefillObject {
        source_sst: info,
        admitted_tables: [table].into(),
    };
    let waiter = executor.submit(
        &candidate,
        cache
            .prepare_refill(candidate.source_sst.object_id)
            .unwrap(),
    );
    // Exercise stale-work isolation defensively; ordinary cancellation need not re-register.
    executor.unregister_objects([object]);
    cache.register_objects([object]);
    let token = cache.prepare_refill(object);
    executor.cancel_tables(&candidate.admitted_tables);
    assert_eq!(cache.prepare_refill(object), token);
    assert!(!wait(waiter).await);
    assert!(
        !wait(
            executor.submit(
                &candidate,
                cache
                    .prepare_refill(candidate.source_sst.object_id)
                    .unwrap()
            )
        )
        .await,
        "a cancelled physical attempt still prevents an overlapping copy"
    );
    drop(permit);
    wait_finished(&executor).await;
    assert!(
        cache.get(object).is_none(),
        "cancellation never queues a replacement"
    );
    assert!(
        wait(
            executor.submit(
                &candidate,
                cache
                    .prepare_refill(candidate.source_sst.object_id)
                    .unwrap()
            )
        )
        .await
    );
}

#[cfg(not(madsim))]
#[test]
fn test_driver_shutdown_finishes_pending_and_future_submissions() {
    for (poll_driver, drop_owner) in [(false, false), (true, false), (true, true)] {
        let table = TableId::default();
        let object = 880.into();
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let (store, cache) =
            runtime.block_on(async { with_pin_cache(mock_sstable_store().await, u64::MAX).await });
        let candidate = PinCacheRefillObject {
            source_sst: SstableInfoInner {
                object_id: object,
                file_size: 1,
                table_ids: vec![table],
                ..Default::default()
            }
            .into(),
            admitted_tables: [table].into(),
        };
        cache.register_objects([object]);
        let old_token = cache.prepare_refill(object).unwrap();
        cache.unregister_objects([object]);
        let (executor, waiter, token) = {
            let _entered = runtime.enter();
            let executor = PinCacheRefillExecutor::new(store, Arc::new(Semaphore::new(0)));
            assert_eq!(
                executor.submit(&candidate, old_token).now_or_never(),
                Some(true)
            );
            assert!(executor.work.lock().is_empty());
            cache.register_objects([object]);
            let token = cache.prepare_refill(object).unwrap();
            let waiter = executor.submit(&candidate, token);
            (executor, waiter, token)
        };
        if poll_driver {
            runtime.block_on(tokio::task::yield_now());
        }
        if drop_owner {
            runtime.block_on(async {
                drop(executor);
                assert!(
                    !wait(waiter).await,
                    "a waiter must not keep its driver alive after the owner exits"
                );
            });
            continue;
        }
        drop(runtime);
        assert!(executor.jobs.is_closed());
        assert_eq!(waiter.now_or_never(), Some(false));
        assert_eq!(
            executor.submit(&candidate, token).now_or_never(),
            Some(false)
        );
        assert_eq!(
            executor.submit(&candidate, old_token).now_or_never(),
            Some(false),
            "shutdown takes precedence over stale admission"
        );
    }
}

#[tokio::test]
async fn test_pin_executor_uses_shared_budget_and_targeted_removal() {
    let table = TableId::default();
    let (store, cache) = with_pin_cache(mock_sstable_store().await, u64::MAX).await;
    let mut objects = HashMap::new();
    for id in 850..853 {
        let (_, info) = gen_test_sstable_with_table_vnodes(&store, id, &[(table, 0)]).await;
        objects.insert(
            info.object_id,
            PinCacheRefillObject {
                source_sst: info,
                admitted_tables: [table].into(),
            },
        );
    }
    cache.register_objects(objects.keys().copied());
    let concurrency = Arc::new(Semaphore::new(2));
    let executor = PinCacheRefillExecutor::new(store, concurrency.clone());
    // Occupy the shared data budget with Foyer work while Pin queues its attempts.
    let foyer_permits = concurrency.acquire_many(2).await.unwrap();
    let mut waiters: HashMap<_, _> = objects
        .into_iter()
        .map(|(object, candidate)| {
            (
                object,
                executor.submit(
                    &candidate,
                    cache
                        .prepare_refill(candidate.source_sst.object_id)
                        .unwrap(),
                ),
            )
        })
        .collect();
    let (removed, survivor) = ([850.into(), 851.into()], 852.into());
    struct WakeCount(AtomicUsize);
    impl ArcWake for WakeCount {
        fn wake_by_ref(arc_self: &Arc<Self>) {
            arc_self.0.fetch_add(1, Ordering::Relaxed);
        }
    }
    let survivor_wakes = Arc::new(WakeCount(AtomicUsize::new(0)));
    let removed_wakes = Arc::new(WakeCount(AtomicUsize::new(0)));
    let survivor_ticket = waiters.remove(&survivor).unwrap();
    let removed_ticket = waiters.remove(&removed[0]).unwrap();
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
    executor.unregister_objects([]);
    executor.unregister_objects(removed);
    assert!(removed_wakes.0.load(Ordering::Relaxed) > 0);
    assert_eq!(
        survivor_wakes.0.load(Ordering::Relaxed),
        0,
        "one admission must not wake unrelated waiters"
    );
    assert_eq!(
        removed_ticket
            .as_mut()
            .poll(&mut Context::from_waker(&removed_waker)),
        Poll::Ready(false)
    );
    assert!(!cache.is_registered(removed[0]));
    assert!(
        !wait(waiters.remove(&removed[1]).unwrap()).await,
        "withdrawal must finish queued waiters without waiting for a permit"
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
    wait_finished(&executor).await;
    assert!(
        executor.work.lock().is_empty(),
        "completed work must not become another live-set"
    );
}
