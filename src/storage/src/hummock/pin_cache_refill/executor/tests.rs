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

use bytes::Bytes;
use futures::task::{ArcWake, waker_ref};
use risingwave_common::hash::VirtualNode;
use risingwave_common::util::epoch::test_epoch;
use risingwave_hummock_sdk::EpochWithGap;
use risingwave_hummock_sdk::compaction_group::hummock_version_ext::SstDeltaInfo;
use risingwave_hummock_sdk::key::{FullKey, TableKey, UserKey, prefix_slice_with_vnode};

use super::*;
use crate::hummock::iterator::test_utils::{iterator_test_table_key_of, mock_sstable_store};
use crate::hummock::pin_cache_refill::PinCacheRefillController;
use crate::hummock::pin_cache_refill::tests::version_with_ssts;
use crate::hummock::test_utils::{
    default_builder_opt_for_test, gen_test_sstable, gen_test_sstable_with_table_ids,
};
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
        let ownership_view = RefillOwnership {
            streaming: Some(&ownership),
            serving: None,
        };
        let changes = controller.replace_pinned_tables(
            tables.into(),
            std::slice::from_ref(&version),
            ownership_view,
        );
        cache.register_objects(changes.inserted);
        controller.unregister_objects(changes.removed);
        let ticket = controller.submit(
            PinCacheRefillPlan {
                objects: [(object, vec![info])].into(),
                admitted_tables: ownership.keys().copied().collect(),
            },
            ownership_view,
        );
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
        let without_a_view = RefillOwnership {
            streaming: Some(&without_a),
            serving: None,
        };
        if migrate_first {
            // RESET must also take effect when ownership is unchanged by the policy update.
            controller.update_ownership(without_a_view);
        }
        if reset_policy {
            let changes = controller.replace_pinned_tables(
                [tables[1]].into(),
                std::slice::from_ref(&version),
                if migrate_first {
                    without_a_view
                } else {
                    ownership_view
                },
            );
            cache.register_objects(changes.inserted);
            controller.unregister_objects(changes.removed);
        }
        controller.update_ownership(without_a_view);
        if reset_policy {
            let changes = controller.replace_pinned_tables(
                tables.into(),
                std::slice::from_ref(&version),
                without_a_view,
            );
            cache.register_objects(changes.inserted);
            controller.unregister_objects(changes.removed);
        }
        controller.update_ownership(ownership_view);
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
async fn test_pin_refill_projection_admission_and_capacity() {
    for fits in [true, false] {
        let table = TableId::from(233);
        let store = mock_sstable_store().await;
        let mut options = default_builder_opt_for_test();
        options.block_capacity = 1;
        let (sst, info) = gen_test_sstable_with_table_ids(
            options,
            873,
            [0, 128].into_iter().map(|vnode| {
                (
                    FullKey {
                        user_key: UserKey::for_test(
                            table,
                            prefix_slice_with_vnode(VirtualNode::from_index(vnode), b"key"),
                        ),
                        epoch_with_gap: EpochWithGap::new_from_epoch(test_epoch(233)),
                    },
                    HummockValue::put(Bytes::from_static(b"value")),
                )
            }),
            store.clone(),
            vec![table.as_raw_id()],
        )
        .await;
        for (vnode, expected) in [(0, true), (128, true), (255, false)] {
            let ownership = HashMap::from([(
                table,
                Bitmap::from_indices(VirtualNode::COUNT_FOR_TEST, [vnode]),
            )]);
            assert_eq!(owns_object(&sst, &ownership), expected);
        }
        assert!(!owns_object(&sst, &HashMap::new()));
        let object = info.object_id;
        // An exact fit also ensures a cached object cannot download a second copy.
        let cache = PinCache::new(
            mock_sstable_store().await.store(),
            info.file_size - u64::from(!fits),
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
        let plan = PinCacheRefillPlan::new(
            &[SstDeltaInfo {
                insert_sst_infos: vec![info.clone(), info],
                ..Default::default()
            }],
            &[object].into(),
            [table].into(),
        );
        assert_eq!(plan.objects.len(), 1, "physical downloads are deduplicated");
        let ownership = [(table, Bitmap::ones(VirtualNode::COUNT_FOR_TEST))].into();
        let ownership_view = RefillOwnership {
            streaming: Some(&ownership),
            serving: None,
        };
        if fits {
            let token = cache.prepare_refill(object).unwrap();
            assert!(matches!(
                refill_pin_cache_object(&store, &plan.objects[&object], &ownership, token).await,
                Ok(Published)
            ));
        }
        let executor = PinCacheRefillExecutor::new(store.clone(), Arc::new(Semaphore::new(1)));
        assert_eq!(
            tokio::time::timeout(
                Duration::from_secs(1),
                executor.submit(plan.clone(), ownership_view).wait()
            )
            .await
            .unwrap(),
            fits
        );
        assert_eq!(cache.get(object).is_some(), fits);
        assert!(cache.is_registered(object));
        if fits {
            let token = cache.prepare_refill(object).unwrap();
            assert!(matches!(
                refill_pin_cache_object(&store, &plan.objects[&object], &ownership, token).await,
                Ok(AlreadyPublished)
            ));
            assert!(matches!(
                refill_pin_cache_object(&store, &plan.objects[&object], &HashMap::new(), token)
                    .await,
                Ok(NotOwned(Some(_)))
            ));
        }
        let state = executor.state.lock();
        assert!(state.objects.is_empty() && state.schedule.is_empty());
        assert_eq!(state.backlog, [[0; 2]; 3]);
    }
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
        admitted_tables: [table].into(),
    };
    let ownership = [(table, Bitmap::ones(VirtualNode::COUNT_FOR_TEST))].into();
    let ownership_view = RefillOwnership {
        streaming: Some(&ownership),
        serving: None,
    };
    // Revoked attempts must skip the missing metadata; live attempts must still report it.
    let revoked = cache.prepare_refill(object).unwrap();
    cache.revoke_refill(object);
    assert!(matches!(
        refill_pin_cache_object(&store, &plan.objects[&object], &ownership, revoked).await,
        Ok(Obsolete)
    ));
    let first = executor.submit(plan.clone(), ownership_view);
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
    let next = executor.submit(plan, ownership_view);
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
            admitted_tables: [TableId::default()].into(),
        };
        let ownership = [(
            TableId::default(),
            Bitmap::ones(VirtualNode::COUNT_FOR_TEST),
        )]
        .into();
        let ownership_view = RefillOwnership {
            streaming: Some(&ownership),
            serving: None,
        };
        let ticket = executor.submit(plan.clone(), ownership_view);
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
            let replacement = executor.submit(plan.clone(), ownership_view);
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
            executor.reproject(ownership_view, &plan.admitted_tables);
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
            executor.reproject(ownership_view, &plan.admitted_tables);
            assert!(executor.state.lock().objects.is_empty());
            assert_eq!(cache.prepare_refill(object), Some(token));
            drop(permit);
            assert!(!ticket.wait().await);
            let replacement = executor.submit(plan, ownership_view);
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
            admitted_tables: [TableId::default()].into(),
        };
        let ownership = [(
            TableId::default(),
            Bitmap::ones(VirtualNode::COUNT_FOR_TEST),
        )]
        .into();
        let ownership_view = RefillOwnership {
            streaming: Some(&ownership),
            serving: None,
        };
        let (executor, ticket) = {
            let _entered = runtime.enter();
            let executor = PinCacheRefillExecutor::new(store, Arc::new(Semaphore::new(0)));
            let ticket = executor.submit(plan.clone(), ownership_view);
            (executor, ticket)
        };
        if poll_driver {
            runtime.block_on(async {
                tokio::time::timeout(Duration::from_secs(1), async {
                    while !matches!(
                        executor.state.lock().objects[&HummockSstableObjectId::from(880)].status,
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
                .submit(plan, ownership_view)
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
    let ownership: HashMap<_, _> = [(
        TableId::default(),
        Bitmap::ones(VirtualNode::COUNT_FOR_TEST),
    )]
    .into();
    let ownership_view = RefillOwnership {
        streaming: Some(&ownership),
        serving: None,
    };
    let all = executor.submit(
        PinCacheRefillPlan {
            objects: objects.clone(),
            admitted_tables: ownership.keys().copied().collect(),
        },
        ownership_view,
    );
    let mut tickets: HashMap<_, _> = objects
        .into_iter()
        .map(|(object, infos)| {
            (
                object,
                executor.submit(
                    PinCacheRefillPlan {
                        objects: [(object, infos)].into(),
                        admitted_tables: ownership.keys().copied().collect(),
                    },
                    ownership_view,
                ),
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
