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

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use risingwave_common::bitmap::Bitmap;
use risingwave_common::config::Role;
use risingwave_common::config::streaming::CacheRefillPolicy;
use risingwave_common::hash::VirtualNode;
use risingwave_common::util::epoch::test_epoch;
use risingwave_hummock_sdk::compaction_group::group_split::split_sst_with_table_ids;
use risingwave_pb::id::TableId;
use tokio::sync::{OwnedSemaphorePermit, Semaphore, TryAcquireError};

use super::super::test_utils::{
    RefillTest, apply_next_events, gen_test_sst_with_object_id, l0_insert_delta,
    mock_sstable_store_with_pin_cache, pinned_version_with_ssts, test_refill_config,
    wait_for_pin_downloads,
};
use super::super::{SstDeltaInfo, default_spawn_refill_task};
use crate::hummock::iterator::test_utils::mock_sstable_store;
use crate::hummock::pin_cache::PinCache;
use crate::hummock::pin_cache::test_utils::in_memory_object_store;
use crate::hummock::test_utils::gen_test_sstable_with_table_vnodes;

// With Pin-only inputs, the worker is the only metadata waiter. A failed try_acquire
// after release proves the permit was handed to its already-polled preparation future.
// These current-thread tests change metadata before yielding to that future again.
async fn release_to_pin_preparation(semaphore: Arc<Semaphore>, mut permit: OwnedSemaphorePermit) {
    tokio::time::timeout(Duration::from_secs(1), async move {
        loop {
            drop(permit);
            permit = match semaphore.clone().try_acquire_owned() {
                Ok(permit) => permit,
                Err(TryAcquireError::NoPermits) => break,
                Err(TryAcquireError::Closed) => panic!("metadata semaphore closed"),
            };
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("Pin must enter metadata preparation");
}

#[tokio::test]
async fn test_pin_admission_uses_logical_tables_and_current_ownership() {
    #[derive(Clone, Copy, Debug)]
    enum Projection {
        AOnly,
        WholeObject,
        SplitReferences,
    }
    use Projection::{AOnly, SplitReferences, WholeObject};

    let b = TableId::from(233);
    let a = TableId::from(234);
    // Expected hits are [selected projection, A-only control object]. B is always locally owned.
    for (projection, pin_b, a_vnodes, expected) in [
        (AOnly, true, None, [false, false]),
        (AOnly, false, Some(&[][..]), [false, false]),
        // The table is local, but neither object contains its owned vnode.
        (WholeObject, false, Some(&[255][..]), [false, false]),
        (AOnly, false, Some(&[0][..]), [true, true]),
        (SplitReferences, true, None, [true, false]),
    ] {
        let (store, cache) = mock_sstable_store_with_pin_cache().await;
        let mut inserted = Vec::new();
        let mut next_sst_id = 900.into();
        for object in [702, 703] {
            let (_, info) =
                gen_test_sstable_with_table_vnodes(&store, object, &[(b, 0), (a, 0), (a, 128)])
                    .await;
            let (b_ref, a_ref) = split_sst_with_table_ids(&info, &mut next_sst_id, 1, 1, vec![a]);
            match (object, projection) {
                (702, WholeObject) => inserted.push(info),
                (702, SplitReferences) => inserted.extend([a_ref, b_ref]),
                _ => inserted.push(a_ref),
            }
        }
        let base = pinned_version_with_ssts(&[a, b], &[]);
        let mut refiller = RefillTest::new(
            Role::Streaming,
            CacheRefillPolicy::Disabled,
            test_refill_config(),
            store,
            default_spawn_refill_task(),
            base.clone(),
        );
        refiller.replace_table_cache_refill_runtime_snapshot(if pin_b {
            [(a, CacheRefillPolicy::Pin), (b, CacheRefillPolicy::Pin)].into()
        } else {
            [(a, CacheRefillPolicy::Pin)].into()
        });
        refiller.update_streaming_table_vnodes(b, Some(Bitmap::ones(VirtualNode::COUNT_FOR_TEST)));
        if let Some(vnodes) = a_vnodes {
            refiller.update_streaming_table_vnodes(
                a,
                Some(Bitmap::from_indices(
                    VirtualNode::COUNT_FOR_TEST,
                    vnodes.iter().copied(),
                )),
            );
        }
        refiller.submit(vec![l0_insert_delta(1.into(), test_epoch(1), inserted)]);
        apply_next_events(&mut refiller).await;

        let actual = [702, 703].map(|object| cache.get(object.into()).is_some());
        assert_eq!(
            actual, expected,
            "projection={projection:?}, pin_b={pin_b}, a_vnodes={a_vnodes:?}"
        );
    }
}

#[tokio::test]
async fn test_pin_owner_drop() {
    for stage in ["queued", "preparing", "submitted"] {
        let table = TableId::from(233);
        let (store, cache) = mock_sstable_store_with_pin_cache().await;
        let (_, info) = gen_test_sst_with_object_id(table, store.clone(), 875).await;
        let (_, queued) = gen_test_sst_with_object_id(table, store.clone(), 876).await;
        let object = info.object_id;
        let weak_store = Arc::downgrade(&store);
        let base = pinned_version_with_ssts(&[table], &[]);
        let mut refiller = RefillTest::new(
            Role::Streaming,
            CacheRefillPolicy::Disabled,
            test_refill_config(),
            store,
            default_spawn_refill_task(),
            base.clone(),
        );
        refiller
            .replace_table_cache_refill_runtime_snapshot([(table, CacheRefillPolicy::Pin)].into());
        refiller
            .update_streaming_table_vnodes(table, Some(Bitmap::ones(VirtualNode::COUNT_FOR_TEST)));
        let permit = refiller
            .cache()
            .foyer
            .concurrency
            .clone()
            .acquire_owned()
            .await
            .unwrap();
        let weak_executor =
            Arc::downgrade(&refiller.cache().pin_cache_refill.as_ref().unwrap().executor);
        let meta_semaphore = refiller
            .cache()
            .foyer
            .meta_refill_concurrency
            .as_ref()
            .unwrap()
            .clone();
        let metadata_permit = if stage == "preparing" {
            Some(meta_semaphore.clone().acquire_owned().await.unwrap())
        } else {
            None
        };
        refiller.submit(vec![l0_insert_delta(1.into(), test_epoch(1), vec![info])]);
        if let Some(permit) = metadata_permit {
            release_to_pin_preparation(meta_semaphore, permit).await;
        }
        if stage == "submitted" {
            wait_for_pin_downloads(&refiller, &[object]).await;
        }
        refiller.submit(vec![l0_insert_delta(
            2.into(),
            test_epoch(2),
            vec![queued.clone()],
        )]);
        let worker = refiller.cache().pin_cache_refill.as_ref().unwrap();
        assert!(!worker.executor.has_pending_for_test(queued.object_id));
        assert!(
            cache.is_registered(queued.object_id),
            "received membership is registered while refill intent remains queued"
        );
        assert_eq!(
            worker.executor.has_pending_for_test(object),
            stage == "submitted"
        );
        drop(refiller);
        assert!(
            weak_executor.upgrade().is_none(),
            "the batch waiter must not retain the executor owner"
        );
        tokio::time::timeout(Duration::from_secs(1), async {
            while weak_store.upgrade().is_some() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("owner drop must release pending and submitted work");
        assert!(cache.get(object).is_none());
        assert!(cache.get(queued.object_id).is_none());
        drop(permit);
    }
}

#[tokio::test]
async fn test_pin_preparation_rechecks_policy_and_vnode_range() {
    for reset_then_set in [false, true] {
        let sibling = TableId::from(233);
        let table = TableId::from(234);
        let (store, cache) = mock_sstable_store_with_pin_cache().await;
        let (_, info) =
            gen_test_sstable_with_table_vnodes(&store, 878, &[(sibling, 0), (table, 0)]).await;
        let object = info.object_id;
        let mut refiller = RefillTest::new(
            Role::Serving,
            CacheRefillPolicy::Disabled,
            test_refill_config(),
            store,
            default_spawn_refill_task(),
            pinned_version_with_ssts(&[sibling, table], &[]),
        );
        refiller.replace_table_cache_refill_runtime_snapshot(
            [
                (sibling, CacheRefillPolicy::Pin),
                (table, CacheRefillPolicy::Pin),
            ]
            .into(),
        );
        refiller.replace_serving_table_vnode_mapping(
            [(table, Bitmap::ones(VirtualNode::COUNT_FOR_TEST))].into(),
        );
        let semaphore = refiller
            .cache()
            .foyer
            .meta_refill_concurrency
            .as_ref()
            .unwrap()
            .clone();
        let permit = semaphore.clone().acquire_owned().await.unwrap();
        refiller.submit(vec![l0_insert_delta(1.into(), test_epoch(1), vec![info])]);
        release_to_pin_preparation(semaphore, permit).await;
        assert!(
            !refiller
                .cache()
                .pin_cache_refill
                .as_ref()
                .unwrap()
                .executor
                .has_pending_for_test(object)
        );
        if reset_then_set {
            // The sibling preserves object registration and its token. Only the captured
            // policy intent can reject this withdrawn-then-reenabled table after the await.
            refiller.replace_table_cache_refill_runtime_snapshot(
                [(sibling, CacheRefillPolicy::Pin)].into(),
            );
            refiller.replace_table_cache_refill_runtime_snapshot(
                [
                    (sibling, CacheRefillPolicy::Pin),
                    (table, CacheRefillPolicy::Pin),
                ]
                .into(),
            );
        } else {
            // Serving mappings are replaced by Meta without reconstructing a streaming actor.
            refiller.replace_serving_table_vnode_mapping(
                [(
                    table,
                    Bitmap::from_indices(VirtualNode::COUNT_FOR_TEST, [255]),
                )]
                .into(),
            );
        }
        apply_next_events(&mut refiller).await;
        assert!(cache.is_registered(object));
        assert!(
            cache.get(object).is_none(),
            "reset_then_set={reset_then_set}"
        );
        assert!(
            !refiller
                .cache()
                .pin_cache_refill
                .as_ref()
                .unwrap()
                .executor
                .has_pending_for_test(object)
        );
    }
}

#[tokio::test(start_paused = true)]
async fn test_selected_batch_endpoint_on_timeout_or_revocation() {
    for revoke_ownership in [false, true] {
        let table = TableId::from(233);
        let (store, cache) = mock_sstable_store_with_pin_cache().await;
        let (_, first_sst) = gen_test_sst_with_object_id(table, store.clone(), 877).await;
        let first_target = pinned_version_with_ssts(&[table], std::slice::from_ref(&first_sst));
        let mut last_version = (*first_target).clone();
        last_version.id = 2.into();
        let last_target = first_target.new_with_local_version(last_version).unwrap();
        let mut config = test_refill_config();
        config.timeout = Duration::from_secs(if revoke_ownership { 60 } else { 1 });
        let mut refiller = RefillTest::new(
            Role::Streaming,
            CacheRefillPolicy::Disabled,
            config,
            store,
            default_spawn_refill_task(),
            pinned_version_with_ssts(&[table], &[]),
        );
        refiller
            .replace_table_cache_refill_runtime_snapshot([(table, CacheRefillPolicy::Pin)].into());
        refiller
            .update_streaming_table_vnodes(table, Some(Bitmap::ones(VirtualNode::COUNT_FOR_TEST)));
        let _permit = refiller
            .cache()
            .foyer
            .concurrency
            .clone()
            .acquire_owned()
            .await
            .unwrap();
        let object = first_sst.object_id;
        // Register received membership as the handler does before backend submission.
        cache.register_objects([first_sst.object_id]);
        let pin = refiller.cache().pin_cache_refill.as_ref().unwrap();
        let first = {
            pin.start_cache_refill(
                &[SstDeltaInfo {
                    insert_sst_infos: vec![first_sst],
                    ..Default::default()
                }],
                first_target,
                Some(&[]),
                &[table].into(),
            )
        };
        // Empty intent still belongs to the selected batch and supplies its endpoint.
        let last = pin.start_cache_refill(&[], last_target, Some(&[]), &[table].into());
        wait_for_pin_downloads(&refiller, &[object]).await;
        if revoke_ownership {
            refiller.update_streaming_table_vnodes(table, None);
        } else {
            tokio::time::advance(Duration::from_secs(1)).await;
        }
        let endpoints =
            tokio::time::timeout(Duration::from_secs(1), async { (first.await, last.await) })
                .await
                .unwrap();
        assert_eq!(
            endpoints,
            (Ok(false), Ok(true)),
            "revoke_ownership={revoke_ownership}"
        );
    }
}

#[tokio::test]
async fn test_pin_changes_before_and_after_submission() {
    #[derive(Clone, Copy, Debug)]
    enum Change {
        PendingOwnership,
        Policy,
        Ownership,
        EquivalentLane,
        OwnedRange,
        NewOwnership,
    }
    for change in [
        Change::PendingOwnership,
        Change::Policy,
        Change::Ownership,
        Change::EquivalentLane,
        Change::OwnedRange,
        Change::NewOwnership,
    ] {
        let tables = [TableId::from(233), TableId::from(234)];
        let (store, cache) = mock_sstable_store_with_pin_cache().await;
        let (_, info) =
            gen_test_sstable_with_table_vnodes(&store, 874, &[(tables[0], 0), (tables[1], 0)])
                .await;
        let object = info.object_id;
        let base = pinned_version_with_ssts(&tables, &[]);
        let mut refiller = RefillTest::new(
            Role::Both,
            CacheRefillPolicy::Disabled,
            test_refill_config(),
            store,
            default_spawn_refill_task(),
            base.clone(),
        );
        refiller.replace_table_cache_refill_runtime_snapshot(
            tables
                .into_iter()
                .map(|table| (table, CacheRefillPolicy::Pin))
                .collect(),
        );
        refiller.update_streaming_table_vnodes(
            tables[0],
            Some(Bitmap::ones(VirtualNode::COUNT_FOR_TEST)),
        );
        if !matches!(change, Change::NewOwnership | Change::PendingOwnership) {
            refiller.update_streaming_table_vnodes(
                tables[1],
                Some(Bitmap::ones(VirtualNode::COUNT_FOR_TEST)),
            );
        }
        let permit = refiller
            .cache()
            .foyer
            .concurrency
            .clone()
            .acquire_owned()
            .await
            .unwrap();
        refiller.submit(vec![l0_insert_delta(1.into(), test_epoch(1), vec![info])]);
        if !matches!(change, Change::PendingOwnership) {
            wait_for_pin_downloads(&refiller, &[object]).await;
        }
        let original_token = cache.prepare_refill(object);
        match change {
            Change::EquivalentLane => {
                refiller.replace_serving_table_vnode_mapping(
                    [(tables[0], Bitmap::ones(VirtualNode::COUNT_FOR_TEST))].into(),
                );
                refiller.update_streaming_table_vnodes(tables[0], None);
            }
            Change::OwnedRange => refiller.update_streaming_table_vnodes(
                tables[0],
                Some(Bitmap::from_indices(VirtualNode::COUNT_FOR_TEST, [255])),
            ),
            Change::NewOwnership => refiller.update_streaming_table_vnodes(
                tables[1],
                Some(Bitmap::ones(VirtualNode::COUNT_FOR_TEST)),
            ),
            Change::Policy => refiller.replace_table_cache_refill_runtime_snapshot(
                [(tables[1], CacheRefillPolicy::Pin)].into(),
            ),
            // These updates represent the union after read-version registration/destruction,
            // not an in-place rescale of a retained actor.
            Change::Ownership | Change::PendingOwnership => {
                refiller.update_streaming_table_vnodes(tables[0], None)
            }
        }
        let cancelled = matches!(
            change,
            Change::Policy | Change::Ownership | Change::OwnedRange
        );
        if cancelled {
            let fresh_token = cache.prepare_refill(object);
            assert_ne!(fresh_token, original_token);
            assert!(cache.is_registered(object), "the sibling keeps membership");
            refiller.replace_table_cache_refill_runtime_snapshot(
                tables
                    .into_iter()
                    .map(|table| (table, CacheRefillPolicy::Pin))
                    .collect(),
            );
            refiller.update_streaming_table_vnodes(
                tables[0],
                Some(Bitmap::ones(VirtualNode::COUNT_FOR_TEST)),
            );
            assert_eq!(cache.prepare_refill(object), fresh_token);
            // Cancellation releases publication even while the physical task waits for capacity.
            apply_next_events(&mut refiller).await;
        } else {
            assert_eq!(cache.prepare_refill(object), original_token);
        }
        drop(permit);
        if !cancelled {
            apply_next_events(&mut refiller).await;
        }
        tokio::time::timeout(Duration::from_secs(1), async {
            while refiller
                .cache()
                .pin_cache_refill
                .as_ref()
                .unwrap()
                .executor
                .has_pending_for_test(object)
            {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        assert_eq!(
            cache.get(object).is_some(),
            !cancelled && !matches!(change, Change::PendingOwnership),
            "{change:?}"
        );
    }
}

#[tokio::test]
async fn test_pin_recovery_keeps_local_files_without_remote_backfill() {
    let table = TableId::from(233);
    let store = mock_sstable_store().await;
    let (_, recovered) = gen_test_sst_with_object_id(table, store.clone(), 826).await;
    let (_, missing) = gen_test_sst_with_object_id(table, store.clone(), 827).await;
    let version = pinned_version_with_ssts(&[table], &[recovered.clone(), missing.clone()]);
    let local_store = in_memory_object_store();
    let bytes = store
        .store()
        .read(&store.get_sst_data_path(recovered.object_id), ..)
        .await
        .unwrap();
    local_store
        .upload(
            &format!("{}-42.sst", recovered.object_id.as_raw_id()),
            bytes,
        )
        .await
        .unwrap();
    // A fresh SstableStore has no cached metadata or remote objects. Recovery must use
    // only local inventory and the version, even after ownership becomes available.
    let store = mock_sstable_store().await;
    let cache = PinCache::new(
        local_store,
        u64::MAX,
        1,
        [recovered.object_id, missing.object_id],
    )
    .await
    .unwrap();
    let store = Arc::new(
        Arc::into_inner(store)
            .unwrap()
            .with_pin_cache(cache.clone()),
    );
    let mut config = test_refill_config();
    config.timeout = Duration::from_secs(60);
    let mut refiller = RefillTest::new(
        Role::Streaming,
        CacheRefillPolicy::Disabled,
        config,
        store,
        default_spawn_refill_task(),
        version.clone(),
    );
    refiller.replace_table_cache_refill_runtime_snapshot([(table, CacheRefillPolicy::Pin)].into());
    assert!(cache.get(recovered.object_id).is_some());
    // Block downloads to prove a full snapshot does not schedule missing-object backfill.
    let _permit = refiller
        .cache()
        .foyer
        .concurrency
        .clone()
        .acquire_owned()
        .await
        .unwrap();
    refiller.update_streaming_table_vnodes(table, Some(Bitmap::ones(VirtualNode::COUNT_FOR_TEST)));
    let mut snapshot = (*version).clone();
    snapshot.id = 2.into();
    refiller.receive_snapshot(snapshot);
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(1), apply_next_events(&mut refiller))
            .await
            .unwrap()
            .len(),
        1
    );

    assert!(cache.get(recovered.object_id).is_some());
    assert!(cache.get(missing.object_id).is_none());
    refiller.replace_table_cache_refill_runtime_snapshot(HashMap::new());
    assert!(
        cache.get(recovered.object_id).is_none(),
        "unpin still revokes recovered routes"
    );
}
