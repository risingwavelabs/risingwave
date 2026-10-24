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
use std::sync::Arc;
use std::time::Duration;

use risingwave_common::bitmap::Bitmap;
use risingwave_common::config::Role;
use risingwave_common::config::streaming::CacheRefillPolicy;
use risingwave_common::hash::VirtualNode;
use risingwave_hummock_sdk::compaction_group::group_split::split_sst_with_table_ids;
use risingwave_pb::id::TableId;

use super::super::test_utils::{
    RefillTest, apply_next_events, gen_test_sst_with_object_id, mock_sstable_store_with_pin_cache,
    pinned_version_with_ssts, test_refill_config,
};
use super::super::{SstDeltaInfo, default_spawn_refill_task};
use crate::hummock::iterator::test_utils::mock_sstable_store;
use crate::hummock::pin_cache::PinCache;
use crate::hummock::pin_cache::test_utils::in_memory_object_store;
use crate::hummock::test_utils::gen_test_sstable_with_table_vnodes;

#[tokio::test]
async fn test_pin_metadata_admission_intersects_projection_policy_and_ownership() {
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
        (AOnly, true, Some(&[255][..]), [false, false]),
        (WholeObject, false, Some(&[255]), [false, false]),
        (AOnly, false, None, [false, false]),
        (AOnly, false, Some(&[]), [false, false]),
        (AOnly, false, Some(&[0]), [true, true]),
        (AOnly, false, Some(&[128]), [true, true]),
        (SplitReferences, true, Some(&[255]), [true, false]),
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
            test_refill_config(CacheRefillPolicy::Disabled),
            store,
            default_spawn_refill_task(),
            base.clone(),
        );
        refiller.replace_table_cache_refill_runtime_snapshot(
            HashMap::new(),
            if pin_b { [a, b].into() } else { [a].into() },
        );
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
        let target = pinned_version_with_ssts(&[a, b], &inserted);
        refiller.start_cache_refill(
            vec![SstDeltaInfo {
                insert_sst_infos: inserted,
                ..Default::default()
            }],
            base,
            target,
            Some(&[]),
        );
        apply_next_events(&mut refiller).await;

        let actual = [702, 703].map(|object| cache.get(object.into()).is_some());
        assert_eq!(
            actual, expected,
            "projection={projection:?}, pin_b={pin_b}, a_vnodes={a_vnodes:?}"
        );
    }
}

#[tokio::test]
async fn test_pin_preparation_owner_drop() {
    for poll_before_drop in [false, true] {
        let table = TableId::from(233);
        let (store, cache) = mock_sstable_store_with_pin_cache().await;
        let (_, info) = gen_test_sst_with_object_id(table, store.clone(), 875).await;
        let (_, queued) = gen_test_sst_with_object_id(table, store.clone(), 876).await;
        let object = info.object_id;
        let weak_store = Arc::downgrade(&store);
        let base = pinned_version_with_ssts(&[table], &[]);
        let mut refiller = RefillTest::new(
            Role::Streaming,
            test_refill_config(CacheRefillPolicy::Disabled),
            store,
            default_spawn_refill_task(),
            base.clone(),
        );
        refiller.replace_table_cache_refill_runtime_snapshot(HashMap::new(), [table].into());
        refiller
            .update_streaming_table_vnodes(table, Some(Bitmap::ones(VirtualNode::COUNT_FOR_TEST)));
        // Metadata must stay blocked before any download Work is allocated.
        let meta_permit = refiller
            .cache
            .foyer
            .meta_refill_concurrency
            .as_ref()
            .unwrap()
            .clone()
            .acquire_owned()
            .await
            .unwrap();
        let weak_executor =
            Arc::downgrade(&refiller.cache.pin_cache_refill.as_ref().unwrap().executor);
        let target = pinned_version_with_ssts(&[table], std::slice::from_ref(&info));
        refiller.start_cache_refill(
            vec![SstDeltaInfo {
                insert_sst_infos: vec![info.clone()],
                ..Default::default()
            }],
            base,
            target.clone(),
            Some(&[]),
        );
        refiller.start_cache_refill(
            vec![SstDeltaInfo {
                insert_sst_infos: vec![queued.clone()],
                ..Default::default()
            }],
            target,
            pinned_version_with_ssts(&[table], &[info, queued.clone()]),
            Some(&[]),
        );
        assert!(
            cache.is_registered(queued.object_id),
            "received membership is registered while refill preparation remains queued"
        );
        assert!(
            !refiller
                .cache
                .pin_cache_refill
                .as_ref()
                .unwrap()
                .executor
                .has_pending_for_test(object)
        );
        if poll_before_drop {
            assert!(
                tokio::time::timeout(Duration::from_millis(5), refiller.versions.next_events())
                    .await
                    .is_err()
            );
        }
        drop(refiller);
        assert!(
            weak_executor.upgrade().is_none(),
            "preparation must not retain the executor owner"
        );
        tokio::time::timeout(Duration::from_secs(1), async {
            while weak_store.upgrade().is_some() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("owner drop must abort metadata preparation and release its context");
        assert!(cache.get(object).is_none());
        assert!(cache.get(queued.object_id).is_none());
        drop(meta_permit);
    }
}

#[tokio::test]
async fn test_pin_metadata_preparation_cancels_before_download_allocation() {
    // Streaming updates represent unions refreshed after read-version registration or
    // destruction. They do not model an in-place vnode update of a retained actor.
    #[derive(Clone, Copy)]
    enum Change {
        EquivalentLane,
        NewOwnership,
        Policy,
        Ownership,
    }
    for change in [
        Change::EquivalentLane,
        Change::NewOwnership,
        Change::Policy,
        Change::Ownership,
    ] {
        let tables = [TableId::from(233), TableId::from(234)];
        let (store, cache) = mock_sstable_store_with_pin_cache().await;
        let (_, info) =
            gen_test_sstable_with_table_vnodes(&store, 874, &[(tables[0], 0), (tables[1], 0)])
                .await;
        let object = info.object_id;
        let base = pinned_version_with_ssts(&tables, &[]);
        let mut config = test_refill_config(CacheRefillPolicy::Disabled);
        config.timeout = Duration::from_secs(60);
        let mut refiller = RefillTest::new(
            Role::Both,
            config,
            store,
            default_spawn_refill_task(),
            base.clone(),
        );
        refiller.replace_table_cache_refill_runtime_snapshot(HashMap::new(), tables.into());
        refiller.update_streaming_table_vnodes(
            tables[0],
            Some(Bitmap::ones(VirtualNode::COUNT_FOR_TEST)),
        );
        if !matches!(change, Change::NewOwnership) {
            refiller.update_streaming_table_vnodes(
                tables[1],
                Some(Bitmap::from_indices(VirtualNode::COUNT_FOR_TEST, [255])),
            );
        }
        let meta_permit = refiller
            .cache
            .foyer
            .meta_refill_concurrency
            .as_ref()
            .unwrap()
            .clone()
            .acquire_owned()
            .await
            .unwrap();
        let target = pinned_version_with_ssts(&tables, std::slice::from_ref(&info));
        refiller.start_cache_refill(
            vec![SstDeltaInfo {
                insert_sst_infos: vec![info],
                ..Default::default()
            }],
            base,
            target,
            Some(&[]),
        );
        let original_token = cache.prepare_refill(object);
        assert!(
            !refiller
                .cache
                .pin_cache_refill
                .as_ref()
                .unwrap()
                .executor
                .has_pending_for_test(object)
        );
        match change {
            Change::EquivalentLane => {
                refiller.replace_serving_table_vnode_mapping(
                    [(tables[0], Bitmap::ones(VirtualNode::COUNT_FOR_TEST))].into(),
                );
                refiller.update_streaming_table_vnodes(tables[0], None);
            }
            Change::NewOwnership => refiller.update_streaming_table_vnodes(
                tables[1],
                Some(Bitmap::ones(VirtualNode::COUNT_FOR_TEST)),
            ),
            Change::Policy => refiller
                .replace_table_cache_refill_runtime_snapshot(HashMap::new(), [tables[1]].into()),
            Change::Ownership => refiller.update_streaming_table_vnodes(
                tables[0],
                Some(Bitmap::from_indices(VirtualNode::COUNT_FOR_TEST, [255])),
            ),
        }
        if matches!(change, Change::Policy | Change::Ownership) {
            let fresh_token = cache.prepare_refill(object);
            assert_ne!(fresh_token, original_token);
            assert!(cache.is_registered(object), "B retains membership");
            // Restoring admission cannot restart preparation or revoke its replacement token.
            refiller.replace_table_cache_refill_runtime_snapshot(HashMap::new(), tables.into());
            refiller.update_streaming_table_vnodes(
                tables[0],
                Some(Bitmap::ones(VirtualNode::COUNT_FOR_TEST)),
            );
            assert_eq!(cache.prepare_refill(object), fresh_token);
            tokio::time::timeout(Duration::from_secs(1), apply_next_events(&mut refiller))
                .await
                .unwrap();

            assert!(
                !refiller
                    .cache
                    .pin_cache_refill
                    .as_ref()
                    .unwrap()
                    .executor
                    .has_pending_for_test(object)
            );
            assert!(cache.get(object).is_none());
        } else {
            assert_eq!(cache.prepare_refill(object), original_token);
            drop(meta_permit);
            apply_next_events(&mut refiller).await;

            assert!(cache.get(object).is_some());
        }
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
    let mut config = test_refill_config(CacheRefillPolicy::Disabled);
    config.timeout = Duration::from_secs(60);
    let mut refiller = RefillTest::new(
        Role::Streaming,
        config,
        store,
        default_spawn_refill_task(),
        version.clone(),
    );
    refiller.replace_table_cache_refill_runtime_snapshot(HashMap::new(), [table].into());
    assert!(cache.get(recovered.object_id).is_some());
    // Block metadata to prove a full snapshot does not schedule missing-object backfill.
    let _permit = refiller
        .cache
        .foyer
        .meta_refill_concurrency
        .as_ref()
        .unwrap()
        .clone()
        .acquire_owned()
        .await
        .unwrap();
    refiller.update_streaming_table_vnodes(table, Some(Bitmap::ones(VirtualNode::COUNT_FOR_TEST)));
    let mut snapshot = (*version).clone();
    snapshot.id = 2.into();
    let snapshot = version.new_with_local_version(snapshot).unwrap();
    refiller.start_cache_refill(vec![], version, snapshot, None);
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(1), apply_next_events(&mut refiller))
            .await
            .unwrap()
            .len(),
        1
    );

    assert!(cache.get(recovered.object_id).is_some());
    assert!(cache.get(missing.object_id).is_none());
    refiller.replace_table_cache_refill_runtime_snapshot(HashMap::new(), HashSet::new());
    assert!(
        cache.get(recovered.object_id).is_none(),
        "unpin still revokes recovered routes"
    );
}
