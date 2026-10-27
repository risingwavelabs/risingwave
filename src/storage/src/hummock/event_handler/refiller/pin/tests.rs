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

use super::super::default_spawn_refill_task;
use super::super::test_utils::{
    RefillTest, apply_next_events, gen_test_sst_with_object_id, l0_insert_delta,
    mock_sstable_store_with_pin_cache, pinned_version_with_ssts, test_refill_config,
    wait_for_pin_downloads,
};
use crate::hummock::test_utils::gen_test_sstable_with_table_vnodes;

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
        // Coarse admission intentionally permits overfetch: vnode 255 is not in this object.
        (WholeObject, false, Some(&[255][..]), [true, true]),
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
        let _metadata_permit = refiller
            .cache()
            .foyer
            .meta_refill_concurrency
            .as_ref()
            .unwrap()
            .clone()
            .acquire_owned()
            .await
            .unwrap();
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
    refiller.update_streaming_table_vnodes(table, Some(Bitmap::ones(VirtualNode::COUNT_FOR_TEST)));
    let permit = refiller
        .cache()
        .foyer
        .concurrency
        .clone()
        .acquire_owned()
        .await
        .unwrap();
    refiller.submit(vec![l0_insert_delta(1.into(), test_epoch(1), vec![info])]);
    refiller.submit(vec![l0_insert_delta(
        2.into(),
        test_epoch(2),
        vec![queued.clone()],
    )]);
    let pin = refiller.cache().pin_cache_refill.as_ref().unwrap();
    assert!(
        cache.is_registered(queued.object_id),
        "received membership is registered before waiting for I/O"
    );
    assert!(pin.executor.has_pending_for_test(object));
    assert!(pin.executor.has_pending_for_test(queued.object_id));
    drop(refiller);
    tokio::time::timeout(Duration::from_secs(1), async {
        while weak_store.upgrade().is_some() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("owner drop must release submitted work even without polling publication");
    assert!(cache.get(object).is_none());
    assert!(cache.get(queued.object_id).is_none());
    drop(permit);
}

#[tokio::test]
async fn test_pin_changes_after_submission() {
    #[derive(Clone, Copy, Debug)]
    enum Change {
        Policy,
        Ownership,
        EquivalentLane,
        OwnedRange,
        NewOwnership,
    }
    for change in [
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
            test_refill_config(CacheRefillPolicy::Disabled),
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
        wait_for_pin_downloads(&refiller, &[object]).await;
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
            Change::Policy => refiller
                .replace_table_cache_refill_runtime_snapshot(HashMap::new(), [tables[1]].into()),
            // These updates represent the union after read-version registration/destruction,
            // not an in-place rescale of a retained actor.
            Change::Ownership => refiller.update_streaming_table_vnodes(tables[0], None),
        }
        let cancelled = matches!(change, Change::Policy | Change::Ownership);
        if cancelled {
            let fresh_token = cache.prepare_refill(object);
            assert_ne!(fresh_token, original_token);
            assert!(cache.is_registered(object), "the sibling keeps membership");
            refiller.replace_table_cache_refill_runtime_snapshot(HashMap::new(), tables.into());
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
        assert_eq!(cache.get(object).is_some(), !cancelled, "{change:?}");
    }
}
