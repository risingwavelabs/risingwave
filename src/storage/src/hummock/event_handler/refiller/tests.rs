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

//! Integration tests for backend composition and version publication.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration;

use futures::FutureExt;
use risingwave_common::bitmap::Bitmap;
use risingwave_common::config::Role;
use risingwave_common::config::streaming::CacheRefillPolicy;
use risingwave_common::hash::VirtualNode;
use risingwave_common::util::epoch::test_epoch;
use risingwave_hummock_sdk::compaction_group::group_split::split_sst_with_table_ids;
use risingwave_hummock_sdk::version::{
    GroupDelta, GroupDeltas, HummockVersion, HummockVersionDelta, IntraLevelDelta,
};
use risingwave_pb::id::TableId;
use tokio::sync::Semaphore;
use tokio::sync::mpsc::unbounded_channel;

use super::foyer::DataCacheRefillTaskGenerator;
use super::test_utils::{
    RefillTest, apply_next_events, deleted_sst, gen_test_sst_with_object_id, l0_insert_delta,
    mock_sstable_store_with_disk_cache, mock_sstable_store_with_pin_cache, pin_cache_for_test,
    pinned_version_with_ssts, test_refill_config, wait_for_pin_downloads,
};
use super::{CacheRefillConfig, SpawnRefillTask, SstDeltaInfo, default_spawn_refill_task};
use crate::hummock::local_version::pinned_version::PinnedVersion;
use crate::hummock::test_utils::gen_test_sstable_with_table_vnodes;

fn gated_foyer_refill() -> (Arc<Semaphore>, SpawnRefillTask) {
    let gate = Arc::new(Semaphore::new(0));
    let worker_gate = gate.clone();
    let spawn: SpawnRefillTask = Arc::new(move |_, _| {
        let gate = worker_gate.clone();
        tokio::spawn(async move {
            gate.acquire().await.unwrap().forget();
        })
    });
    (gate, spawn)
}

#[tokio::test]
async fn test_pin_refill_observes_inserts_alongside_table_truncation() {
    let table_id = TableId::from(233);
    let group: risingwave_pb::id::CompactionGroupId = 1.into();
    let sibling_table_id = TableId::from(234);
    let (sstable_store, pin_cache) = mock_sstable_store_with_pin_cache().await;
    let (_, sst_info) = gen_test_sst_with_object_id(table_id, sstable_store.clone(), 1001).await;

    let (_, old_sibling) =
        gen_test_sst_with_object_id(sibling_table_id, sstable_store.clone(), 1002).await;
    let removed = old_sibling.object_id;
    let base = pinned_version_with_ssts(&[table_id, sibling_table_id], &[old_sibling]);
    let mut refiller = RefillTest::new(
        Role::Streaming,
        test_refill_config(CacheRefillPolicy::Disabled),
        sstable_store.clone(),
        default_spawn_refill_task(),
        base.clone(),
    );
    refiller.replace_table_cache_refill_runtime_snapshot(
        HashMap::new(),
        HashSet::from([table_id, sibling_table_id]),
    );
    refiller
        .update_streaming_table_vnodes(table_id, Some(Bitmap::ones(VirtualNode::COUNT_FOR_TEST)));
    // Meta's epoch commit can append SSTs and truncate a staging table in the same group.
    let mut delta = l0_insert_delta(base.id(), test_epoch(233), vec![sst_info.clone()]);
    delta
        .group_deltas
        .get_mut(&group)
        .unwrap()
        .group_deltas
        .push(GroupDelta::PruneTableIdsFromSsts([sibling_table_id].into()));
    refiller.submit(vec![delta]);

    assert_eq!(apply_next_events(&mut refiller).await.len(), 1);
    assert!(pin_cache.get(sst_info.object_id).is_some());
    assert!(!pin_cache.is_registered(removed));
}

#[tokio::test]
async fn test_pin_policy_changes_preserve_delta_admission() {
    for queued_reset in [false, true] {
        let table = TableId::from(233);
        let (store, cache) = mock_sstable_store_with_pin_cache().await;
        let (_, info) = gen_test_sst_with_object_id(table, store.clone(), 1001).await;
        let (_, later) = gen_test_sst_with_object_id(table, store.clone(), 1002).await;
        let (gate, spawn) = gated_foyer_refill();
        let base = pinned_version_with_ssts(&[table], &[]);
        let mut refiller = RefillTest::new(
            Role::Streaming,
            test_refill_config(CacheRefillPolicy::Disabled),
            store,
            spawn,
            base.clone(),
        );
        refiller
            .update_streaming_table_vnodes(table, Some(Bitmap::ones(VirtualNode::COUNT_FOR_TEST)));
        if queued_reset {
            refiller.replace_table_cache_refill_runtime_snapshot(HashMap::new(), [table].into());
        }
        refiller.submit(vec![l0_insert_delta(
            1.into(),
            test_epoch(1),
            vec![info.clone()],
        )]);
        // Neither a late SET nor RESET followed by SET may warm an observed delta.
        refiller.replace_table_cache_refill_runtime_snapshot(HashMap::new(), HashSet::new());
        refiller.replace_table_cache_refill_runtime_snapshot(HashMap::new(), [table].into());
        assert!(
            cache.is_registered(info.object_id),
            "membership follows received versions, independently of refill intent"
        );
        gate.add_permits(1);
        assert_eq!(apply_next_events(&mut refiller).await.len(), 1);
        assert!(cache.get(info.object_id).is_none());

        // Policy changes must still admit future deltas for the same table.
        gate.add_permits(1);
        refiller.submit(vec![l0_insert_delta(
            2.into(),
            test_epoch(2),
            vec![later.clone()],
        )]);
        assert_eq!(apply_next_events(&mut refiller).await.len(), 1);
        assert!(cache.get(later.object_id).is_some());
        assert!(cache.get(info.object_id).is_none());
    }
}

#[tokio::test]
async fn test_pin_overlay_preserves_only_foyer_sibling() {
    // Put the pinned table last: a table-switch separator deliberately fails open for
    // the previous table's final block, whereas largest_key gives an exact vnode end.
    let pinned = TableId::from(234);
    let sibling = TableId::from(233);
    let (store, _cache_dir) = mock_sstable_store_with_disk_cache(None).await;
    let (sst, info) =
        gen_test_sstable_with_table_vnodes(&store, 860, &[(sibling, 0), (pinned, 0)]).await;
    let cache = pin_cache_for_test().await;
    let store = Arc::new(
        Arc::into_inner(store)
            .unwrap()
            .with_pin_cache(cache.clone()),
    );
    let spawn: SpawnRefillTask = Arc::new(move |plan, context| {
        assert_eq!(
            plan[0].insert_sst_infos.len(),
            1,
            "retain the physical object for its sibling"
        );
        for delete_sst_infos in [vec![], vec![deleted_sst(999.into())]] {
            let delta = SstDeltaInfo {
                delete_sst_infos,
                ..plan[0].clone()
            };
            let tasks = DataCacheRefillTaskGenerator {
                context: &context,
                delta: &delta,
                ssts: std::slice::from_ref(&sst),
            }
            .generate_unfiltered_tasks();
            assert!(!tasks.is_empty());
            for task in tasks {
                for index in task.blks {
                    assert_eq!(sst.meta.block_metas[index].table_id(), sibling);
                }
            }
        }
        tokio::spawn(async {})
    });
    let mut config = test_refill_config(CacheRefillPolicy::Disabled);
    config.data_refill_levels.insert(0);
    let mut refiller = RefillTest::new(
        Role::Serving,
        config,
        store,
        spawn,
        pinned_version_with_ssts(&[pinned, sibling], &[]),
    );
    refiller.replace_table_cache_refill_runtime_snapshot(
        [
            (sibling, CacheRefillPolicy::Serving),
            (pinned, CacheRefillPolicy::Serving),
        ]
        .into(),
        HashSet::from([pinned]),
    );
    refiller.replace_serving_table_vnode_mapping(
        [
            (pinned, Bitmap::ones(VirtualNode::COUNT_FOR_TEST)),
            (sibling, Bitmap::ones(VirtualNode::COUNT_FOR_TEST)),
        ]
        .into(),
    );
    refiller.submit(vec![l0_insert_delta(
        1.into(),
        test_epoch(1),
        vec![info.clone()],
    )]);
    assert_eq!(apply_next_events(&mut refiller).await.len(), 1);

    assert!(cache.get(info.object_id).is_some());
    // Pin withdrawal retires the published route through membership.
    refiller.replace_table_cache_refill_runtime_snapshot(HashMap::new(), HashSet::new());
    assert!(!cache.is_registered(info.object_id));
    assert!(cache.get(info.object_id).is_none());
}

#[tokio::test]
async fn test_pin_policy_change_preserves_old_version_references() {
    let sibling = TableId::from(233);
    let table = TableId::from(234);
    let (store, cache) = mock_sstable_store_with_pin_cache().await;
    let (_, shared) =
        gen_test_sstable_with_table_vnodes(&store, 830, &[(sibling, 0), (table, 0)]).await;
    let (_, target) = gen_test_sstable_with_table_vnodes(&store, 831, &[(table, 0)]).await;
    // Meta's commit correction can split one physical SST into references in two groups.
    let mut next_sst_id = 850.into();
    let (sibling_ref, table_ref) =
        split_sst_with_table_ids(&shared, &mut next_sst_id, 1, 1, vec![table]);
    let mut version =
        pinned_version_with_ssts(&[sibling, table], std::slice::from_ref(&sibling_ref))
            .to_protobuf();
    let table_group = 2.into();
    let source_group = version.state_table_info[&sibling].compaction_group_id;
    let mut table_levels = version.levels[&source_group].clone();
    table_levels.group_id = table_group;
    table_levels.l0.as_mut().unwrap().sub_levels[0].table_infos = vec![table_ref.clone().into()];
    version.levels.insert(table_group, table_levels);
    version
        .state_table_info
        .get_mut(&table)
        .unwrap()
        .compaction_group_id = table_group;
    for id in [sibling, table] {
        version
            .state_table_info
            .get_mut(&id)
            .unwrap()
            .committed_epoch = test_epoch(1);
    }
    let version = PinnedVersion::new(
        HummockVersion::from_rpc_protobuf(&version),
        unbounded_channel().0,
    );
    let mut compacted = HummockVersionDelta::default();
    compacted.prev_id = version.id();
    compacted.id = 2.into();
    compacted.group_deltas.insert(
        table_group,
        GroupDeltas {
            group_deltas: vec![GroupDelta::IntraLevel(IntraLevelDelta::new(
                0,
                0,
                [table_ref.sst_id].into(),
                vec![target.clone()],
                0,
                0,
            ))],
        },
    );
    let mut refiller = RefillTest::new(
        Role::Streaming,
        test_refill_config(CacheRefillPolicy::Disabled),
        store,
        default_spawn_refill_task(),
        version.clone(),
    );
    refiller.replace_table_cache_refill_runtime_snapshot(HashMap::new(), [sibling, table].into());
    let old_token = cache.prepare_refill(shared.object_id);
    refiller.submit(vec![compacted]);
    // RESET A while B's compaction is pending. The applied B reference still pins the
    // old file; looking only at planning would incorrectly unregister it through A.
    refiller.replace_table_cache_refill_runtime_snapshot(HashMap::new(), [table].into());
    assert_eq!(cache.prepare_refill(shared.object_id), old_token);
    assert!(cache.is_registered(shared.object_id));
    let events = tokio::time::timeout(Duration::from_secs(1), refiller.versions().next_events())
        .await
        .expect("membership-only updates need no download ownership");
    assert_eq!(events.len(), 1);

    assert!(cache.get(target.object_id).is_none());
    assert!(cache.is_registered(shared.object_id));
    refiller.publish(&events);
    assert!(!cache.is_registered(shared.object_id));
    assert!(cache.is_registered(target.object_id));
    assert!(cache.get(target.object_id).is_none());
}

#[tokio::test]
async fn test_foyer_failure_waits_for_pin() {
    let table = TableId::from(233);
    let ordinary_table = TableId::from(234);
    let (store, cache) = mock_sstable_store_with_pin_cache().await;
    let (_, pinned) = gen_test_sst_with_object_id(table, store.clone(), 832).await;
    let (_, ordinary) = gen_test_sst_with_object_id(ordinary_table, store.clone(), 834).await;
    store
        .store()
        .delete(&store.get_sst_data_path(ordinary.object_id))
        .await
        .unwrap();
    store.clear_meta_cache().await.unwrap();
    let foyer_finished = Arc::new(tokio::sync::Notify::new());
    let worker_finished = foyer_finished.clone();
    let default_spawn = default_spawn_refill_task();
    let spawn: SpawnRefillTask = Arc::new(move |deltas, context| {
        assert!(
            deltas
                .iter()
                .flat_map(|delta| &delta.insert_sst_infos)
                .any(|sst| sst.table_ids.contains(&ordinary_table))
        );
        let finished = worker_finished.clone();
        let task = default_spawn(deltas, context);
        tokio::spawn(async move {
            task.await.unwrap();
            finished.notify_one();
        })
    });
    let base = pinned_version_with_ssts(&[table, ordinary_table], &[]);
    let mut config = test_refill_config(CacheRefillPolicy::Enabled);
    config.timeout = Duration::from_secs(60);
    let mut refiller = RefillTest::new(Role::Both, config, store, spawn, base.clone());
    refiller.replace_table_cache_refill_runtime_snapshot(HashMap::new(), [table].into());
    refiller.replace_serving_table_vnode_mapping(
        [(ordinary_table, Bitmap::ones(VirtualNode::COUNT_FOR_TEST))].into(),
    );
    refiller.update_streaming_table_vnodes(table, Some(Bitmap::ones(VirtualNode::COUNT_FOR_TEST)));
    let permit = refiller
        .cache()
        .foyer
        .concurrency
        .clone()
        .acquire_owned()
        .await
        .unwrap();
    let delta = l0_insert_delta(base.id(), test_epoch(233), vec![pinned.clone(), ordinary]);
    refiller.submit(vec![delta]);
    wait_for_pin_downloads(&refiller, &[pinned.object_id]).await;
    tokio::time::timeout(Duration::from_secs(1), foyer_finished.notified())
        .await
        .expect("the real Foyer metadata error must finish before checking publication");
    assert!(
        refiller.versions().next_events().now_or_never().is_none(),
        "Foyer failure cannot bypass unfinished Pin work"
    );
    assert!(cache.get(pinned.object_id).is_none());
    drop(permit);
    assert_eq!(apply_next_events(&mut refiller).await.len(), 1);
    assert!(cache.get(pinned.object_id).is_some());
}

#[tokio::test(start_paused = true)]
async fn test_refill_timeouts_and_download_cancellation() {
    #[derive(Clone, Copy, Debug)]
    enum Case {
        PinTimeout,
        FoyerTimeout,
    }
    for case in [Case::PinTimeout, Case::FoyerTimeout] {
        let table = TableId::from(233);
        let (store, cache) = mock_sstable_store_with_pin_cache().await;
        let (_, info) = gen_test_sst_with_object_id(table, store.clone(), 810).await;
        let object = info.object_id;
        let mut inserted = vec![info.clone()];
        let mut tables = vec![table];
        let ordinary = TableId::from(234);
        if matches!(case, Case::FoyerTimeout) {
            inserted.push(
                gen_test_sst_with_object_id(ordinary, store.clone(), 811)
                    .await
                    .1,
            );
            tables.push(ordinary);
        }
        let mut config = test_refill_config(CacheRefillPolicy::Enabled);
        config.timeout = Duration::from_secs(1);
        if matches!(case, Case::PinTimeout) {
            config.timeout = Duration::from_millis(50);
        }
        let pin_timeout = config.timeout;
        let default_spawn = default_spawn_refill_task();
        let spawn: SpawnRefillTask = Arc::new(move |plan, mut context| {
            if matches!(case, Case::FoyerTimeout) {
                assert!(
                    plan.iter()
                        .flat_map(|delta| &delta.insert_sst_infos)
                        .any(|sst| sst.table_ids.contains(&ordinary))
                );
                context.config = Arc::new(CacheRefillConfig {
                    timeout: Duration::from_millis(1),
                    ..test_refill_config(CacheRefillPolicy::Enabled)
                });
                context.meta_refill_concurrency = Some(Arc::new(Semaphore::new(0)));
            } else if matches!(case, Case::PinTimeout) {
                // Foyer has no admitted work and a longer timeout. Only Pin's own
                // event timer can release its blocked download in this case.
                context.config = Arc::new(CacheRefillConfig {
                    timeout: Duration::from_secs(60),
                    ..test_refill_config(CacheRefillPolicy::Enabled)
                });
            }
            default_spawn(plan, context)
        });
        let mut refiller = RefillTest::new(
            Role::Both,
            config,
            store,
            spawn,
            pinned_version_with_ssts(&tables, &[]),
        );
        let mut permit = Some(
            refiller
                .cache()
                .foyer
                .concurrency
                .clone()
                .acquire_owned()
                .await
                .unwrap(),
        );
        refiller.replace_table_cache_refill_runtime_snapshot(HashMap::new(), [table].into());
        if matches!(case, Case::FoyerTimeout) {
            refiller.replace_serving_table_vnode_mapping(
                [(ordinary, Bitmap::ones(VirtualNode::COUNT_FOR_TEST))].into(),
            );
        }
        refiller.update_streaming_table_vnodes(
            table,
            Some(Bitmap::from_indices(VirtualNode::COUNT_FOR_TEST, [0])),
        );
        refiller.submit(vec![l0_insert_delta(1.into(), test_epoch(1), inserted)]);
        wait_for_pin_downloads(&refiller, &[info.object_id]).await;
        if !matches!(case, Case::PinTimeout) {
            assert!(
                tokio::time::timeout(Duration::from_millis(5), refiller.versions().next_events())
                    .await
                    .is_err(),
                "{case:?}"
            );
        }
        assert!(cache.get(info.object_id).is_none());
        if !matches!(case, Case::PinTimeout) {
            drop(permit.take());
        }
        if matches!(case, Case::PinTimeout) {
            tokio::time::advance(pin_timeout).await;
        }
        if matches!(case, Case::PinTimeout) {
            let events =
                refiller.versions().next_events().now_or_never().expect(
                    "Pin timeout starts at submission, even before the queue first polls it",
                );
            assert_eq!(events.len(), 1);
            refiller.publish(&events);
        } else {
            assert_eq!(apply_next_events(&mut refiller).await.len(), 1);
        }

        if matches!(case, Case::PinTimeout) {
            assert!(cache.get(info.object_id).is_none());
            assert!(
                refiller
                    .cache()
                    .pin_cache_refill
                    .as_ref()
                    .unwrap()
                    .executor
                    .has_pending_for_test(object),
                "the event timeout releases publication, not the executor-owned copy"
            );
            // Releasing the version must not cancel the executor's upload.
            drop(permit.take());
            tokio::time::timeout(Duration::from_secs(1), async {
                while cache.get(info.object_id).is_none() {
                    tokio::task::yield_now().await;
                }
            })
            .await
            .unwrap();
        }
        assert!(cache.get(info.object_id).is_some(), "{case:?}");
    }
}

#[tokio::test]
async fn test_pin_event_rejects_retired_table_projection_of_live_object() {
    let tables = [TableId::from(233), TableId::from(234)];
    for owns_survivor in [false, true] {
        let (store, cache) = mock_sstable_store_with_pin_cache().await;
        let (_, info) =
            gen_test_sstable_with_table_vnodes(&store, 842, &tables.map(|table| (table, 0))).await;
        let base = pinned_version_with_ssts(&tables, &[]);
        let (foyer_gate, spawn) = gated_foyer_refill();
        let mut config = test_refill_config(CacheRefillPolicy::Disabled);
        config.timeout = Duration::from_secs(5);
        let mut refiller = RefillTest::new(Role::Streaming, config, store, spawn, base.clone());
        refiller.replace_table_cache_refill_runtime_snapshot(HashMap::new(), tables.into());
        for table in tables.into_iter().take(1 + usize::from(owns_survivor)) {
            refiller.update_streaming_table_vnodes(
                table,
                Some(Bitmap::ones(VirtualNode::COUNT_FOR_TEST)),
            );
        }

        let group = 1.into();
        let inserted = l0_insert_delta(base.id(), test_epoch(1), vec![info.clone()]);
        let mut pruned = HummockVersionDelta::default();
        pruned.prev_id = inserted.id;
        pruned.id = (inserted.id.as_raw_id() + 1).into();
        // Match Meta's unregister path: remove A and prune its SST projections together.
        pruned.removed_table_ids.insert(tables[0]);
        pruned.group_deltas.insert(
            group,
            GroupDeltas {
                group_deltas: vec![GroupDelta::PruneTableIdsFromSsts([tables[0]].into())],
            },
        );
        let raw_deltas = [inserted, pruned];
        refiller.submit(raw_deltas.to_vec());
        foyer_gate.add_permits(1);
        let events = apply_next_events(&mut refiller).await;
        assert_eq!(events.len(), 1);
        assert_eq!(events.last().unwrap().new_pinned_version.id(), 3);

        assert!(
            cache.is_registered(info.object_id),
            "B still pins the object"
        );
        assert_eq!(
            cache.get(info.object_id).is_some(),
            owns_survivor,
            "event plans admit only projections surviving their target"
        );
    }
}
