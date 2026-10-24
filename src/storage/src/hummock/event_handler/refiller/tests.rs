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

use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use futures::FutureExt;
use parking_lot::Mutex;
use risingwave_common::bitmap::Bitmap;
use risingwave_common::config::Role;
use risingwave_common::config::streaming::CacheRefillPolicy;
use risingwave_common::hash::VirtualNode;
use risingwave_common::util::epoch::test_epoch;
use risingwave_hummock_sdk::compaction_group::group_split::split_sst_with_table_ids;
use risingwave_hummock_sdk::sstable_info::SstableInfo;
use risingwave_hummock_sdk::version::{
    GroupDelta, GroupDeltas, HummockVersion, HummockVersionDelta, IntraLevelDelta,
};
use risingwave_hummock_sdk::{HummockSstableObjectId, HummockVersionId};
use risingwave_pb::hummock::StateTableInfoDelta;
use risingwave_pb::id::TableId;
use tokio::sync::Semaphore;
use tokio::sync::mpsc::unbounded_channel;

use super::foyer::DataCacheRefillTaskGenerator;
use super::test_utils::{
    RefillTest, apply_next_events, deleted_sst, gen_test_sst_with_object_id,
    mock_sstable_store_with_disk_cache, mock_sstable_store_with_pin_cache, pin_cache_for_test,
    pinned_version_for_test, pinned_version_with_ssts, test_refill_config,
};
use super::{CacheRefillConfig, SpawnRefillTask, SstDeltaInfo, default_spawn_refill_task};
use crate::hummock::local_version::pinned_version::PinnedVersion;
use crate::hummock::test_utils::gen_test_sstable_with_table_vnodes;

async fn wait_for_pin_downloads(refiller: &RefillTest, objects: &[HummockSstableObjectId]) {
    tokio::time::timeout(Duration::from_secs(1), async {
        while !objects.iter().all(|object| {
            refiller
                .cache
                .pin_cache_refill
                .as_ref()
                .unwrap()
                .executor
                .has_pending_for_test(*object)
        }) {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("metadata admission must submit the expected downloads");
}

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

fn l0_insert_delta(
    prev_id: HummockVersionId,
    committed_epoch: u64,
    ssts: Vec<SstableInfo>,
) -> HummockVersionDelta {
    let mut delta = HummockVersionDelta::default();
    delta.prev_id = prev_id;
    delta.id = (prev_id.as_raw_id() + 1).into();
    delta.state_table_info_delta = ssts
        .iter()
        .flat_map(|sst| &sst.table_ids)
        .map(|&table| {
            (
                table,
                StateTableInfoDelta {
                    compaction_group_id: 1.into(),
                    committed_epoch,
                },
            )
        })
        .collect();
    delta.group_deltas.insert(
        1.into(),
        GroupDeltas {
            group_deltas: vec![GroupDelta::NewL0SubLevel(ssts)],
        },
    );
    delta
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
    let mut sst_deltas = Vec::new();
    let update = refiller
        .versions
        .resolve_update(
            crate::hummock::event_handler::HummockVersionUpdate::VersionDeltas(vec![delta]),
            |before, delta| sst_deltas.extend(before.build_sst_delta_infos(delta)),
        )
        .unwrap();
    assert_eq!(
        update.event.new_pinned_version.levels[&group]
            .l0
            .sub_levels
            .iter()
            .flat_map(|level| &level.table_infos)
            .map(|sst| sst.object_id)
            .collect::<Vec<_>>(),
        vec![sst_info.object_id],
        "the SDK must apply both the append and sibling truncation",
    );
    refiller.start_cache_refill(
        sst_deltas,
        base,
        update.event.new_pinned_version,
        update.version_deltas.as_deref(),
    );

    assert_eq!(apply_next_events(&mut refiller).await.len(), 1);
    assert!(pin_cache.get(sst_info.object_id).is_some());
}

#[tokio::test]
async fn test_pin_policy_changes_preserve_delta_admission() {
    for queued_reset in [false, true] {
        let table = TableId::from(233);
        let (store, cache) = mock_sstable_store_with_pin_cache().await;
        let (_, info) = gen_test_sst_with_object_id(table, store.clone(), 1001).await;
        let (_, later) = gen_test_sst_with_object_id(table, store.clone(), 1002).await;
        let (gate, spawn) = gated_foyer_refill();
        let base = pinned_version_for_test();
        let next = pinned_version_with_ssts(&[table], std::slice::from_ref(&info));
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
            // Hold an earlier event active so RESET revokes a pending plan.
            refiller.start_cache_refill(vec![], base.clone(), base.clone(), Some(&[]));
        }
        refiller.start_cache_refill(
            vec![SstDeltaInfo {
                insert_sst_infos: vec![info.clone()],
                ..Default::default()
            }],
            base,
            next.clone(),
            Some(&[]),
        );
        // Neither a late SET nor RESET followed by SET may warm an observed delta.
        refiller.replace_table_cache_refill_runtime_snapshot(HashMap::new(), HashSet::new());
        refiller.replace_table_cache_refill_runtime_snapshot(HashMap::new(), [table].into());
        assert!(
            cache.is_registered(info.object_id),
            "membership follows received versions, independently of refill intent"
        );
        gate.add_permits(if queued_reset { 2 } else { 1 });
        let expected = if queued_reset { 2 } else { 1 };
        let mut published = 0;
        while published < expected {
            published += apply_next_events(&mut refiller).await.len();
        }
        assert_eq!(published, expected);
        assert!(cache.get(info.object_id).is_none());

        // Policy changes must still admit future deltas for the same table.
        gate.add_permits(1);
        refiller.start_cache_refill(
            vec![SstDeltaInfo {
                insert_sst_infos: vec![later.clone()],
                ..Default::default()
            }],
            next,
            pinned_version_with_ssts(&[table], &[info.clone(), later.clone()]),
            Some(&[]),
        );
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
        pinned_version_for_test(),
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
    refiller.start_cache_refill(
        vec![SstDeltaInfo {
            insert_sst_infos: vec![info.clone()],
            ..Default::default()
        }],
        pinned_version_for_test(),
        pinned_version_with_ssts(&[pinned, sibling], std::slice::from_ref(&info)),
        Some(&[]),
    );
    assert_eq!(apply_next_events(&mut refiller).await.len(), 1);

    assert!(cache.get(info.object_id).is_some());
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
    let mut target_version = (*version).clone();
    let deltas = target_version.build_sst_delta_infos(&compacted);
    target_version.apply_version_delta(&compacted);
    let target_version = version.new_with_local_version(target_version).unwrap();
    let mut refiller = RefillTest::new(
        Role::Streaming,
        test_refill_config(CacheRefillPolicy::Disabled),
        store,
        default_spawn_refill_task(),
        version.clone(),
    );
    refiller.replace_table_cache_refill_runtime_snapshot(HashMap::new(), [sibling, table].into());
    let old_token = cache.prepare_refill(shared.object_id);
    refiller.start_cache_refill(deltas, version, target_version, Some(&[compacted]));
    // RESET A while B's compaction is pending. The applied B reference still pins the
    // old file; looking only at planning would incorrectly unregister it through A.
    refiller.replace_table_cache_refill_runtime_snapshot(HashMap::new(), [table].into());
    assert_eq!(cache.prepare_refill(shared.object_id), old_token);
    assert!(cache.is_registered(shared.object_id));
    let events = tokio::time::timeout(Duration::from_secs(1), refiller.versions.next_events())
        .await
        .expect("membership-only updates need no download ownership");
    assert_eq!(events.len(), 1);

    assert!(cache.get(target.object_id).is_none());
    assert!(cache.is_registered(shared.object_id));
    refiller.on_versions_published(&events);
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
        .cache
        .foyer
        .concurrency
        .clone()
        .acquire_owned()
        .await
        .unwrap();
    let delta = l0_insert_delta(base.id(), test_epoch(233), vec![pinned.clone(), ordinary]);
    let mut version = (*base).clone();
    let sst_deltas = version.build_sst_delta_infos(&delta);
    version.apply_version_delta(&delta);
    let target = base.new_with_local_version(version).unwrap();
    refiller.start_cache_refill(sst_deltas, base, target, Some(&[delta]));
    wait_for_pin_downloads(&refiller, &[pinned.object_id]).await;
    tokio::time::timeout(Duration::from_secs(1), foyer_finished.notified())
        .await
        .expect("the real Foyer metadata error must finish before checking publication");
    assert!(
        refiller.versions.next_events().now_or_never().is_none(),
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
        OwnershipChange,
    }
    for case in [Case::PinTimeout, Case::FoyerTimeout, Case::OwnershipChange] {
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
                // batch timer can release its blocked download in this case.
                context.config = Arc::new(CacheRefillConfig {
                    timeout: Duration::from_secs(60),
                    ..test_refill_config(CacheRefillPolicy::Enabled)
                });
            }
            default_spawn(plan, context)
        });
        let mut refiller =
            RefillTest::new(Role::Both, config, store, spawn, pinned_version_for_test());
        let mut permit = Some(
            refiller
                .cache
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
        refiller.start_cache_refill(
            vec![SstDeltaInfo {
                insert_sst_infos: inserted.clone(),
                ..Default::default()
            }],
            pinned_version_for_test(),
            pinned_version_with_ssts(&tables, &inserted),
            Some(&[]),
        );
        wait_for_pin_downloads(&refiller, &[info.object_id]).await;
        assert!(
            tokio::time::timeout(Duration::from_millis(5), refiller.versions.next_events())
                .await
                .is_err(),
            "{case:?}"
        );
        assert!(cache.get(info.object_id).is_none());
        if matches!(case, Case::OwnershipChange) {
            // Destroying the last local instance removes this table's streaming ownership.
            refiller.update_streaming_table_vnodes(table, None);
        }
        if !matches!(case, Case::PinTimeout) {
            drop(permit.take());
        }
        if matches!(case, Case::PinTimeout) {
            tokio::time::advance(pin_timeout).await;
        }
        assert_eq!(apply_next_events(&mut refiller).await.len(), 1);

        if matches!(case, Case::PinTimeout) {
            assert!(cache.get(info.object_id).is_none());
            assert!(
                refiller
                    .cache
                    .pin_cache_refill
                    .as_ref()
                    .unwrap()
                    .executor
                    .has_pending_for_test(object),
                "the batch timeout releases publication, not the executor-owned copy"
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
        assert_eq!(
            cache.get(info.object_id).is_some(),
            !matches!(case, Case::OwnershipChange),
            "{case:?}"
        );
    }
}

#[tokio::test]
async fn test_pin_batch_rejects_retired_table_projection_of_live_object() {
    let tables = [TableId::from(233), TableId::from(234)];
    for (separate_events, owns_survivor) in [(false, false), (true, false), (true, true)] {
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

        // Hold an empty active batch so both updates below wait for one submission boundary.
        let mut active = (*base).clone();
        active.id = 2.into();
        let active = base.new_with_local_version(active).unwrap();
        refiller.start_cache_refill(vec![], base, active.clone(), Some(&[]));
        let group = 1.into();
        let inserted = l0_insert_delta(2.into(), test_epoch(1), vec![info.clone()]);
        let mut pruned = HummockVersionDelta::default();
        pruned.prev_id = 3.into();
        pruned.id = 4.into();
        // Match Meta's unregister path: remove A and prune its SST projections together.
        pruned.removed_table_ids.insert(tables[0]);
        pruned.group_deltas.insert(
            group,
            GroupDeltas {
                group_deltas: vec![GroupDelta::PruneTableIdsFromSsts([tables[0]].into())],
            },
        );
        let raw_deltas = [inserted, pruned];
        let mut previous = active;
        let mut version = (*previous).clone();
        let mut extracted = Vec::new();
        for delta in &raw_deltas {
            let infos = version.build_sst_delta_infos(delta);
            assert_eq!(infos.is_empty(), delta.id == 4);
            extracted.extend(infos);
            version.apply_version_delta(delta);
            if separate_events || delta.id == 4 {
                let target = previous.new_with_local_version(version.clone()).unwrap();
                refiller.start_cache_refill(
                    std::mem::take(&mut extracted),
                    previous,
                    target.clone(),
                    Some(if separate_events {
                        std::slice::from_ref(delta)
                    } else {
                        &raw_deltas
                    }),
                );
                previous = target;
            }
        }
        assert_eq!(
            version.levels[&group].l0.sub_levels[0].table_infos[0].table_ids,
            vec![tables[1]],
            "the physical object survives, but its A projection has retired"
        );
        foyer_gate.add_permits(1);
        assert_eq!(apply_next_events(&mut refiller).await.len(), 1);
        foyer_gate.add_permits(if separate_events { 2 } else { 1 });
        let events = apply_next_events(&mut refiller).await;
        assert_eq!(events.len(), if separate_events { 2 } else { 1 });
        assert_eq!(events.last().unwrap().new_pinned_version.id(), 4);

        assert!(
            cache.is_registered(info.object_id),
            "B still pins the object"
        );
        assert_eq!(
            cache.get(info.object_id).is_some(),
            owns_survivor,
            "merged batches admit only projections surviving their selected target"
        );
    }
}

#[tokio::test]
async fn test_pin_commit_compaction_batch_publication() {
    for (batch_size, fail_target, slow_commit_foyer) in [
        (1, false, false),
        (16, false, false),
        (16, true, false),
        (16, false, true),
    ] {
        let table = TableId::from(233);
        let ordinary_table = TableId::from(234);
        let (store, cache) = mock_sstable_store_with_pin_cache().await;
        let mut infos = Vec::new();
        for object in 839..=842 {
            infos.push(
                gen_test_sst_with_object_id(table, store.clone(), object)
                    .await
                    .1,
            );
        }
        let [initial, middle, last, later] = infos.try_into().unwrap();
        let ordinary = gen_test_sst_with_object_id(ordinary_table, store.clone(), 843)
            .await
            .1;
        let ordinary_output = gen_test_sst_with_object_id(ordinary_table, store.clone(), 844)
            .await
            .1;
        if fail_target {
            store
                .store()
                .delete(&store.get_sst_data_path(last.object_id))
                .await
                .unwrap();
            store.clear_meta_cache().await.unwrap();
        }
        let base = pinned_version_with_ssts(&[table, ordinary_table], &[]);
        let gates = std::array::from_fn::<_, 4, _>(|_| Arc::new(Semaphore::new(0)));
        let waiting = Arc::new(Mutex::new(VecDeque::from(gates.clone())));
        let output = ordinary_output.object_id;
        let completed = Arc::new(AtomicUsize::new(0));
        let worker_completed = completed.clone();
        let spawn: SpawnRefillTask = Arc::new(move |deltas, _| {
            let mut waiting = waiting.lock();
            if waiting.len() == 2 {
                assert!(
                    deltas
                        .iter()
                        .flat_map(|delta| &delta.insert_sst_infos)
                        .any(|sst| sst.object_id == output),
                    "the final Foyer condition must contain ordinary compaction work"
                );
            }
            let gate = waiting.pop_front().unwrap();
            let completed = worker_completed.clone();
            tokio::spawn(async move {
                gate.acquire().await.unwrap().forget();
                completed.fetch_add(1, Ordering::SeqCst);
            })
        });
        let mut config = test_refill_config(CacheRefillPolicy::Enabled);
        config.timeout = Duration::from_secs(5);
        config.pin_cache_max_batch_size = batch_size;
        let mut refiller = RefillTest::new(Role::Both, config, store, spawn, base.clone());
        refiller.replace_table_cache_refill_runtime_snapshot(HashMap::new(), [table].into());
        refiller
            .update_streaming_table_vnodes(table, Some(Bitmap::ones(VirtualNode::COUNT_FOR_TEST)));

        // A real earlier Pin attempt holds metadata admission while two later events queue.
        let first_permit = refiller
            .cache
            .foyer
            .meta_refill_concurrency
            .as_ref()
            .unwrap()
            .clone()
            .acquire_owned()
            .await
            .unwrap();
        let committed = l0_insert_delta(base.id(), test_epoch(233), vec![initial.clone()]);
        let mut version = (*base).clone();
        let infos = version.build_sst_delta_infos(&committed);
        version.apply_version_delta(&committed);
        let mut previous = base.new_with_local_version(version).unwrap();
        refiller.start_cache_refill(infos, base, previous.clone(), Some(&[committed]));
        assert!(cache.is_registered(initial.object_id));
        assert!(
            !refiller
                .cache
                .pin_cache_refill
                .as_ref()
                .unwrap()
                .executor
                .has_pending_for_test(initial.object_id)
        );

        let inserted = l0_insert_delta(
            previous.id(),
            test_epoch(234),
            vec![middle.clone(), ordinary.clone()],
        );
        let mut compacted = HummockVersionDelta::default();
        compacted.prev_id = inserted.id;
        compacted.id = 4.into();
        compacted.group_deltas.insert(
            1.into(),
            GroupDeltas {
                // Match Meta's separate deletion and insertion operations. Only the
                // second sublevel is compacted; the earlier SST remains in L0.
                group_deltas: vec![
                    GroupDelta::IntraLevel(IntraLevelDelta::new(
                        0,
                        0,
                        [middle.sst_id, ordinary.sst_id].into(),
                        vec![],
                        0,
                        0,
                    )),
                    GroupDelta::IntraLevel(IntraLevelDelta::new(
                        0,
                        2,
                        HashSet::new(),
                        vec![last.clone(), ordinary_output],
                        0,
                        0,
                    )),
                ],
            },
        );
        for delta in [inserted, compacted] {
            let mut version = (*previous).clone();
            let infos = version.build_sst_delta_infos(&delta);
            version.apply_version_delta(&delta);
            let target = previous.new_with_local_version(version).unwrap();
            refiller.start_cache_refill(infos, previous, target.clone(), Some(&[delta]));
            previous = target;
        }
        assert!(cache.is_registered(middle.object_id));
        assert!(cache.is_registered(last.object_id));
        assert!(cache.get(middle.object_id).is_none());
        drop(first_permit);
        tokio::time::timeout(Duration::from_secs(1), async {
            while !refiller.cache.pin_cache_refill.as_ref().unwrap().is_idle() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("Pin attempts must settle while all Foyer conditions remain blocked");
        let meta_permit = if fail_target {
            None
        } else {
            tokio::time::timeout(Duration::from_secs(1), async {
                while cache.get(last.object_id).is_none() {
                    tokio::task::yield_now().await;
                }
            })
            .await
            .unwrap();
            Some(
                refiller
                    .cache
                    .foyer
                    .meta_refill_concurrency
                    .as_ref()
                    .unwrap()
                    .clone()
                    .acquire_owned()
                    .await
                    .unwrap(),
            )
        };

        let appended = l0_insert_delta(previous.id(), test_epoch(235), vec![later.clone()]);
        let mut version = (*previous).clone();
        let infos = version.build_sst_delta_infos(&appended);
        version.apply_version_delta(&appended);
        let target = previous.new_with_local_version(version).unwrap();
        refiller.start_cache_refill(infos, previous, target, Some(&[appended]));
        gates[3].add_permits(1);
        assert!(cache.is_registered(later.object_id));

        gates[0].add_permits(1);
        let first = apply_next_events(&mut refiller).await;
        assert_eq!(first.len(), 1);
        assert_eq!(first[0].new_pinned_version.id(), 2);
        assert_eq!(cache.get(middle.object_id).is_some(), batch_size == 1);
        assert!(cache.is_registered(middle.object_id));
        // A later ready target must not bypass the commit's independent Foyer attempt.
        let (ready_foyer, blocked_foyer) = if slow_commit_foyer { (2, 1) } else { (1, 2) };
        gates[ready_foyer].add_permits(1);
        tokio::time::timeout(Duration::from_secs(1), async {
            while completed.load(Ordering::SeqCst) != 3 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("only the selected Foyer task must remain unfinished");
        if batch_size == 1 {
            let events = apply_next_events(&mut refiller).await;
            assert_eq!(events.len(), 1);
            assert_eq!(events[0].new_pinned_version.id(), 3);
        }
        for _ in 0..2 {
            assert!(
                refiller.versions.next_events().now_or_never().is_none(),
                "a merged target must wait for both events' independent Foyer attempts"
            );
        }
        if meta_permit.is_some() {
            assert!(
                cache.get(later.object_id).is_none(),
                "publishing older work must not abort the newer metadata preparation"
            );
        }
        drop(meta_permit);
        tokio::time::timeout(Duration::from_secs(1), async {
            while cache.get(later.object_id).is_none() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("the next Pin batch must finish before the previous batch's Foyer condition");
        assert_eq!(cache.get(last.object_id).is_some(), !fail_target);
        assert!(refiller.versions.next_events().now_or_never().is_none());

        gates[blocked_foyer].add_permits(1);
        let events = apply_next_events(&mut refiller).await;
        let ids = events
            .iter()
            .map(|event| event.new_pinned_version.id().as_raw_id())
            .collect::<Vec<_>>();
        let first_id = if batch_size == 1 { 4 } else { 3 };
        let published = *ids.last().unwrap();
        assert!(published >= 4);
        assert_eq!(ids, (first_id..=published).collect::<Vec<_>>());
        if published < 5 {
            let events = apply_next_events(&mut refiller).await;
            assert_eq!(events.len(), 1);
            assert_eq!(events[0].new_pinned_version.id(), 5);
        }
        assert!(!cache.is_registered(middle.object_id));
        assert!(cache.is_registered(last.object_id));
        assert!(cache.get(later.object_id).is_some());
    }
}
