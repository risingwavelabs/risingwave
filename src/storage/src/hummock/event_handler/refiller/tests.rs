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
use risingwave_hummock_sdk::CompactionGroupId;
use risingwave_hummock_sdk::compaction_group::group_split::split_sst_with_table_ids;
use risingwave_hummock_sdk::version::{
    GroupDelta, GroupDeltas, HummockVersion, HummockVersionDelta, IntraLevelDelta,
};
use risingwave_pb::hummock::{PbLevel, PbLevelType};
use risingwave_pb::id::TableId;
use tokio::sync::Semaphore;
use tokio::sync::mpsc::unbounded_channel;

use super::test_utils::{
    RefillTest, apply_next_events, gen_test_sst_with_object_id, l0_insert_delta,
    mock_sstable_store_with_disk_cache, mock_sstable_store_with_pin_cache, pin_cache_for_test,
    pinned_version_with_ssts, test_refill_config, wait_for_pin_downloads,
};
use super::{CacheRefillConfig, SpawnRefillTask, default_spawn_refill_task};
use crate::hummock::SstableBlockIndex;
use crate::hummock::iterator::test_utils::mock_sstable_store_with_object_store;
use crate::hummock::local_version::pinned_version::PinnedVersion;
use crate::hummock::recent_filter::simple::SimpleRecentFilter;
use crate::hummock::sstable_store::CachePolicy;
use crate::hummock::test_utils::gen_test_sstable_with_table_vnodes;
use crate::monitor::StoreLocalStatistic;

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
async fn test_pin_metadata_changes_preserve_delta_admission() {
    #[derive(Debug, PartialEq)]
    enum Change {
        LateSet,
        ResetThenSet,
        LateRegistration,
    }
    for change in [
        Change::LateSet,
        Change::ResetThenSet,
        Change::LateRegistration,
    ] {
        let table = TableId::from(233);
        let (store, cache) = mock_sstable_store_with_pin_cache().await;
        let (_, info) = gen_test_sst_with_object_id(table, store.clone(), 1001).await;
        let (_, later) = gen_test_sst_with_object_id(table, store.clone(), 1002).await;
        let (gate, spawn) = gated_foyer_refill();
        let base = pinned_version_with_ssts(&[table], &[]);
        let mut refiller = RefillTest::new(
            Role::Streaming,
            CacheRefillPolicy::Disabled,
            test_refill_config(),
            store,
            spawn,
            base.clone(),
        );
        if change != Change::LateRegistration {
            refiller.update_streaming_table_vnodes(
                table,
                Some(Bitmap::ones(VirtualNode::COUNT_FOR_TEST)),
            );
        }
        if change != Change::LateSet {
            refiller.replace_table_cache_refill_runtime_snapshot(HashMap::new(), [table].into());
        }
        refiller.submit(vec![l0_insert_delta(
            1.into(),
            test_epoch(1),
            vec![info.clone()],
        )]);
        // Receipt-time rejection is permanent for this event. Late metadata cannot create
        // download intent; a later delta may use the new policy/ownership normally.
        if change == Change::ResetThenSet {
            refiller.replace_table_cache_refill_runtime_snapshot(HashMap::new(), HashSet::new());
        }
        refiller.replace_table_cache_refill_runtime_snapshot(HashMap::new(), [table].into());
        if change == Change::LateRegistration {
            refiller.update_streaming_table_vnodes(
                table,
                Some(Bitmap::ones(VirtualNode::COUNT_FOR_TEST)),
            );
        }
        assert!(
            cache.is_registered(info.object_id),
            "membership follows received versions, independently of refill intent"
        );
        gate.add_permits(1);
        assert_eq!(apply_next_events(&mut refiller).await.len(), 1);
        assert!(cache.get(info.object_id).is_none(), "{change:?}");

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
async fn test_mixed_sst_refills_both_backends_before_publication() {
    // L0 commit, then compaction with zero, one, or both parent SSTs read.
    for (compaction, read_parents, legacy_unscoped) in [
        (false, 0, false),
        (true, 0, false),
        (true, 1, false),
        (true, 2, false),
        (true, 2, true),
    ] {
        let pinned = TableId::from(234);
        let sibling = TableId::from(233);
        let recent = SimpleRecentFilter::new(3, Duration::from_secs(60));
        let (store, _cache_dir) =
            mock_sstable_store_with_disk_cache(Some(Arc::new(recent.into()))).await;
        // SST upload warms the writer's recent filter. Serving must have independent caches.
        let writer = mock_sstable_store_with_object_store(store.store()).await;
        let rows = [(sibling, 0), (sibling, 2), (pinned, 0)];
        let (sst, info) = gen_test_sstable_with_table_vnodes(&writer, 860, &rows).await;
        assert_eq!(sst.block_count(), rows.len());
        // Commit and compaction are grouped by compaction group, not by cache backend.
        let (base, delta) = if compaction {
            let mut parents = Vec::new();
            for (index, rows) in [&rows[..1], &rows[1..]].into_iter().enumerate() {
                let (_, parent) =
                    gen_test_sstable_with_table_vnodes(&writer, 858 + index as u64, rows).await;
                if index < read_parents {
                    // Actual reads record both SST-level recent admission and block inheritance.
                    let mut stats = StoreLocalStatistic::default();
                    let parent_sst = store.sstable(&parent, &mut stats).await.unwrap();
                    for block in 0..parent_sst.block_count() {
                        store
                            .get(&parent_sst, block, CachePolicy::NotFill, &mut stats)
                            .await
                            .unwrap();
                    }
                    stats.discard();
                }
                parents.push(parent);
            }
            let mut version = pinned_version_with_ssts(&[pinned, sibling], &parents).to_protobuf();
            for table in version.state_table_info.values_mut() {
                table.committed_epoch = test_epoch(1);
            }
            version
                .levels
                .get_mut(&CompactionGroupId::from(1))
                .unwrap()
                .levels
                .push(PbLevel {
                    level_idx: 1,
                    level_type: PbLevelType::Nonoverlapping as i32,
                    ..Default::default()
                });
            let base = PinnedVersion::new(
                HummockVersion::from_rpc_protobuf(&version),
                unbounded_channel().0,
            );
            let mut delta = HummockVersionDelta::default();
            delta.prev_id = base.id();
            delta.id = 2.into();
            delta.group_deltas.insert(
                1.into(),
                GroupDeltas {
                    // Match apply_compact_task: delete inputs, then append output at L1.
                    group_deltas: vec![
                        GroupDelta::IntraLevel(IntraLevelDelta::new(
                            0,
                            0,
                            parents.iter().map(|parent| parent.sst_id).collect(),
                            vec![],
                            0,
                            0,
                        )),
                        GroupDelta::IntraLevel(IntraLevelDelta::new(
                            1,
                            0,
                            HashSet::new(),
                            vec![info.clone()],
                            0,
                            0,
                        )),
                    ],
                },
            );
            (base, delta)
        } else {
            (
                pinned_version_with_ssts(&[pinned, sibling], &[]),
                l0_insert_delta(1.into(), test_epoch(1), vec![info.clone()]),
            )
        };
        let cache = pin_cache_for_test().await;
        let store = Arc::new(
            Arc::into_inner(store)
                .unwrap()
                .with_pin_cache(cache.clone()),
        );
        let mut config = test_refill_config();
        config.data_refill_levels.extend([0, 1]);
        config.skip_recent_filter = false;
        config.skip_inheritance_filter = false;
        config.timeout = Duration::from_secs(60);
        let foyer_gate = Arc::new(Semaphore::new(0));
        let task_gate = foyer_gate.clone();
        let spawn: SpawnRefillTask = Arc::new(move |deltas, context| {
            let gate = task_gate.clone();
            tokio::spawn(async move {
                gate.acquire().await.unwrap().forget();
                default_spawn_refill_task()(deltas, context).await.unwrap();
            })
        });
        let mut refiller = RefillTest::new(
            if legacy_unscoped {
                Role::Both
            } else {
                Role::Serving
            },
            CacheRefillPolicy::Disabled,
            config,
            store.clone(),
            spawn,
            base,
        );
        refiller.replace_table_cache_refill_runtime_snapshot(
            [
                (
                    sibling,
                    if legacy_unscoped {
                        CacheRefillPolicy::Enabled
                    } else {
                        CacheRefillPolicy::Serving
                    },
                ),
                // Keep exclusion coverage when both switches are enabled; the legacy row
                // also proves that ordinary Disabled cannot veto Pin's independent switch.
                (
                    pinned,
                    if legacy_unscoped {
                        CacheRefillPolicy::Disabled
                    } else {
                        CacheRefillPolicy::Serving
                    },
                ),
            ]
            .into(),
            HashSet::from([pinned]),
        );
        let mut ownership = HashMap::from([(pinned, Bitmap::ones(VirtualNode::COUNT_FOR_TEST))]);
        if !legacy_unscoped {
            ownership.insert(sibling, Bitmap::ones(VirtualNode::COUNT_FOR_TEST));
        }
        refiller.replace_serving_table_vnode_mapping(ownership);
        // Foyer waits independently; occupying shared capacity would also stall Pin preparation.
        refiller.submit(vec![delta]);
        tokio::time::timeout(Duration::from_secs(5), async {
            while cache.get(info.object_id).is_none() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("Pin must download independently of Foyer");
        // File visibility precedes the Pin completion notifications. This only observes
        // pending publication; it does not claim that Foyer is the sole remaining condition.
        assert!(refiller.versions().next_events().now_or_never().is_none());
        foyer_gate.add_permits(1);
        assert_eq!(apply_next_events(&mut refiller).await.len(), 1);

        // Pin admission does not inherit Foyer's recent/block filters.
        let expected_blocks = if !compaction || read_parents == 2 {
            [true, true, false]
        } else {
            // Reading the first parent admits this SST delta, but cannot warm the second
            // parent's disjoint ordinary-table range through inheritance.
            [read_parents == 1, false, false]
        };
        // Wait for Foyer's asynchronous disk writes, then discard memory copies.
        store.block_cache().storage().wait().await;
        store.block_cache().memory().clear();
        for (index, expected) in expected_blocks.into_iter().enumerate() {
            let key = SstableBlockIndex {
                sst_id: info.object_id,
                block_idx: index as u64,
            };
            assert_eq!(
                store.block_cache().get(&key).await.unwrap().is_some(),
                expected,
                "compaction={compaction}, read_parents={read_parents}, block={index}"
            );
        }
        if !expected_blocks.iter().any(|cached| *cached) {
            continue;
        }
        // Remove both alternative sources: the ordinary table must still read from Foyer.
        refiller.replace_table_cache_refill_runtime_snapshot(HashMap::new(), HashSet::new());
        assert!(!cache.is_registered(info.object_id));
        assert!(cache.get(info.object_id).is_none());
        store
            .store()
            .delete(&store.get_sst_data_path(info.object_id))
            .await
            .unwrap();
        store.block_cache().memory().clear();
        let mut stats = StoreLocalStatistic::default();
        for (index, expected) in expected_blocks.into_iter().enumerate() {
            if expected {
                store
                    .get(&sst, index, CachePolicy::NotFill, &mut stats)
                    .await
                    .unwrap();
            }
        }
        assert_eq!(stats.cache_data_block_miss, 0);
        stats.discard();
    }
}

#[tokio::test]
async fn test_common_admission_rejects_before_backend_work() {
    // Meta sends an empty per-worker table mapping when this serving worker owns no slots.
    let ordinary = TableId::from(233);
    let pinned = TableId::from(234);
    let (store, _cache_dir) = mock_sstable_store_with_disk_cache(None).await;
    let writer = mock_sstable_store_with_object_store(store.store()).await;
    let (_, info) =
        gen_test_sstable_with_table_vnodes(&writer, 870, &[(ordinary, 0), (pinned, 0)]).await;
    let cache = pin_cache_for_test().await;
    let store = Arc::new(
        Arc::into_inner(store)
            .unwrap()
            .with_pin_cache(cache.clone()),
    );
    let mut config = test_refill_config();
    config.data_refill_levels.insert(0);
    config.timeout = Duration::from_secs(60);
    let mut refiller = RefillTest::new(
        Role::Serving,
        CacheRefillPolicy::Disabled,
        config,
        store.clone(),
        default_spawn_refill_task(),
        pinned_version_with_ssts(&[ordinary, pinned], &[]),
    );
    refiller.replace_table_cache_refill_runtime_snapshot(
        [(ordinary, CacheRefillPolicy::Both)].into(),
        [pinned].into(),
    );
    refiller.replace_serving_table_vnode_mapping(HashMap::new());
    let _meta = refiller
        .cache()
        .foyer
        .meta_refill_concurrency
        .as_ref()
        .unwrap()
        .clone()
        .acquire_owned()
        .await
        .unwrap();
    let _data = refiller
        .cache()
        .foyer
        .concurrency
        .clone()
        .acquire_owned()
        .await
        .unwrap();
    refiller.submit(vec![l0_insert_delta(
        1.into(),
        test_epoch(1),
        vec![info.clone()],
    )]);
    tokio::time::timeout(Duration::from_secs(5), apply_next_events(&mut refiller))
        .await
        .expect("rejected tables must not wait for backend I/O capacity");
    assert!(
        cache.is_registered(info.object_id),
        "refill filtering must not alter membership"
    );
    assert!(cache.get(info.object_id).is_none());
    assert!(
        !refiller
            .cache()
            .pin_cache_refill
            .as_ref()
            .unwrap()
            .executor
            .has_pending_for_test(info.object_id)
    );
    assert!(
        store
            .sstable_cached(info.object_id)
            .await
            .unwrap()
            .is_none()
    );
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
        CacheRefillPolicy::Disabled,
        test_refill_config(),
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

#[tokio::test(start_paused = true)]
async fn test_foyer_failure_or_timeout_waits_for_pin() {
    for metadata_error in [true, false] {
        let table = TableId::from(233);
        let ordinary_table = TableId::from(234);
        let (store, cache) = mock_sstable_store_with_pin_cache().await;
        let (_, pinned) = gen_test_sst_with_object_id(table, store.clone(), 832).await;
        let (_, ordinary) = gen_test_sst_with_object_id(ordinary_table, store.clone(), 834).await;
        if metadata_error {
            store
                .store()
                .delete(&store.get_sst_data_path(ordinary.object_id))
                .await
                .unwrap();
            store.clear_meta_cache().await.unwrap();
        }
        let finished = Arc::new(tokio::sync::Notify::new());
        let completion = finished.clone();
        let default_spawn = default_spawn_refill_task();
        let spawn: SpawnRefillTask = Arc::new(move |deltas, mut context| {
            assert!(
                deltas
                    .iter()
                    .flat_map(|delta| &delta.insert_sst_infos)
                    .any(|sst| sst.table_ids.contains(&ordinary_table))
            );
            context.config = Arc::new(CacheRefillConfig {
                // The error leg must finish before the one-second observation bound;
                // it cannot pass by falling back to its own timeout.
                timeout: if metadata_error {
                    Duration::from_secs(60)
                } else {
                    Duration::from_millis(1)
                },
                ..test_refill_config()
            });
            if !metadata_error {
                context.meta_refill_concurrency = Some(Arc::new(Semaphore::new(0)));
            }
            let finished = completion.clone();
            let task = default_spawn(deltas, context);
            tokio::spawn(async move {
                task.await.unwrap();
                finished.notify_one();
            })
        });
        let mut config = test_refill_config();
        config.timeout = Duration::from_secs(60);
        let base = pinned_version_with_ssts(&[table, ordinary_table], &[]);
        let mut refiller = RefillTest::new(
            Role::Both,
            CacheRefillPolicy::Enabled,
            config,
            store,
            spawn,
            base,
        );
        refiller.replace_table_cache_refill_runtime_snapshot(HashMap::new(), [table].into());
        refiller.replace_serving_table_vnode_mapping(
            [(ordinary_table, Bitmap::ones(VirtualNode::COUNT_FOR_TEST))].into(),
        );
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
        refiller.submit(vec![l0_insert_delta(
            1.into(),
            test_epoch(1),
            vec![pinned.clone(), ordinary],
        )]);
        wait_for_pin_downloads(&refiller, &[pinned.object_id]).await;
        tokio::time::timeout(Duration::from_secs(1), finished.notified())
            .await
            .expect("Foyer must finish while Pin is still blocked");
        assert!(
            refiller.versions().next_events().now_or_never().is_none(),
            "metadata_error={metadata_error}"
        );
        assert!(cache.get(pinned.object_id).is_none());
        drop(permit);
        assert_eq!(apply_next_events(&mut refiller).await.len(), 1);
        assert!(cache.get(pinned.object_id).is_some());
    }
}

#[tokio::test(start_paused = true)]
async fn test_pin_timeout_during_preparation_or_submitted_copy() {
    for preparing in [true, false] {
        let table = TableId::from(233);
        let (store, cache) = mock_sstable_store_with_pin_cache().await;
        let (_, info) = gen_test_sst_with_object_id(table, store.clone(), 810).await;
        let mut config = test_refill_config();
        config.timeout = Duration::from_millis(50);
        let timeout = config.timeout;
        let mut refiller = RefillTest::new(
            Role::Streaming,
            CacheRefillPolicy::Disabled,
            config,
            store,
            default_spawn_refill_task(),
            pinned_version_with_ssts(&[table], &[]),
        );
        refiller.replace_table_cache_refill_runtime_snapshot(HashMap::new(), [table].into());
        refiller
            .update_streaming_table_vnodes(table, Some(Bitmap::ones(VirtualNode::COUNT_FOR_TEST)));
        let semaphore = if preparing {
            refiller
                .cache()
                .foyer
                .meta_refill_concurrency
                .as_ref()
                .unwrap()
                .clone()
        } else {
            refiller.cache().foyer.concurrency.clone()
        };
        let permit = semaphore.acquire_owned().await.unwrap();
        refiller.submit(vec![l0_insert_delta(
            1.into(),
            test_epoch(1),
            vec![info.clone()],
        )]);
        if !preparing {
            wait_for_pin_downloads(&refiller, &[info.object_id]).await;
            tokio::time::advance(timeout).await;
        }
        // In the preparation case, paused time advances to the Pin timeout while its
        // metadata permit remains held. No object download can have been submitted.
        assert_eq!(apply_next_events(&mut refiller).await.len(), 1);
        assert!(cache.get(info.object_id).is_none());
        assert_eq!(
            refiller
                .cache()
                .pin_cache_refill
                .as_ref()
                .unwrap()
                .executor
                .has_pending_for_test(info.object_id),
            !preparing,
        );
        drop(permit);
        if preparing {
            assert!(
                !refiller
                    .cache()
                    .pin_cache_refill
                    .as_ref()
                    .unwrap()
                    .has_unfinished_requests()
            );
            continue;
        }
        tokio::time::timeout(Duration::from_secs(1), async {
            while cache.get(info.object_id).is_none() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("publication timeout must not cancel the executor-owned copy");
    }
}

#[tokio::test]
async fn test_pin_refill_after_table_pruning() {
    // A mixed commit truncates A while appending a shared SST. Separate raw deltas
    // append first and then unregister A. Only B's surviving projection may refill.
    for (mixed_commit, separate_events, owns_survivor) in [
        (true, false, true),
        (true, false, false),
        (false, false, false),
        (false, false, true),
        (false, true, false),
        (false, true, true),
    ] {
        let tables = [TableId::from(233), TableId::from(234)];
        let (store, cache) = mock_sstable_store_with_pin_cache().await;
        let (_, old) = gen_test_sst_with_object_id(tables[0], store.clone(), 841).await;
        let (_, inserted) =
            gen_test_sstable_with_table_vnodes(&store, 842, &tables.map(|table| (table, 0))).await;
        let base = pinned_version_with_ssts(&tables, std::slice::from_ref(&old));
        let mut refiller = RefillTest::new(
            Role::Streaming,
            CacheRefillPolicy::Disabled,
            test_refill_config(),
            store,
            default_spawn_refill_task(),
            base.clone(),
        );
        refiller.replace_table_cache_refill_runtime_snapshot(HashMap::new(), tables.into());
        for table in tables.into_iter().take(1 + usize::from(owns_survivor)) {
            refiller.update_streaming_table_vnodes(
                table,
                Some(Bitmap::ones(VirtualNode::COUNT_FOR_TEST)),
            );
        }
        let group = 1.into();
        let mut append = l0_insert_delta(base.id(), test_epoch(1), vec![inserted.clone()]);
        let prune = GroupDelta::PruneTableIdsFromSsts([tables[0]].into());
        let deltas = if mixed_commit {
            append
                .group_deltas
                .get_mut(&group)
                .unwrap()
                .group_deltas
                .push(prune);
            vec![append]
        } else {
            let mut unregister = HummockVersionDelta::default();
            unregister.prev_id = append.id;
            unregister.id = 3.into();
            unregister.removed_table_ids.insert(tables[0]);
            unregister.group_deltas.insert(
                group,
                GroupDeltas {
                    group_deltas: vec![prune],
                },
            );
            vec![append, unregister]
        };
        if separate_events {
            for delta in deltas {
                refiller.submit(vec![delta]);
            }
        } else {
            refiller.submit(deltas);
        }
        if !owns_survivor {
            // A later actor registration cannot retroactively admit B, including when Pin
            // must read the raw structural delta rather than the filtered SST summary.
            refiller.update_streaming_table_vnodes(
                tables[1],
                Some(Bitmap::ones(VirtualNode::COUNT_FOR_TEST)),
            );
        }
        let events = apply_next_events(&mut refiller).await;
        assert_eq!(events.len(), if separate_events { 2 } else { 1 });
        assert_eq!(
            events.last().unwrap().new_pinned_version.id(),
            if mixed_commit { 2 } else { 3 }
        );
        assert!(!cache.is_registered(old.object_id));
        assert!(
            cache.is_registered(inserted.object_id),
            "B still pins the shared object"
        );
        assert_eq!(
            cache.get(inserted.object_id).is_some(),
            owns_survivor,
            "mixed_commit={mixed_commit}"
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
        // Writer-populated metadata must not conceal whether the selected Pin batch loaded it.
        store.clear_meta_cache().await.unwrap();
        let serving_store = store.clone();
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
        let mut config = test_refill_config();
        config.timeout = Duration::from_secs(5);
        config.pin_cache_max_batch_size = batch_size;
        let mut refiller = RefillTest::new(
            Role::Both,
            CacheRefillPolicy::Enabled,
            config,
            store,
            spawn,
            base.clone(),
        );
        refiller.replace_table_cache_refill_runtime_snapshot(HashMap::new(), [table].into());
        refiller
            .update_streaming_table_vnodes(table, Some(Bitmap::ones(VirtualNode::COUNT_FOR_TEST)));

        // An earlier submitted Pin copy waits for data capacity while later events queue.
        let first_permit = refiller
            .cache()
            .foyer
            .concurrency
            .clone()
            .acquire_owned()
            .await
            .unwrap();
        let committed = l0_insert_delta(base.id(), test_epoch(233), vec![initial.clone()]);
        refiller.submit(vec![committed]);
        wait_for_pin_downloads(&refiller, &[initial.object_id]).await;
        assert!(cache.is_registered(initial.object_id));

        let inserted = l0_insert_delta(
            2.into(),
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
            refiller.submit(vec![delta]);
        }
        assert!(cache.is_registered(middle.object_id));
        assert!(cache.is_registered(last.object_id));
        assert!(cache.get(middle.object_id).is_none());
        drop(first_permit);
        tokio::time::timeout(Duration::from_secs(1), async {
            while refiller
                .cache()
                .pin_cache_refill
                .as_ref()
                .unwrap()
                .has_unfinished_requests()
            {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("Pin must select the next batch while all Foyer conditions remain blocked");
        let data_permit = if fail_target {
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
                    .cache()
                    .foyer
                    .concurrency
                    .clone()
                    .acquire_owned()
                    .await
                    .unwrap(),
            )
        };

        let appended = l0_insert_delta(4.into(), test_epoch(235), vec![later.clone()]);
        refiller.submit(vec![appended]);
        gates[3].add_permits(1);
        assert!(cache.is_registered(later.object_id));

        gates[0].add_permits(1);
        let first = apply_next_events(&mut refiller).await;
        assert_eq!(first.len(), 1);
        assert_eq!(first[0].new_pinned_version.id(), 2);
        assert_eq!(cache.get(middle.object_id).is_some(), batch_size == 1);
        assert_eq!(
            serving_store
                .sstable_cached(middle.object_id)
                .await
                .unwrap()
                .is_some(),
            batch_size == 1,
            "merged Pin batches must prune the intermediate object before metadata preparation",
        );
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
                refiller.versions().next_events().now_or_never().is_none(),
                "a merged target must wait for both events' independent Foyer attempts"
            );
        }
        if data_permit.is_some() {
            assert!(
                cache.get(later.object_id).is_none(),
                "publishing older work must not cancel the newer download"
            );
        }
        drop(data_permit);
        tokio::time::timeout(Duration::from_secs(1), async {
            while cache.get(later.object_id).is_none() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("the next Pin batch must finish before the previous batch's Foyer condition");
        assert_eq!(cache.get(last.object_id).is_some(), !fail_target);
        assert!(refiller.versions().next_events().now_or_never().is_none());

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
