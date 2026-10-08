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

use bytes::Bytes;
use risingwave_common::bitmap::Bitmap;
use risingwave_common::hash::VirtualNode;
use risingwave_common::util::epoch::test_epoch;
use risingwave_hummock_sdk::key::{FullKey, UserKey, prefix_slice_with_vnode};
use risingwave_hummock_sdk::sstable_info::SstableInfoInner;
use risingwave_hummock_sdk::version::{GroupDeltas, IntraLevelDelta};
use risingwave_hummock_sdk::{CompactionGroupId, EpochWithGap};
use risingwave_pb::hummock::hummock_version::Levels;
use risingwave_pb::hummock::{
    HummockVersion as PbHummockVersion, Level, OverlappingLevel, StateTableInfo,
    StateTableInfoDelta,
};
use tokio::sync::{Semaphore, mpsc};

use super::*;
use crate::hummock::iterator::test_utils::mock_sstable_store;
use crate::hummock::test_utils::{default_builder_opt_for_test, gen_test_sstable_with_table_ids};
use crate::hummock::value::HummockValue;

pub(super) fn version_with_ssts(ssts: &[SstableInfo]) -> PinnedVersion {
    let group = 1.into();
    let version = PbHummockVersion {
        id: 1.into(),
        levels: [(
            group,
            Levels {
                group_id: group,
                l0: Some(OverlappingLevel {
                    sub_levels: vec![Level {
                        table_infos: ssts.iter().cloned().map(Into::into).collect(),
                        ..Default::default()
                    }],
                    ..Default::default()
                }),
                ..Default::default()
            },
        )]
        .into(),
        state_table_info: ssts
            .iter()
            .flat_map(|sst| &sst.table_ids)
            .map(|&table| {
                (
                    table,
                    StateTableInfo {
                        compaction_group_id: group,
                        ..Default::default()
                    },
                )
            })
            .collect(),
        ..Default::default()
    };
    PinnedVersion::new(
        HummockVersion::from_rpc_protobuf(&version),
        mpsc::unbounded_channel().0,
    )
}

fn intra_level_delta(deleted: &[SstableInfo], inserted: &[SstableInfo]) -> HummockVersionDelta {
    let mut delta = HummockVersionDelta::default();
    delta.group_deltas = [(
        1.into(),
        GroupDeltas {
            group_deltas: vec![GroupDelta::IntraLevel(IntraLevelDelta::new(
                0,
                0,
                deleted.iter().map(|sst| sst.sst_id).collect(),
                inserted.to_vec(),
                0,
                0,
            ))],
        },
    )]
    .into();
    delta
}

#[tokio::test]
async fn test_membership_tracks_physical_references_without_registering_future_objects() {
    let tables = [TableId::from(233), TableId::from(234)];
    let branches = tables.map(|table| {
        SstableInfo::from(SstableInfoInner {
            object_id: 1001.into(),
            sst_id: (table.as_raw_id() as u64).into(),
            file_size: 8,
            table_ids: vec![table],
            ..Default::default()
        })
    });
    let both = version_with_ssts(&branches);
    let mut only_b = (*version_with_ssts(&branches[1..])).clone();
    only_b.state_table_info = both.state_table_info.clone();
    let only_b = PinnedVersion::new(only_b, mpsc::unbounded_channel().0);
    let store = mock_sstable_store().await;
    let cache = crate::hummock::pin_cache::PinCache::new(
        crate::hummock::pin_cache::test_utils::in_memory_object_store(),
        u64::MAX,
        1,
        2,
        [],
    )
    .await
    .unwrap();
    let store = Arc::new(
        Arc::into_inner(store)
            .unwrap()
            .with_pin_cache(cache.clone()),
    );
    let mut empty = (*both).clone();
    empty
        .levels
        .get_mut(&CompactionGroupId::new(1))
        .unwrap()
        .l0
        .sub_levels
        .clear();
    let empty = PinnedVersion::new(empty, mpsc::unbounded_channel().0);
    let mut controller =
        PinCacheRefillController::new(store, empty.clone(), Arc::new(Semaphore::new(1)));
    let changes = controller.replace_pinned_tables(
        tables.into(),
        std::slice::from_ref(&empty),
        RefillOwnership::default(),
    );
    assert!(changes.inserted.is_empty() && changes.removed.is_empty());
    let (candidates, changes) = controller.apply_version_update(
        &[SstDeltaInfo {
            insert_sst_infos: branches.to_vec(),
            ..Default::default()
        }],
        both.clone(),
        Some(&[intra_level_delta(&[], &branches)]),
    );
    assert_eq!(candidates, [1001.into()].into());
    assert_eq!(changes.inserted, [(1001.into(), 8)].into());
    assert!(
        !cache.is_registered(1001.into()),
        "planning must not register future objects"
    );
    cache.register_objects(changes.inserted);
    // Policy diffs use the resident snapshot and leave physical application to the gate.
    let changes = controller.replace_pinned_tables(
        [tables[1]].into(),
        std::slice::from_ref(&both),
        RefillOwnership::default(),
    );
    assert!(changes.inserted.is_empty() && changes.removed.is_empty());
    let changes = controller.replace_pinned_tables(
        HashSet::new(),
        std::slice::from_ref(&both),
        RefillOwnership::default(),
    );
    assert_eq!(changes.removed, [1001.into()].into());
    assert!(cache.is_registered(1001.into()));
    controller.unregister_objects(changes.removed);
    let changes = controller.replace_pinned_tables(
        tables.into(),
        std::slice::from_ref(&both),
        RefillOwnership::default(),
    );
    assert_eq!(changes.inserted, [(1001.into(), 8)].into());
    assert!(!cache.is_registered(1001.into()));
    cache.register_objects(changes.inserted);

    let (_, changes) = controller.apply_version_update(
        &[SstDeltaInfo {
            delete_sst_infos: vec![branches[0].clone()],
            ..Default::default()
        }],
        only_b.clone(),
        Some(&[intra_level_delta(&branches[..1], &[])]),
    );
    assert!(changes.inserted.is_empty() && changes.removed.is_empty());
    let (_, changes) = controller.apply_version_update(
        &[SstDeltaInfo {
            delete_sst_infos: vec![branches[1].clone()],
            ..Default::default()
        }],
        empty,
        Some(&[intra_level_delta(&branches[1..], &[])]),
    );
    assert_eq!(changes.removed, [1001.into()].into());
    assert!(
        cache.is_registered(1001.into()),
        "planning must not withdraw the applied object"
    );
    controller.unregister_objects(changes.removed);
    assert!(!cache.is_registered(1001.into()));

    // A full snapshot restores membership without scheduling a backfill.
    let (candidates, changes) = controller.apply_version_update(&[], both.clone(), None);
    assert!(candidates.is_empty());
    assert_eq!(changes.inserted, [(1001.into(), 8)].into());
    assert!(changes.removed.is_empty());
    let (candidates, changes) = controller.apply_version_update(&[], both.clone(), Some(&[]));
    assert!(candidates.is_empty() && changes.inserted.is_empty() && changes.removed.is_empty());

    // Table removal, registration, and SST pruning change membership without SST deltas.
    let mut removed = HummockVersionDelta::default();
    removed.removed_table_ids = tables.into();
    let mut registered = HummockVersionDelta::default();
    registered.state_table_info_delta.insert(
        tables[1],
        StateTableInfoDelta {
            committed_epoch: 1,
            compaction_group_id: 1.into(),
        },
    );
    let mut pruned = HummockVersionDelta::default();
    pruned.group_deltas.insert(
        1.into(),
        GroupDeltas {
            group_deltas: vec![GroupDelta::PruneTableIdsFromSsts(tables.into())],
        },
    );
    let mut version = (*both).clone();
    for (delta, present) in [(removed, false), (registered, true), (pruned, false)] {
        // A trailing no-op ensures the controller checks the entire batch.
        let mut raw_deltas = [delta, HummockVersionDelta::default()];
        for delta in &mut raw_deltas {
            delta.prev_id = version.id;
            delta.id = (version.id.as_raw_id() + 1).into();
            assert!(version.build_sst_delta_infos(delta).is_empty());
            version.apply_version_delta(delta);
        }
        let target = PinnedVersion::new(version.clone(), mpsc::unbounded_channel().0);
        let (candidates, changes) = controller.apply_version_update(&[], target, Some(&raw_deltas));
        assert!(candidates.is_empty());
        if present {
            assert_eq!(changes.inserted, [(1001.into(), 8)].into());
            assert!(changes.removed.is_empty());
        } else {
            assert_eq!(changes.removed, [1001.into()].into());
            assert!(changes.inserted.is_empty());
        }
    }
}

#[tokio::test]
async fn test_submission_intersects_projection_admission_and_current_ownership() {
    // Keep A last so its exact last key excludes vnode 255, without a table-switch separator.
    let b = TableId::from(233);
    let a = TableId::from(234);
    for (project_b, admit_b, current_vnode, empty_bitmap) in [
        // B is owned and admitted, but outside the logical projection.
        (false, true, Some(255), false),
        // B is owned and projected, but was not admitted by the plan.
        (true, false, Some(255), false),
        // A is unowned at submission, with an absent or empty bitmap.
        (false, false, None, false),
        (false, false, None, true),
        // A owns the matching vnode at submission.
        (false, false, Some(0), false),
    ] {
        let store = mock_sstable_store().await;
        let (_, info) = gen_test_sstable_with_table_ids(
            default_builder_opt_for_test(),
            702,
            [b, a].into_iter().map(|table| {
                (
                    FullKey {
                        user_key: UserKey::for_test(
                            table,
                            prefix_slice_with_vnode(VirtualNode::ZERO, b"key"),
                        ),
                        epoch_with_gap: EpochWithGap::new_from_epoch(test_epoch(233)),
                    },
                    HummockValue::put(Bytes::from_static(b"value")),
                )
            }),
            store.clone(),
            vec![b.as_raw_id(), a.as_raw_id()],
        )
        .await;
        let object = info.object_id;
        let version = version_with_ssts(std::slice::from_ref(&info));
        let cache = crate::hummock::pin_cache::PinCache::new(
            crate::hummock::pin_cache::test_utils::in_memory_object_store(),
            u64::MAX,
            1,
            2,
            [],
        )
        .await
        .unwrap();
        let store = Arc::new(
            Arc::into_inner(store)
                .unwrap()
                .with_pin_cache(cache.clone()),
        );
        let mut controller =
            PinCacheRefillController::new(store, version.clone(), Arc::new(Semaphore::new(1)));
        let changes =
            controller.replace_pinned_tables([a, b].into(), &[version], RefillOwnership::default());
        cache.register_objects(changes.inserted);
        let mut projection = info.get_inner();
        if !project_b {
            projection.table_ids = vec![a];
        }
        let plan = PinCacheRefillPlan::new(
            &[SstDeltaInfo {
                insert_sst_infos: vec![projection.into()],
                ..Default::default()
            }],
            &[object].into(),
            if admit_b { [a, b].into() } else { [a].into() },
        );
        let mut ownership = HashMap::from([(b, Bitmap::ones(VirtualNode::COUNT_FOR_TEST))]);
        if let Some(vnode) = current_vnode {
            ownership.insert(
                a,
                Bitmap::from_indices(VirtualNode::COUNT_FOR_TEST, [vnode]),
            );
        } else if empty_bitmap {
            ownership.insert(a, Bitmap::zeros(VirtualNode::COUNT_FOR_TEST));
        }
        let ownership = RefillOwnership {
            streaming: Some(&ownership),
            serving: None,
        };
        assert!(
            tokio::time::timeout(
                std::time::Duration::from_secs(1),
                controller.submit(plan, ownership).wait()
            )
            .await
            .unwrap()
        );
        assert_eq!(cache.get(object).is_some(), current_vnode == Some(0));
    }
}
