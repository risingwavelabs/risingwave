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
use risingwave_common::hash::VirtualNode;
use risingwave_common::util::epoch::test_epoch;
use risingwave_hummock_sdk::compact_task::CompactTask;
use risingwave_hummock_sdk::compaction_group::group_split::build_split_key;
use risingwave_hummock_sdk::key::{FullKey, TableKey, UserKey, prefix_slice_with_vnode};
use risingwave_hummock_sdk::key_range::KeyRange;
use risingwave_hummock_sdk::level::InputLevel;
use risingwave_hummock_sdk::sstable_info::SstableInfoInner;
use risingwave_hummock_sdk::version::{GroupDeltas, IntraLevelDelta};
use risingwave_hummock_sdk::{CompactionGroupId, EpochWithGap};
use risingwave_pb::hummock::compact_task::TaskType;
use risingwave_pb::hummock::hummock_version::Levels;
use risingwave_pb::hummock::{
    CompactionConfig, CompatibilityVersion, GroupConstruct, GroupDestroy, GroupMerge,
    HummockVersion as PbHummockVersion, Level, LevelType, OverlappingLevel, StateTableInfo,
    StateTableInfoDelta,
};
use tokio::sync::{Semaphore, mpsc};

use super::*;
use crate::hummock::TableHolder;
use crate::hummock::iterator::test_utils::mock_sstable_store;
use crate::hummock::pin_cache::PinCache;
use crate::hummock::pin_cache::test_utils::in_memory_object_store;
use crate::hummock::test_utils::{default_builder_opt_for_test, gen_test_sstable_with_table_ids};
use crate::hummock::value::HummockValue;

// Fixture setup only: registration and publication remain explicit in each test.
pub(super) async fn with_pin_cache(
    store: SstableStoreRef,
    capacity: u64,
) -> (SstableStoreRef, Arc<PinCache>) {
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

// Sorted rows define the physical table/vnode layout; one entry per block exposes boundaries.
pub(super) async fn sst_with_vnodes(
    store: &SstableStoreRef,
    object: u64,
    rows: &[(TableId, usize)],
) -> (TableHolder, SstableInfo) {
    let mut options = default_builder_opt_for_test();
    options.block_capacity = 1;
    let mut tables: Vec<_> = rows.iter().map(|(table, _)| table.as_raw_id()).collect();
    tables.dedup();
    gen_test_sstable_with_table_ids(
        options,
        object,
        rows.iter().map(|&(table, vnode)| {
            (
                FullKey {
                    user_key: UserKey::for_test(
                        table,
                        prefix_slice_with_vnode(VirtualNode::from_index(vnode), b"key"),
                    ),
                    epoch_with_gap: EpochWithGap::new_from_epoch(test_epoch(1)),
                },
                HummockValue::put(Bytes::from_static(b"value")),
            )
        }),
        store.clone(),
        tables,
    )
    .await
}

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

pub(super) fn intra_level_delta(
    deleted: &[SstableInfo],
    inserted: &[SstableInfo],
    group_version: u64,
) -> HummockVersionDelta {
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
                group_version,
            ))],
        },
    )]
    .into();
    delta
}

// Enumerate the authoritative version independently of the controller's selection/delta helpers.
fn reference_counts(
    version: &HummockVersion,
    pinned_tables: &HashSet<TableId>,
) -> HashMap<HummockSstableObjectId, u32> {
    let mut counts = HashMap::new();
    for (group_id, levels) in &version.levels {
        for sst in levels
            .l0
            .sub_levels
            .iter()
            .chain(&levels.levels)
            .flat_map(|level| &level.table_infos)
        {
            if sst.table_ids.iter().any(|table| {
                pinned_tables.contains(table)
                    && version
                        .state_table_info
                        .info()
                        .get(table)
                        .is_some_and(|info| info.compaction_group_id == *group_id)
            }) {
                *counts.entry(sst.object_id).or_default() += 1;
            }
        }
    }
    counts
}

fn assert_reference_counts(controller: &PinCacheRefillController) {
    assert_eq!(
        controller.object_ref_counts,
        reference_counts(&controller.planning_version, &controller.pinned_table_ids),
        "incremental counts must match the authoritative version"
    );
}

fn apply_and_check_version_deltas(
    controller: &mut PinCacheRefillController,
    deltas: &[HummockVersionDelta],
) -> (PinCacheRefillPlan, PinCacheObjectChanges) {
    let before = reference_counts(&controller.planning_version, &controller.pinned_table_ids);
    let mut version = (*controller.planning_version).clone();
    let mut deltas = deltas.to_vec();
    let mut sst_deltas = Vec::new();
    for delta in &mut deltas {
        delta.prev_id = version.id;
        delta.id = (version.id.as_raw_id() + 1).into();
        sst_deltas.extend(version.build_sst_delta_infos(delta));
        version.apply_version_delta(delta);
    }
    let after = reference_counts(&version, &controller.pinned_table_ids);
    let result = controller.apply_version_update(
        &sst_deltas,
        PinnedVersion::new(version, mpsc::unbounded_channel().0),
        Some(&deltas),
    );
    assert_reference_counts(controller);
    assert_eq!(
        result.1.inserted,
        after
            .keys()
            .filter(|id| !before.contains_key(*id))
            .copied()
            .collect()
    );
    assert_eq!(
        result.1.removed,
        before
            .keys()
            .filter(|id| !after.contains_key(*id))
            .copied()
            .collect()
    );
    result
}

// Mirrors Meta's apply_compact_task: per-input-level deletion followed by output insertion.
fn compaction_delta(
    version: &HummockVersion,
    group: CompactionGroupId,
    input: (u32, &SstableInfo),
    output: (u32, &SstableInfo),
) -> HummockVersionDelta {
    let group_version = version.levels[&group].compaction_group_version_id;
    let mut delta = HummockVersionDelta::default();
    delta.group_deltas.insert(
        group,
        GroupDeltas {
            group_deltas: vec![
                GroupDelta::IntraLevel(IntraLevelDelta::new(
                    input.0,
                    0,
                    [input.1.sst_id].into(),
                    vec![],
                    0,
                    group_version,
                )),
                GroupDelta::IntraLevel(IntraLevelDelta::new(
                    output.0,
                    0,
                    HashSet::new(),
                    vec![output.1.clone()],
                    0,
                    group_version,
                )),
            ],
        },
    );
    delta
}

#[tokio::test]
async fn test_membership_tracks_physical_references() {
    let tables = [TableId::from(233), TableId::from(234)];
    let branches = tables.map(|table| {
        SstableInfo::from(SstableInfoInner {
            object_id: 1001.into(),
            sst_id: (table.as_raw_id() as u64).into(),
            table_ids: vec![table],
            ..Default::default()
        })
    });
    let both = version_with_ssts(&branches);
    let mut only_b = (*version_with_ssts(&branches[1..])).clone();
    only_b.state_table_info = both.state_table_info.clone();
    let only_b = PinnedVersion::new(only_b, mpsc::unbounded_channel().0);
    let mut empty = (*both).clone();
    empty
        .levels
        .get_mut(&CompactionGroupId::new(1))
        .unwrap()
        .l0
        .sub_levels
        .clear();
    let empty = PinnedVersion::new(empty, mpsc::unbounded_channel().0);
    let mut controller = PinCacheRefillController::new(
        mock_sstable_store().await,
        empty.clone(),
        Arc::new(Semaphore::new(1)),
    );
    let changes = controller.replace_pinned_tables(
        tables.into(),
        std::slice::from_ref(&empty),
        RefillOwnership::default(),
    );
    assert_reference_counts(&controller);
    assert!(changes.inserted.is_empty() && changes.removed.is_empty());
    let (plan, changes) = controller.apply_version_update(
        &[SstDeltaInfo {
            insert_sst_infos: branches[1..].to_vec(),
            ..Default::default()
        }],
        only_b.clone(),
        Some(&[intra_level_delta(&[], &branches[1..], 0)]),
    );
    assert_reference_counts(&controller);
    assert_eq!(
        plan.objects.keys().copied().collect::<HashSet<_>>(),
        [1001.into()].into()
    );
    assert_eq!(changes.inserted, [1001.into()].into());
    // A new logical reference can require refill without adding physical membership.
    let (plan, changes) = controller.apply_version_update(
        &[SstDeltaInfo {
            insert_sst_infos: branches[..1].to_vec(),
            ..Default::default()
        }],
        both.clone(),
        Some(&[intra_level_delta(&[], &branches[..1], 0)]),
    );
    assert_reference_counts(&controller);
    assert_eq!(
        plan.objects.keys().copied().collect::<HashSet<_>>(),
        [1001.into()].into()
    );
    assert!(changes.inserted.is_empty() && changes.removed.is_empty());
    let (_, changes) = controller.apply_version_update(
        &[SstDeltaInfo {
            delete_sst_infos: vec![branches[0].clone()],
            ..Default::default()
        }],
        only_b,
        Some(&[intra_level_delta(&branches[..1], &[], 0)]),
    );
    assert_reference_counts(&controller);
    assert!(changes.inserted.is_empty() && changes.removed.is_empty());
    let (_, changes) = controller.apply_version_update(
        &[SstDeltaInfo {
            delete_sst_infos: vec![branches[1].clone()],
            ..Default::default()
        }],
        empty,
        Some(&[intra_level_delta(&branches[1..], &[], 0)]),
    );
    assert_reference_counts(&controller);
    assert_eq!(changes.removed, [1001.into()].into());

    // The planning version is now empty, but the resident snapshot still contains both SSTs.
    // Policy diffs must use the resident snapshot, while reference counts follow planning.
    let changes = controller.replace_pinned_tables(
        [tables[1]].into(),
        std::slice::from_ref(&both),
        RefillOwnership::default(),
    );
    assert_reference_counts(&controller);
    assert!(changes.inserted.is_empty() && changes.removed.is_empty());
    let changes = controller.replace_pinned_tables(
        HashSet::new(),
        std::slice::from_ref(&both),
        RefillOwnership::default(),
    );
    assert_reference_counts(&controller);
    assert_eq!(changes.removed, [1001.into()].into());
    let changes = controller.replace_pinned_tables(
        tables.into(),
        std::slice::from_ref(&both),
        RefillOwnership::default(),
    );
    assert_reference_counts(&controller);
    assert_eq!(changes.inserted, [1001.into()].into());

    // A full snapshot restores membership without scheduling a backfill.
    let (plan, changes) = controller.apply_version_update(&[], both.clone(), None);
    assert_reference_counts(&controller);
    assert!(plan.objects.is_empty());
    assert_eq!(changes.inserted, [1001.into()].into());
    assert!(changes.removed.is_empty());
    let (plan, changes) = controller.apply_version_update(&[], both.clone(), Some(&[]));
    assert_reference_counts(&controller);
    assert!(plan.objects.is_empty() && changes.inserted.is_empty() && changes.removed.is_empty());

    // Meta unregisters tables and prunes their SST projections in the same delta.
    for table in tables {
        let mut removed = HummockVersionDelta::default();
        removed.removed_table_ids.insert(table);
        removed.group_deltas.insert(
            1.into(),
            GroupDeltas {
                group_deltas: vec![GroupDelta::PruneTableIdsFromSsts([table].into())],
            },
        );
        // A trailing no-op ensures the controller checks the entire batch.
        let deltas = [removed, HummockVersionDelta::default()];
        let (_, changes) = apply_and_check_version_deltas(&mut controller, &deltas);
        assert!(changes.inserted.is_empty());
        assert_eq!(
            changes.removed,
            if table == tables[1] {
                [1001.into()].into()
            } else {
                HashSet::new()
            }
        );
    }
    assert!(
        controller
            .planning_version
            .state_table_info
            .info()
            .is_empty()
    );
}

#[tokio::test]
async fn test_pin_reference_counts_match_versions_across_batch_boundaries() {
    let tables = [TableId::from(233), TableId::from(234)];
    let group = CompactionGroupId::new(10);
    let split_group = CompactionGroupId::new(11);
    // Membership needs only immutable SST metadata; no cache or object-store fixture.
    let sst = |object: u64, table_ids: &[TableId]| {
        let key = |table| {
            FullKey::new(
                table,
                TableKey(VirtualNode::ZERO.to_be_bytes().to_vec()),
                test_epoch(1),
            )
            .encode()
            .into()
        };
        SstableInfo::from(SstableInfoInner {
            object_id: object.into(),
            sst_id: object.into(),
            table_ids: table_ids.to_vec(),
            key_range: KeyRange::new(key(table_ids[0]), key(*table_ids.last().unwrap())),
            ..Default::default()
        })
    };
    let shared = sst(1002u64, &tables);
    let output_a = sst(1003, &tables[..1]);
    let output_b = sst(1004, &tables[1..]);
    let mut pb = version_with_ssts(std::slice::from_ref(&shared)).to_protobuf();
    let mut levels = pb.levels.remove(&CompactionGroupId::new(1)).unwrap();
    levels.group_id = group;
    levels.levels = (1..=2)
        .map(|level_idx| Level {
            level_idx,
            level_type: LevelType::Nonoverlapping as i32,
            ..Default::default()
        })
        .collect();
    pb.levels.insert(group, levels);
    for info in pb.state_table_info.values_mut() {
        info.compaction_group_id = group;
        info.committed_epoch = test_epoch(1);
    }
    let initial = HummockVersion::from_rpc_protobuf(&pb);
    let mut version = initial.clone();
    let mut deltas = Vec::new();
    let push = |mut delta: HummockVersionDelta,
                version: &mut HummockVersion,
                deltas: &mut Vec<HummockVersionDelta>| {
        delta.prev_id = version.id;
        delta.id = (version.id.as_raw_id() + 1).into();
        version.apply_version_delta(&delta);
        deltas.push(delta);
    };

    // Match Meta's topology split, preserving each moved table's committed epoch.
    let mut split = HummockVersionDelta::default();
    split.group_deltas.insert(
        split_group,
        GroupDeltas {
            group_deltas: vec![GroupDelta::GroupConstruct(Box::new(GroupConstruct {
                parent_group_id: group,
                group_id: split_group,
                new_sst_start_id: 900.into(),
                version: CompatibilityVersion::LATEST as i32,
                split_key: Some(build_split_key(tables[1], VirtualNode::ZERO).to_vec()),
                group_config: Some(CompactionConfig {
                    max_level: 2,
                    ..Default::default()
                }),
                ..Default::default()
            }))],
        },
    );
    split.state_table_info_delta.insert(
        tables[1],
        StateTableInfoDelta {
            committed_epoch: test_epoch(1),
            compaction_group_id: split_group,
        },
    );
    push(split, &mut version, &mut deltas);
    assert_eq!(
        reference_counts(&version, &tables.into())[&shared.object_id],
        2
    );

    // Actual compactions replace live input with new physical outputs at the current group version.
    let a = version.levels[&group].l0.sub_levels[0].table_infos[0].clone();
    push(
        compaction_delta(&version, group, (0, &a), (1, &output_a)),
        &mut version,
        &mut deltas,
    );
    assert_eq!(
        reference_counts(&version, &tables.into())[&shared.object_id],
        1
    );

    let mut merge = HummockVersionDelta::default();
    merge.group_deltas.insert(
        group,
        GroupDeltas {
            group_deltas: vec![GroupDelta::GroupMerge(GroupMerge {
                left_group_id: group,
                right_group_id: split_group,
            })],
        },
    );
    merge.state_table_info_delta = tables
        .into_iter()
        .map(|table| {
            (
                table,
                StateTableInfoDelta {
                    committed_epoch: test_epoch(1),
                    compaction_group_id: group,
                },
            )
        })
        .collect();
    push(merge, &mut version, &mut deltas);

    // Meta's trivial move reuses the descriptor from a nonoverlapping level into an empty L2.
    let task = CompactTask {
        input_ssts: version.levels[&group]
            .levels
            .iter()
            .map(|level| InputLevel {
                level_idx: level.level_idx,
                level_type: level.level_type,
                table_infos: level.table_infos.clone(),
            })
            .collect(),
        target_level: 2,
        task_type: TaskType::Dynamic,
        ..Default::default()
    };
    assert!(task.is_trivial_move_task());
    let mut trivial_move = compaction_delta(&version, group, (1, &output_a), (2, &output_a));
    trivial_move.trivial_move = task.is_trivial_move_task();
    assert!(version.build_sst_delta_infos(&trivial_move).is_empty());
    push(trivial_move, &mut version, &mut deltas);
    assert!(version.levels[&group].levels[0].table_infos.is_empty());
    assert_eq!(
        version.levels[&group].levels[1].table_infos,
        vec![output_a.clone()],
        "the SDK must actually move the SST from L1 to L2"
    );
    let b = version.levels[&group].l0.sub_levels[0].table_infos[0].clone();
    push(
        compaction_delta(&version, group, (0, &b), (1, &output_b)),
        &mut version,
        &mut deltas,
    );
    assert!(!reference_counts(&version, &tables.into()).contains_key(&shared.object_id));

    // Meta drops A by pruning, then destroys the now-empty dynamic group when B is dropped.
    for table in tables {
        let mut dropped = HummockVersionDelta::default();
        dropped.removed_table_ids.insert(table);
        dropped.group_deltas.insert(
            group,
            GroupDeltas {
                group_deltas: vec![if table == tables[0] {
                    GroupDelta::PruneTableIdsFromSsts([table].into())
                } else {
                    GroupDelta::GroupDestroy(GroupDestroy {})
                }],
            },
        );
        push(dropped, &mut version, &mut deltas);
    }
    assert!(version.levels.is_empty());

    for batch_size in [1, 2, deltas.len()] {
        let initial = PinnedVersion::new(initial.clone(), mpsc::unbounded_channel().0);
        let mut controller = PinCacheRefillController::new(
            mock_sstable_store().await,
            initial.clone(),
            Arc::new(Semaphore::new(1)),
        );
        let changes =
            controller.replace_pinned_tables(tables.into(), &[initial], RefillOwnership::default());
        assert_eq!(changes.inserted, [shared.object_id].into());
        assert_reference_counts(&controller);
        for batch in deltas.chunks(batch_size) {
            let _ = apply_and_check_version_deltas(&mut controller, batch);
        }
        assert!(controller.object_ref_counts.is_empty());
        assert_eq!(*controller.planning_version, version);
    }
}
