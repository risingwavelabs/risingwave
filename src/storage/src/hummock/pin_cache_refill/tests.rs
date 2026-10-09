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

use risingwave_hummock_sdk::CompactionGroupId;
use risingwave_hummock_sdk::sstable_info::SstableInfoInner;
use risingwave_hummock_sdk::version::{GroupDeltas, IntraLevelDelta};
use risingwave_pb::hummock::hummock_version::Levels;
use risingwave_pb::hummock::{
    HummockVersion as PbHummockVersion, Level, OverlappingLevel, StateTableInfo,
    StateTableInfoDelta,
};
use tokio::sync::mpsc;

use super::*;

fn version_with_ssts(ssts: &[SstableInfo]) -> PinnedVersion {
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

#[test]
fn test_membership_tracks_physical_references() {
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
    let mut empty = (*both).clone();
    empty
        .levels
        .get_mut(&CompactionGroupId::new(1))
        .unwrap()
        .l0
        .sub_levels
        .clear();
    let empty = PinnedVersion::new(empty, mpsc::unbounded_channel().0);
    let mut controller = PinCacheRefillController::new(empty.clone());
    let changes = controller.replace_pinned_tables(tables.into(), std::slice::from_ref(&empty));
    assert!(changes.inserted.is_empty() && changes.removed.is_empty());
    let (candidates, changes) = controller.apply_version_update(
        &[SstDeltaInfo {
            insert_sst_infos: branches[1..].to_vec(),
            ..Default::default()
        }],
        only_b.clone(),
        Some(&[intra_level_delta(&[], &branches[1..])]),
    );
    assert_eq!(candidates, [1001.into()].into());
    assert_eq!(changes.inserted, [(1001.into(), 8)].into());
    // A new logical reference can require refill without adding physical membership.
    let (candidates, changes) = controller.apply_version_update(
        &[SstDeltaInfo {
            insert_sst_infos: branches[..1].to_vec(),
            ..Default::default()
        }],
        both.clone(),
        Some(&[intra_level_delta(&[], &branches[..1])]),
    );
    assert_eq!(candidates, [1001.into()].into());
    assert!(changes.inserted.is_empty() && changes.removed.is_empty());
    let (_, changes) = controller.apply_version_update(
        &[SstDeltaInfo {
            delete_sst_infos: vec![branches[0].clone()],
            ..Default::default()
        }],
        only_b,
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

    // The planning version is now empty, but the resident snapshot still contains both SSTs.
    // Policy diffs must use the resident snapshot, while reference counts follow planning.
    let changes = controller.replace_pinned_tables([tables[1]].into(), std::slice::from_ref(&both));
    assert!(changes.inserted.is_empty() && changes.removed.is_empty());
    let changes = controller.replace_pinned_tables(HashSet::new(), std::slice::from_ref(&both));
    assert_eq!(changes.removed, [1001.into()].into());
    let changes = controller.replace_pinned_tables(tables.into(), std::slice::from_ref(&both));
    assert_eq!(changes.inserted, [(1001.into(), 8)].into());

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
