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

use risingwave_common::util::epoch::test_epoch;
use risingwave_hummock_sdk::level::Levels;
use risingwave_hummock_sdk::sstable_info::SstableInfoInner;
use risingwave_hummock_sdk::version::{GroupDeltas, HummockVersionStateTableInfo};
use risingwave_pb::hummock::{StateTableInfo, StateTableInfoDelta};

use super::*;

#[test]
fn test_refill_candidates_are_independent_of_membership_changes() {
    let tables = [TableId::new(233), TableId::new(234)];
    let pinned = tables.into();
    let group = 1.into();
    let object: HummockSstableObjectId = 1001.into();
    let references = tables.map(|table| {
        SstableInfo::from(SstableInfoInner {
            object_id: 1001.into(),
            sst_id: (table.as_raw_id() as u64).into(),
            table_ids: vec![table],
            ..Default::default()
        })
    });
    let version_with = |ssts: &[SstableInfo]| {
        let mut version = HummockVersion::default();
        let mut levels = Levels {
            group_id: group,
            ..Default::default()
        };
        levels
            .l0
            .sub_levels
            .push(risingwave_hummock_sdk::level::Level {
                table_infos: ssts.to_vec(),
                ..risingwave_hummock_sdk::level::Level::default()
            });
        version.levels.insert(group, levels);
        version.state_table_info = HummockVersionStateTableInfo::from_protobuf_owned(
            tables
                .map(|table| {
                    (
                        table,
                        StateTableInfo {
                            compaction_group_id: group,
                            ..Default::default()
                        },
                    )
                })
                .into(),
        );
        version
    };
    let both = version_with(&references);
    let only_b = version_with(&references[1..]);
    let observed = [SstDeltaInfo {
        insert_sst_infos: references.to_vec(),
        ..Default::default()
    }];
    // O already belongs to B; observing the new A reference still creates refill intent.
    let mut inserted = both.version_delta_after();
    inserted.group_deltas.insert(
        group,
        GroupDeltas {
            group_deltas: vec![GroupDelta::NewL0SubLevel(references[..1].to_vec())],
        },
    );
    let plan = PinCacheRefillPlan::from_version_update(
        &[SstDeltaInfo {
            insert_sst_infos: references[..1].to_vec(),
            ..Default::default()
        }],
        &both,
        Some(&[inserted]),
        &pinned,
    );
    assert_eq!(
        plan.objects.keys().copied().collect::<HashSet<_>>(),
        [1001.into()].into()
    );
    assert_eq!(plan.objects[&object].admitted_tables, [tables[0]].into());
    // The authoritative target can drop one logical reference while preserving the object.
    let mut plan = PinCacheRefillPlan::new(
        observed.iter().flat_map(|delta| &delta.insert_sst_infos),
        &pinned,
    );
    plan.retain_in_version(&only_b, &pinned);
    assert_eq!(plan.objects[&object].admitted_tables, [tables[1]].into());
    plan.retain_in_version(&version_with(&[]), &pinned);
    assert!(plan.objects.is_empty());
    // Neither a restored snapshot nor an empty delta payload introduces refill intent.
    for raw in [None, Some([].as_slice())] {
        assert!(
            PinCacheRefillPlan::from_version_update(&[], &both, raw, &pinned)
                .objects
                .is_empty()
        );
    }
    // Structural raw changes can retain insertions omitted by the SST summary.
    let mut target = version_with(&[]);
    let mut raw = target.version_delta_after();
    for table in tables {
        raw.state_table_info_delta.insert(
            table,
            StateTableInfoDelta {
                committed_epoch: test_epoch(233),
                compaction_group_id: group,
            },
        );
    }
    raw.group_deltas.insert(
        group,
        GroupDeltas {
            group_deltas: vec![
                GroupDelta::NewL0SubLevel(references[..1].to_vec()),
                GroupDelta::PruneTableIdsFromSsts([tables[1]].into()),
            ],
        },
    );
    let summary = target.build_sst_delta_infos(&raw);
    target.apply_version_delta(&raw);
    let plan = PinCacheRefillPlan::from_version_update(&summary, &target, Some(&[raw]), &pinned);
    assert_eq!(plan.objects[&object].admitted_tables, [tables[0]].into());
}
