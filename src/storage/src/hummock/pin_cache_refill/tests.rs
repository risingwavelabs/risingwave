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

use risingwave_hummock_sdk::sstable_info::SstableInfoInner;
use risingwave_pb::hummock::hummock_version::Levels;
use risingwave_pb::hummock::{
    HummockVersion as PbHummockVersion, Level, OverlappingLevel, StateTableInfo,
};
use tokio::sync::{Semaphore, mpsc};

use super::*;
use crate::hummock::iterator::test_utils::mock_sstable_store;

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
    let only_b = version_with_ssts(&branches[1..]);
    for update in [
        PinCacheMembershipUpdate::Delta,
        PinCacheMembershipUpdate::Rebuild,
    ] {
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
        let empty = version_with_ssts(&[]);
        let mut controller =
            PinCacheRefillController::new(store, empty.clone(), Arc::new(Semaphore::new(1)));
        controller.replace_pinned_tables(tables.into(), &[empty]);
        let (candidates, changes) = controller.apply_version_update(
            &[SstDeltaInfo {
                insert_sst_infos: branches.to_vec(),
                ..Default::default()
            }],
            both.clone(),
            update,
        );
        assert_eq!(candidates, [1001.into()].into());
        assert_eq!(changes.inserted, [(1001.into(), 8)].into());
        assert!(
            !cache.is_registered(1001.into()),
            "planning must not register future objects"
        );
        cache.register_objects(changes.inserted);
        let (_, changes) = controller.apply_version_update(
            &[SstDeltaInfo {
                delete_sst_infos: vec![branches[0].clone()],
                ..Default::default()
            }],
            only_b.clone(),
            update,
        );
        assert!(changes.inserted.is_empty() && changes.removed.is_empty());
        let (_, changes) = controller.apply_version_update(
            &[SstDeltaInfo {
                delete_sst_infos: vec![branches[1].clone()],
                ..Default::default()
            }],
            version_with_ssts(&[]),
            update,
        );
        assert_eq!(changes.removed, [1001.into()].into());
        assert!(
            cache.is_registered(1001.into()),
            "planning must not withdraw the applied object"
        );
        controller.unregister_objects(changes.removed);
        assert!(!cache.is_registered(1001.into()));
    }
}
