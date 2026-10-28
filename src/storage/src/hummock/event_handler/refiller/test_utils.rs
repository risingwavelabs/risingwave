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

//! Shared fixtures for backend and coordinator tests; each helper has multiple test owners.

use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;

use ::foyer::{
    BlockEngineConfig, CacheBuilder, DeviceBuilder, FsDeviceBuilder, HybridCacheBuilder,
    PsyncIoEngineConfig,
};
use risingwave_common::config::MetricLevel;
use risingwave_common::util::epoch::test_epoch;
use risingwave_hummock_sdk::key::FullKey;
use risingwave_hummock_sdk::sstable_info::{SstableInfo, SstableInfoInner};
use risingwave_hummock_sdk::version::{
    GroupDelta, GroupDeltas, HummockVersion, HummockVersionDelta,
};
use risingwave_hummock_sdk::{EpochWithGap, HummockSstableObjectId, HummockVersionId};
use risingwave_pb::hummock::hummock_version::PbLevels;
use risingwave_pb::hummock::{
    PbHummockVersion, PbLevel, PbOverlappingLevel, PbStateTableInfo, StateTableInfoDelta,
};
use risingwave_pb::id::TableId;
use tokio::sync::mpsc::unbounded_channel;

use super::CacheRefillConfig;
pub(super) use crate::hummock::event_handler::hummock_event_handler::tests::RefillTest;
use crate::hummock::event_handler::version_update::VersionUpdateEvent;
use crate::hummock::iterator::test_utils::{iterator_test_table_key_of, mock_sstable_store};
use crate::hummock::local_version::pinned_version::PinnedVersion;
use crate::hummock::pin_cache::PinCache;
use crate::hummock::pin_cache::test_utils::in_memory_object_store;
use crate::hummock::sstable_store::{SstableStore, SstableStoreConfig};
use crate::hummock::test_utils::{default_builder_opt_for_test, gen_test_sstable_with_table_ids};
use crate::hummock::value::HummockValue;
use crate::hummock::{RecentFilter, SstableStoreRef, TableHolder};
use crate::monitor::global_hummock_state_store_metrics;

pub(super) async fn mock_sstable_store_with_disk_cache(
    recent_filter: Option<Arc<RecentFilter<(HummockSstableObjectId, usize)>>>,
) -> (SstableStoreRef, tempfile::TempDir) {
    let cache_dir = tempfile::tempdir().unwrap();
    let device = FsDeviceBuilder::new(cache_dir.path())
        .with_capacity(16 << 20)
        .build()
        .unwrap();
    let block_cache = HybridCacheBuilder::new()
        .memory(64 << 20)
        .with_shards(2)
        .storage()
        .with_io_engine_config(PsyncIoEngineConfig::new())
        .with_engine_config(
            BlockEngineConfig::new(device)
                .with_block_size(1 << 20)
                .with_buffer_pool_size(4 << 20),
        )
        .build()
        .await
        .unwrap();
    assert!(block_cache.is_hybrid());
    let memory_store = mock_sstable_store().await;
    let sstable_store = Arc::new(SstableStore::new(SstableStoreConfig {
        store: memory_store.store(),
        path: "test".to_owned(),
        prefetch_buffer_capacity: 64 << 20,
        max_prefetch_block_number: 16,
        recent_filter: recent_filter.unwrap_or_else(|| memory_store.recent_filter().clone()),
        state_store_metrics: Arc::new(global_hummock_state_store_metrics(MetricLevel::Disabled)),
        use_new_object_prefix_strategy: true,
        skip_bloom_filter_in_serde: false,
        meta_cache: memory_store.meta_cache().clone(),
        block_cache,
        vector_meta_cache: CacheBuilder::new(64 << 20).build(),
        vector_block_cache: CacheBuilder::new(64 << 20).build(),
    }));
    (sstable_store, cache_dir)
}

pub(super) fn test_refill_config() -> CacheRefillConfig {
    CacheRefillConfig {
        timeout: Duration::from_secs(1),
        pin_cache_max_batch_size: 16,
        data_refill_levels: HashSet::new(),
        meta_refill_concurrency: 1,
        concurrency: 1,
        unit: 1,
        threshold: 0.0,
        skip_recent_filter: true,
        skip_inheritance_filter: true,
    }
}

pub(super) fn deleted_sst(object_id: HummockSstableObjectId) -> SstableInfo {
    SstableInfo::from(SstableInfoInner {
        object_id,
        ..Default::default()
    })
}

pub(super) fn pinned_version_for_test() -> PinnedVersion {
    PinnedVersion::new(
        HummockVersion::from(PbHummockVersion::default()),
        unbounded_channel().0,
    )
}

pub(super) async fn wait_for_pin_downloads(
    refiller: &RefillTest,
    objects: &[HummockSstableObjectId],
) {
    tokio::time::timeout(Duration::from_secs(1), async {
        while !objects.iter().all(|object| {
            refiller
                .cache()
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
    .expect("Pin admission must submit the expected downloads");
}

pub(super) async fn apply_next_events(refiller: &mut RefillTest) -> Vec<VersionUpdateEvent> {
    let events = tokio::time::timeout(Duration::from_secs(5), refiller.versions().next_events())
        .await
        .expect("cache refill must release the next version batch");
    refiller.publish(&events);
    events
}

pub(super) async fn pin_cache_for_test() -> Arc<PinCache> {
    PinCache::new(in_memory_object_store(), u64::MAX, 1, [])
        .await
        .unwrap()
}

pub(super) async fn mock_sstable_store_with_pin_cache() -> (SstableStoreRef, Arc<PinCache>) {
    let cache = pin_cache_for_test().await;
    let store = Arc::new(
        Arc::into_inner(mock_sstable_store().await)
            .unwrap()
            .with_pin_cache(cache.clone()),
    );
    (store, cache)
}

pub(in crate::hummock::event_handler) fn pinned_version_with_ssts(
    table_ids: &[TableId],
    ssts: &[SstableInfo],
) -> PinnedVersion {
    let group = 1.into();
    let version = PbHummockVersion {
        id: 1.into(),
        levels: [(
            group,
            PbLevels {
                group_id: group,
                l0: Some(PbOverlappingLevel {
                    sub_levels: vec![PbLevel {
                        table_infos: ssts.iter().cloned().map(Into::into).collect(),
                        ..Default::default()
                    }],
                    ..Default::default()
                }),
                ..Default::default()
            },
        )]
        .into(),
        state_table_info: table_ids
            .iter()
            .map(|&table| {
                (
                    table,
                    PbStateTableInfo {
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
        unbounded_channel().0,
    )
}

pub(super) async fn gen_test_sst_with_object_id(
    table_id: TableId,
    sstable_store: SstableStoreRef,
    object_id: u64,
) -> (TableHolder, SstableInfo) {
    gen_test_sstable_with_table_ids(
        default_builder_opt_for_test(),
        object_id,
        (0..2).map(|idx| {
            (
                FullKey {
                    user_key: risingwave_hummock_sdk::key::UserKey::for_test(
                        table_id,
                        iterator_test_table_key_of(idx),
                    ),
                    epoch_with_gap: EpochWithGap::new_from_epoch(test_epoch(233)),
                },
                HummockValue::put(vec![idx as u8]),
            )
        }),
        sstable_store,
        vec![table_id.as_raw_id()],
    )
    .await
}

pub(super) fn l0_insert_delta(
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
