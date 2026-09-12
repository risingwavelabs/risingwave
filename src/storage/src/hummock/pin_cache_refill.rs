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

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use futures::future::join_all;
use risingwave_common::config::streaming::CacheRefillPolicy;
use risingwave_hummock_sdk::HummockSstableObjectId;
use risingwave_hummock_sdk::compaction_group::hummock_version_ext::SstDeltaInfo;
use risingwave_hummock_sdk::sstable_info::SstableInfo;
use risingwave_pb::id::TableId;
use thiserror_ext::AsReport;
use tokio::sync::Semaphore;

use crate::hummock::SstableStoreRef;
use crate::hummock::local_version::pinned_version::PinnedVersion;

#[derive(Clone, Copy)]
pub(crate) enum PinCacheMembershipUpdate {
    Delta,
    Rebuild,
}

/// Maintains logical pin-cache membership separately from Foyer block-refill policy.
pub(crate) struct PinCacheRefillController {
    sstable_store: SstableStoreRef,
    pinned_table_ids: Option<HashSet<TableId>>,
    version: PinnedVersion,
}

impl PinCacheRefillController {
    pub(crate) fn new(sstable_store: SstableStoreRef, version: PinnedVersion) -> Self {
        Self {
            sstable_store,
            pinned_table_ids: None,
            version,
        }
    }

    pub(crate) fn replace_policies(&mut self, policies: &HashMap<TableId, CacheRefillPolicy>) {
        let pinned_table_ids = policies
            .iter()
            .filter_map(|(&table_id, policy)| policy.is_pinned().then_some(table_id))
            .collect();
        if self.pinned_table_ids.as_ref() == Some(&pinned_table_ids) {
            return;
        }
        self.pinned_table_ids = Some(pinned_table_ids);
        self.rebuild_desired_objects();
    }

    pub(crate) fn apply_version_update(
        &mut self,
        deltas: &[SstDeltaInfo],
        new_version: PinnedVersion,
        membership_update: PinCacheMembershipUpdate,
    ) -> HashSet<HummockSstableObjectId> {
        self.version = new_version;
        let Some(pin_cache) = self.sstable_store.pin_cache() else {
            return HashSet::new();
        };
        match membership_update {
            PinCacheMembershipUpdate::Delta => self.apply_desired_object_delta(deltas),
            PinCacheMembershipUpdate::Rebuild => self.rebuild_desired_objects(),
        }

        deltas
            .iter()
            .flat_map(|delta| &delta.insert_sst_infos)
            .filter(|sst| pin_cache.is_desired(sst.object_id))
            .map(|sst| sst.object_id)
            .collect()
    }

    fn rebuild_desired_objects(&self) {
        let Some(pin_cache) = self.sstable_store.pin_cache() else {
            return;
        };
        let Some(pinned_table_ids) = &self.pinned_table_ids else {
            return;
        };

        let compaction_group_ids = pinned_table_ids
            .iter()
            .filter_map(|table_id| {
                self.version
                    .state_table_info
                    .info()
                    .get(table_id)
                    .map(|info| info.compaction_group_id)
            })
            .collect::<HashSet<_>>();
        let objects = compaction_group_ids
            .into_iter()
            .flat_map(|compaction_group_id| {
                let levels = self
                    .version
                    .get_compaction_group_levels(compaction_group_id);
                levels.l0.sub_levels.iter().chain(levels.levels.iter())
            })
            .flat_map(|level| &level.table_infos)
            .filter(|sst| Self::is_pinned(sst, pinned_table_ids))
            .map(|sst| (sst.object_id, sst.file_size));
        pin_cache.replace_desired_objects(objects);
    }

    fn apply_desired_object_delta(&self, deltas: &[SstDeltaInfo]) {
        let Some(pin_cache) = self.sstable_store.pin_cache() else {
            return;
        };
        let Some(pinned_table_ids) = &self.pinned_table_ids else {
            return;
        };

        let mut removed = HashSet::new();
        let mut inserted = HashMap::new();
        for delta in deltas {
            for sst in delta
                .delete_sst_infos
                .iter()
                .filter(|sst| Self::is_pinned(sst, pinned_table_ids))
            {
                inserted.remove(&sst.object_id);
                removed.insert(sst.object_id);
            }
            for sst in delta
                .insert_sst_infos
                .iter()
                .filter(|sst| Self::is_pinned(sst, pinned_table_ids))
            {
                removed.remove(&sst.object_id);
                inserted.insert(sst.object_id, sst.file_size);
            }
        }
        pin_cache.apply_desired_object_delta(removed, inserted);
    }

    fn is_pinned(sst: &SstableInfo, pinned_table_ids: &HashSet<TableId>) -> bool {
        sst.table_ids
            .iter()
            .any(|table_id| pinned_table_ids.contains(table_id))
    }
}

pub(crate) async fn refill_pin_cache_objects(
    sstable_store: SstableStoreRef,
    concurrency: Arc<Semaphore>,
    object_ids: HashSet<HummockSstableObjectId>,
) {
    let futures = object_ids.into_iter().map(|object_id| {
        let sstable_store = sstable_store.clone();
        let concurrency = concurrency.clone();
        async move {
            let _permit = concurrency.acquire().await.unwrap();
            if let Err(error) = sstable_store.pin_sst(object_id).await {
                tracing::warn!(
                    object_id = object_id.as_raw_id(),
                    error = %error.as_report(),
                    "pin cache refill failed"
                );
            }
        }
    });
    join_all(futures).await;
}
