// Copyright 2022 Singularity Data
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::collections::BTreeMap;

use risingwave_common::error::Result;
use risingwave_common::util::epoch::INVALID_EPOCH;
use risingwave_hummock_sdk::{HummockContextId, HummockSSTableId, HummockVersionId};
use risingwave_pb::hummock::{
    HummockPinnedSnapshot, HummockPinnedVersion, HummockStaleSstables, HummockVersion, Level,
    LevelType, SstableIdInfo,
};

use crate::hummock::model::CurrentHummockVersionId;
use crate::model::MetadataModel;
use crate::storage::MetaStore;

#[derive(Default)]
pub struct Versioning {
    pub(crate) current_version_id: CurrentHummockVersionId,
    pub(crate) hummock_versions: BTreeMap<HummockVersionId, HummockVersion>,
    pub(crate) pinned_versions: BTreeMap<HummockContextId, HummockPinnedVersion>,
    pub(crate) pinned_snapshots: BTreeMap<HummockContextId, HummockPinnedSnapshot>,
    pub(crate) stale_sstables: BTreeMap<HummockVersionId, HummockStaleSstables>,
    pub(crate) sstable_id_infos: BTreeMap<HummockSSTableId, SstableIdInfo>,
}

impl Versioning {
    pub async fn init(&mut self, meta_store: &impl MetaStore) -> Result<()> {
        self.current_version_id = CurrentHummockVersionId::get(meta_store)
            .await?
            .unwrap_or_else(CurrentHummockVersionId::new);

        self.hummock_versions = HummockVersion::list(meta_store)
            .await?
            .into_iter()
            .map(|version| (version.id, version))
            .collect();

        // Insert the initial version.
        if self.hummock_versions.is_empty() {
            let init_version = HummockVersion {
                id: self.current_version_id.id(),
                levels: vec![
                    Level {
                        level_idx: 0,
                        level_type: LevelType::Overlapping as i32,
                        table_infos: vec![],
                    },
                    Level {
                        level_idx: 1,
                        level_type: LevelType::Nonoverlapping as i32,
                        table_infos: vec![],
                    },
                ],
                uncommitted_epochs: vec![],
                max_committed_epoch: INVALID_EPOCH,
                safe_epoch: INVALID_EPOCH,
            };
            init_version.insert(meta_store).await?;
            self.hummock_versions.insert(init_version.id, init_version);
        }

        self.pinned_versions = HummockPinnedVersion::list(meta_store)
            .await?
            .into_iter()
            .map(|p| (p.context_id, p))
            .collect();
        self.pinned_snapshots = HummockPinnedSnapshot::list(meta_store)
            .await?
            .into_iter()
            .map(|p| (p.context_id, p))
            .collect();

        self.stale_sstables = HummockStaleSstables::list(meta_store)
            .await?
            .into_iter()
            .map(|s| (s.version_id, s))
            .collect();

        self.sstable_id_infos = SstableIdInfo::list(meta_store)
            .await?
            .into_iter()
            .map(|s| (s.id, s))
            .collect();

        Ok(())
    }

    pub fn current_version_ref(&self) -> &HummockVersion {
        self.hummock_versions
            .get(&self.current_version_id.id())
            .expect("current version should always be available.")
    }

    pub fn current_version(&self) -> HummockVersion {
        self.current_version_ref().clone()
    }
}
