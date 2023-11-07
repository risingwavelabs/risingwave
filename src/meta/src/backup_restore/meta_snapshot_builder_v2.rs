// Copyright 2023 RisingWave Labs
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

#![expect(dead_code, reason = "WIP")]

use std::future::Future;

use itertools::Itertools;
use risingwave_backup::error::{BackupError, BackupResult};
use risingwave_backup::meta_snapshot_v2::{MetaSnapshotV2, MetadataV2};
use risingwave_backup::MetaSnapshotId;
use risingwave_hummock_sdk::compaction_group::hummock_version_ext::HummockVersionUpdateExt;
use risingwave_meta_model_v2 as model_v2;
use risingwave_pb::hummock::{HummockVersion, HummockVersionDelta};
use sea_orm::{EntityTrait, QueryOrder, TransactionTrait};

use crate::controller::SqlMetaStore;

const VERSION: u32 = 2;

pub struct MetaSnapshotV2Builder {
    snapshot: MetaSnapshotV2,
    meta_store: SqlMetaStore,
}

impl MetaSnapshotV2Builder {
    pub fn new(meta_store: SqlMetaStore) -> Self {
        Self {
            snapshot: MetaSnapshotV2::default(),
            meta_store,
        }
    }

    pub async fn build<D: Future<Output = HummockVersion>>(
        &mut self,
        id: MetaSnapshotId,
        hummock_version_builder: D,
    ) -> BackupResult<()> {
        self.snapshot.format_version = VERSION;
        self.snapshot.id = id;
        // Get `hummock_version` before `meta_store_snapshot`.
        // We have ensure the required delta logs for replay is available, see
        // `HummockManager::delete_version_deltas`.
        let hummock_version = hummock_version_builder.await;
        let txn = self
            .meta_store
            .conn
            .begin_with_config(
                Some(sea_orm::IsolationLevel::Serializable),
                Some(sea_orm::AccessMode::ReadOnly),
            )
            .await
            .map_err(|e| BackupError::MetaStorage(e.into()))?;
        let version_deltas = model_v2::prelude::HummockVersionDelta::find()
            .order_by_asc(model_v2::hummock_version_delta::Column::Id)
            .all(&txn)
            .await
            .map_err(|e| BackupError::MetaStorage(e.into()))?
            .into_iter()
            .map_into::<HummockVersionDelta>();
        let hummock_version = {
            let mut redo_state = hummock_version;
            let mut max_log_id = None;
            for version_delta in version_deltas {
                if version_delta.prev_id == redo_state.id {
                    redo_state.apply_version_delta(&version_delta);
                }
                max_log_id = Some(version_delta.id);
            }
            if let Some(max_log_id) = max_log_id {
                if max_log_id != redo_state.id {
                    return Err(BackupError::Other(anyhow::anyhow!(format!(
                        "inconsistent hummock version: expected {}, actual {}",
                        max_log_id, redo_state.id
                    ))));
                }
            }
            redo_state
        };
        let version_stats = model_v2::prelude::HummockVersionStats::find_by_id(
            hummock_version.id as model_v2::HummockVersionId,
        )
        .one(&txn)
        .await
        .map_err(|e| BackupError::MetaStorage(e.into()))?
        .unwrap_or_else(|| panic!("version stats for version {} not found", hummock_version.id));
        let compaction_configs = model_v2::prelude::CompactionConfig::find()
            .all(&txn)
            .await
            .map_err(|e| BackupError::MetaStorage(e.into()))?;

        // TODO: other metadata
        let cluster_id = "TODO".to_string();

        txn.commit()
            .await
            .map_err(|e| BackupError::MetaStorage(e.into()))?;
        self.snapshot.metadata = MetadataV2 {
            cluster_id,
            hummock_version,
            version_stats,
            compaction_configs,
        };
        Ok(())
    }

    pub fn finish(self) -> BackupResult<MetaSnapshotV2> {
        // Any sanity check goes here.
        Ok(self.snapshot)
    }
}
