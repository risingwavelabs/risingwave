// Copyright 2023 Singularity Data
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
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use futures::future::Shared;
use futures::FutureExt;
use risingwave_backup::error::BackupError;
use risingwave_backup::meta_snapshot::MetaSnapshot;
use risingwave_backup::storage::{
    DummyMetaSnapshotStorage, MetaSnapshotStorageRef, ObjectStoreMetaSnapshotStorage,
};
use risingwave_backup::MetaSnapshotId;
use risingwave_common::config::RwConfig;
use risingwave_object_store::object::object_metrics::ObjectStoreMetrics;
use risingwave_object_store::object::parse_remote_object_store;

use crate::error::{StorageError, StorageResult};
use crate::hummock::local_version::pinned_version::{PinVersionAction, PinnedVersion};
use crate::hummock::HummockError;

pub type BackupReaderRef = Arc<BackupReader>;

type VersionHolder = (
    PinnedVersion,
    tokio::sync::mpsc::UnboundedReceiver<PinVersionAction>,
);

pub async fn parse_meta_snapshot_storage(
    config: &RwConfig,
) -> StorageResult<MetaSnapshotStorageRef> {
    let backup_object_store = Arc::new(
        parse_remote_object_store(
            &config.backup.storage_url,
            Arc::new(ObjectStoreMetrics::unused()),
            true,
            "Meta Backup",
        )
        .await,
    );
    let store = Arc::new(
        ObjectStoreMetaSnapshotStorage::new(&config.backup.storage_directory, backup_object_store)
            .await?,
    );
    Ok(store)
}

type InflightRequest = Shared<Pin<Box<dyn Future<Output = Result<PinnedVersion, String>> + Send>>>;
/// `BackupReader` helps to access historical hummock versions,
/// which are persisted in meta snapshots (aka backups).
pub struct BackupReader {
    versions: parking_lot::RwLock<HashMap<MetaSnapshotId, VersionHolder>>,
    inflight_request: parking_lot::Mutex<HashMap<MetaSnapshotId, InflightRequest>>,
    store: MetaSnapshotStorageRef,
    refresh_tx: tokio::sync::mpsc::UnboundedSender<u64>,
}

impl BackupReader {
    pub fn new(store: MetaSnapshotStorageRef) -> BackupReaderRef {
        let (refresh_tx, refresh_rx) = tokio::sync::mpsc::unbounded_channel();
        let instance = Arc::new(Self {
            store,
            versions: Default::default(),
            inflight_request: Default::default(),
            refresh_tx,
        });
        tokio::spawn(Self::start_manifest_refresher(instance.clone(), refresh_rx));
        instance
    }

    pub fn unused() -> BackupReaderRef {
        Self::new(Arc::new(DummyMetaSnapshotStorage::default()))
    }

    async fn start_manifest_refresher(
        backup_reader: BackupReaderRef,
        mut refresh_rx: tokio::sync::mpsc::UnboundedReceiver<u64>,
    ) {
        loop {
            let expect_manifest_id = refresh_rx.recv().await;
            if expect_manifest_id.is_none() {
                break;
            }
            let expect_manifest_id = expect_manifest_id.unwrap();
            let previous_id = backup_reader.store.manifest().manifest_id;
            if expect_manifest_id <= previous_id {
                continue;
            }
            if let Err(e) = backup_reader.store.refresh_manifest().await {
                // reschedule refresh request
                tracing::warn!("failed to refresh backup manifest, will retry. {}", e);
                let backup_reader_clone = backup_reader.clone();
                tokio::spawn(async move {
                    tokio::time::sleep(Duration::from_secs(60)).await;
                    backup_reader_clone.try_refresh_manifest(expect_manifest_id);
                });
                continue;
            }
            // purge stale version cache
            let manifest: HashSet<MetaSnapshotId> = backup_reader
                .store
                .manifest()
                .snapshot_metadata
                .iter()
                .map(|s| s.id)
                .collect();
            backup_reader
                .versions
                .write()
                .retain(|k, _v| manifest.contains(k));
        }
    }

    pub fn try_refresh_manifest(self: &BackupReaderRef, min_manifest_id: u64) {
        let _ = self
            .refresh_tx
            .send(min_manifest_id)
            .inspect_err(|e| tracing::warn!("failed to send refresh_manifest request {}", e));
    }

    /// Tries to get a hummock version eligible for querying `epoch`.
    /// SSTs of the returned version are expected to be guarded by corresponding backup.
    /// Otherwise, reading the version may encounter object store error, due to SST absence.
    pub async fn try_get_hummock_version(
        self: &BackupReaderRef,
        epoch: u64,
    ) -> StorageResult<Option<PinnedVersion>> {
        // 1. check manifest to locate snapshot, if any.
        let snapshot_id = self
            .store
            .manifest()
            .snapshot_metadata
            .iter()
            .find(|v| epoch >= v.safe_epoch && epoch <= v.max_committed_epoch)
            .map(|s| s.id);
        let snapshot_id = match snapshot_id {
            None => {
                return Ok(None);
            }
            Some(s) => s,
        };
        // 2. load hummock version of chosen snapshot.
        let future = {
            let mut req_guard = self.inflight_request.lock();
            if let Some((v, _)) = self.versions.read().get(&snapshot_id) {
                return Ok(Some(v.clone()));
            }
            if let Some(f) = req_guard.get(&snapshot_id) {
                f.clone()
            } else {
                let this = self.clone();
                let f = async move {
                    let snapshot = this.store.get(snapshot_id).await.map_err(|e| {
                        format!("failed to get meta snapshot {}. {}", snapshot_id, e)
                    })?;
                    let version_holder = build_version_holder(snapshot);
                    let version_clone = version_holder.0.clone();
                    this.versions.write().insert(snapshot_id, version_holder);
                    Ok(version_clone)
                }
                .boxed()
                .shared();
                req_guard.insert(snapshot_id, f.clone());
                f
            }
        };
        let result = future
            .await
            .map(Some)
            .map_err(|e| HummockError::read_backup_error(e).into());
        self.inflight_request.lock().remove(&snapshot_id);
        result
    }
}

fn build_version_holder(s: MetaSnapshot) -> VersionHolder {
    let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
    (PinnedVersion::new(s.metadata.hummock_version, tx), rx)
}

impl From<BackupError> for StorageError {
    fn from(e: BackupError) -> Self {
        HummockError::other(e).into()
    }
}
