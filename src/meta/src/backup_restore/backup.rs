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

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use anyhow::anyhow;
use itertools::Itertools;
use risingwave_common::bail;
use risingwave_hummock_sdk::compaction_group::hummock_version_ext::HummockVersionExt;
use risingwave_hummock_sdk::HummockSstableId;
use tokio::task::JoinHandle;

use crate::backup_restore::db_snapshot::{DbSnapshot, DbSnapshotBuilder};
use crate::backup_restore::error::BackupError;
use crate::backup_restore::storage::BackupStorageRef;
use crate::backup_restore::DbSnapshotId;
use crate::hummock::{HummockManagerRef, HummockVersionSafePoint};
use crate::manager::{IdCategory, MetaSrvEnv};
use crate::storage::MetaStore;
use crate::MetaResult;

#[derive(Clone)]
pub enum BackupJobResult {
    Cancelled,
    Finished(DbSnapshot),
    Failed,
}

/// `BackupJobHandle` tracks running job.
struct BackupJobHandle {
    job_id: u64,
    join_handle: JoinHandle<BackupJobResult>,
    hummock_version_safe_point: HummockVersionSafePoint,
}

impl BackupJobHandle {
    pub fn new(
        job_id: u64,
        join_handle: JoinHandle<BackupJobResult>,
        hummock_version_safe_point: HummockVersionSafePoint,
    ) -> Self {
        Self {
            job_id,
            join_handle,
            hummock_version_safe_point,
        }
    }
}

pub type BackupManagerRef<S> = Arc<BackupManager<S>>;

/// `BackupManager` manages lifecycle of all existent backups and the running backup job.
pub struct BackupManager<S: MetaStore> {
    env: MetaSrvEnv<S>,
    hummock_manager: HummockManagerRef<S>,
    backup_store: BackupStorageRef,
    /// Existent database snapshot.
    db_snapshots: parking_lot::RwLock<HashMap<DbSnapshotId, DbSnapshot>>,
    /// Tracks the running backup job. Concurrent jobs is not supported.
    running_backup_job: tokio::sync::Mutex<Option<BackupJobHandle>>,
}

impl<S: MetaStore> BackupManager<S> {
    pub async fn new(
        env: MetaSrvEnv<S>,
        hummock_manager: HummockManagerRef<S>,
        backup_store: BackupStorageRef,
    ) -> MetaResult<Self> {
        // TODO #6482 refine me: meta node actually only need SST manifest in memory.
        let db_snapshots = backup_store
            .list()
            .await?
            .into_iter()
            .map(|s| (s.id, s))
            .collect();
        Ok(Self {
            env,
            hummock_manager,
            backup_store,
            db_snapshots: parking_lot::RwLock::new(db_snapshots),
            running_backup_job: tokio::sync::Mutex::new(None),
        })
    }

    #[cfg(test)]
    pub fn for_test(env: MetaSrvEnv<S>, hummock_manager: HummockManagerRef<S>) -> Self {
        Self {
            env,
            hummock_manager,
            backup_store: Box::new(crate::backup_restore::DummyBackupStorage {}),
            db_snapshots: Default::default(),
            running_backup_job: Default::default(),
        }
    }

    /// Starts a backup job in background. It's non-blocking.
    /// Returns job id.
    pub async fn start_backup_job(self: &Arc<Self>) -> MetaResult<u64> {
        let mut guard = self.running_backup_job.lock().await;
        if let Some(job) = (*guard).as_ref() {
            bail!(format!(
                "concurrent backup job is not supported: existent job {}",
                job.job_id
            ));
        }
        let job_id = self
            .env
            .id_gen_manager()
            .generate::<{ IdCategory::Backup }>()
            .await?;
        let hummock_version_safe_point = self.hummock_manager.register_safe_point().await;
        let worker = BackupWorker::new(self.clone());
        let worker_join_handle = worker.start(job_id);
        let job_handle =
            BackupJobHandle::new(job_id, worker_join_handle, hummock_version_safe_point);
        *guard = Some(job_handle);
        Ok(job_id)
    }

    async fn finish_backup_job(&self, job_id: u64, status: BackupJobResult) -> MetaResult<()> {
        // _job_handle holds `hummock_version_safe_point` until the db snapshot is added to meta.
        // It ensures db snapshot's SSTs won't be deleted in between.
        let _job_handle = self
            .take_job_handle_by_job_id(job_id)
            .await
            .ok_or_else(|| anyhow!("{} is not a running backup job", job_id))?;
        match status {
            BackupJobResult::Cancelled => {}
            BackupJobResult::Finished(db_snapshot) => {
                self.db_snapshots
                    .write()
                    .insert(db_snapshot.id, db_snapshot);
            }
            BackupJobResult::Failed => {}
        }
        Ok(())
    }

    async fn take_job_handle_by_job_id(&self, job_id: u64) -> Option<BackupJobHandle> {
        let mut guard = self.running_backup_job.lock().await;
        match (*guard).as_ref() {
            None => {
                return None;
            }
            Some(job_handle) => {
                if job_handle.job_id != job_id {
                    return None;
                }
            }
        }
        guard.take()
    }

    /// Deletes existent backups from backup storage, and removes their references in
    /// `BackupManager`.
    pub async fn delete_backups(&self, ids: &[DbSnapshotId]) -> MetaResult<()> {
        self.backup_store.delete(ids).await?;
        let ids: HashSet<DbSnapshotId> = HashSet::from_iter(ids.iter().cloned());
        self.db_snapshots.write().retain(|id, _| !ids.contains(id));
        Ok(())
    }

    /// List all existent backups.
    pub fn list_backups(&self) -> Vec<DbSnapshot> {
        self.db_snapshots.read().values().cloned().collect_vec()
    }

    /// List all `SSTables` required by backups.
    pub fn list_pinned_ssts(&self) -> Vec<HummockSstableId> {
        self.db_snapshots
            .read()
            .values()
            .flat_map(|s| s.metadata.hummock_version.get_sst_ids())
            .collect_vec()
    }
}

/// `BackupWorker` creates a database snapshot.
struct BackupWorker<S: MetaStore> {
    backup_manager: BackupManagerRef<S>,
}

impl<S: MetaStore> BackupWorker<S> {
    fn new(backup_manager: BackupManagerRef<S>) -> Self {
        Self { backup_manager }
    }

    fn start(self, job_id: u64) -> JoinHandle<BackupJobResult> {
        let job = async move {
            let mut db_snapshot_builder =
                DbSnapshotBuilder::new(self.backup_manager.env.meta_store_ref().clone());
            // Reuse job id as db snapshot id.
            db_snapshot_builder.build(job_id).await?;
            let db_snapshot = db_snapshot_builder.finish()?;
            let job_status = BackupJobResult::Finished(db_snapshot);
            self.backup_manager
                .finish_backup_job(job_id, job_status.clone())
                .await?;
            Ok::<BackupJobResult, BackupError>(job_status)
        };
        tokio::spawn(async move {
            job.await.unwrap_or_else(|e| {
                tracing::warn!("failed backup job {}: {}", job_id, e);
                BackupJobResult::Failed
            })
        })
    }
}
