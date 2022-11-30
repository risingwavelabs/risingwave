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

use std::sync::Arc;

use itertools::Itertools;
use risingwave_common::bail;
use risingwave_hummock_sdk::HummockSstableId;
use tokio::task::JoinHandle;

use crate::backup_restore::db_snapshot::DbSnapshotBuilder;
use crate::backup_restore::error::BackupError;
use crate::backup_restore::storage::BackupStorageRef;
use crate::backup_restore::DbSnapshotId;
use crate::hummock::{HummockManagerRef, HummockVersionSafePoint};
use crate::manager::{IdCategory, MetaSrvEnv};
use crate::storage::MetaStore;
use crate::MetaResult;

pub enum BackupJobResult {
    Finished,
    Failed(BackupError),
}

/// `BackupJobHandle` tracks running job.
struct BackupJobHandle {
    job_id: u64,
    hummock_version_safe_point: HummockVersionSafePoint,
}

impl BackupJobHandle {
    pub fn new(job_id: u64, hummock_version_safe_point: HummockVersionSafePoint) -> Self {
        Self {
            job_id,
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
    /// Tracks the running backup job. Concurrent jobs is not supported.
    running_backup_job: tokio::sync::Mutex<Option<BackupJobHandle>>,
}

impl<S: MetaStore> BackupManager<S> {
    pub fn new(
        env: MetaSrvEnv<S>,
        hummock_manager: HummockManagerRef<S>,
        backup_store: BackupStorageRef,
    ) -> Self {
        Self {
            env,
            hummock_manager,
            backup_store,
            running_backup_job: tokio::sync::Mutex::new(None),
        }
    }

    #[cfg(test)]
    pub fn for_test(env: MetaSrvEnv<S>, hummock_manager: HummockManagerRef<S>) -> Self {
        Self {
            env,
            hummock_manager,
            backup_store: Box::new(crate::backup_restore::DummyBackupStorage {}),
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
        // TODO #6482: ideally `BackupWorker` and its r/w IO can be made external to meta node.
        // The pros of keeping `BackupWorker` in meta node are:
        // - It makes meta node the only writer of backup storage, which eases implementation.
        // - It's likely meta store is deployed in the same node with meta node.
        // - IO volume of metadata snapshot is not expected to be large.
        // - Backup job is not expected to be frequent.
        BackupWorker::new(self.clone()).start(job_id);
        let job_handle = BackupJobHandle::new(job_id, hummock_version_safe_point);
        *guard = Some(job_handle);
        Ok(job_id)
    }

    async fn finish_backup_job(&self, job_id: u64, status: BackupJobResult) {
        // _job_handle holds `hummock_version_safe_point` until the db snapshot is added to meta.
        // It ensures db snapshot's SSTs won't be deleted in between.
        let _job_handle = self
            .take_job_handle_by_job_id(job_id)
            .await
            .expect("job id should match");
        match status {
            BackupJobResult::Finished => {
                tracing::info!("succeeded backup job {}", job_id);
            }
            BackupJobResult::Failed(e) => {
                tracing::warn!("failed backup job {}: {}", job_id, e);
            }
        }
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
        Ok(())
    }

    /// List all `SSTables` required by backups.
    pub async fn list_pinned_ssts(&self) -> MetaResult<Vec<HummockSstableId>> {
        let r = self
            .backup_store
            .list()
            .await?
            .into_iter()
            .flat_map(|s| s.ssts)
            .collect_vec();
        Ok(r)
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

    fn start(self, job_id: u64) -> JoinHandle<()> {
        let backup_manager_clone = self.backup_manager.clone();
        let job = async move {
            let mut db_snapshot_builder =
                DbSnapshotBuilder::new(backup_manager_clone.env.meta_store_ref());
            // Reuse job id as db snapshot id.
            db_snapshot_builder.build(job_id).await?;
            let db_snapshot = db_snapshot_builder.finish()?;
            backup_manager_clone
                .backup_store
                .create(&db_snapshot)
                .await?;
            backup_manager_clone
                .finish_backup_job(job_id, BackupJobResult::Finished)
                .await;
            Ok::<(), BackupError>(())
        };
        tokio::spawn(async move {
            if let Err(e) = job.await {
                self.backup_manager
                    .finish_backup_job(job_id, BackupJobResult::Failed(e))
                    .await;
            }
        })
    }
}
