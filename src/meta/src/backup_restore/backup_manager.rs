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

use std::sync::Arc;
use std::time::Instant;

use itertools::Itertools;
use prometheus::Registry;
use risingwave_backup::error::BackupError;
use risingwave_backup::storage::MetaSnapshotStorageRef;
use risingwave_backup::{MetaBackupJobId, MetaSnapshotId, MetaSnapshotManifest};
use risingwave_common::bail;
use risingwave_hummock_sdk::HummockSstableId;
use risingwave_pb::backup_service::{BackupJobStatus, MetaBackupManifestId};
use risingwave_pb::meta::subscribe_response::{Info, Operation};
use tokio::task::JoinHandle;

use crate::backup_restore::meta_snapshot_builder::MetaSnapshotBuilder;
use crate::backup_restore::metrics::BackupManagerMetrics;
use crate::hummock::{HummockManagerRef, HummockVersionSafePoint};
use crate::manager::{IdCategory, MetaSrvEnv};
use crate::storage::MetaStore;
use crate::MetaResult;

pub enum BackupJobResult {
    Succeeded,
    Failed(BackupError),
}

/// `BackupJobHandle` tracks running job.
struct BackupJobHandle {
    job_id: u64,
    #[expect(dead_code)]
    hummock_version_safe_point: HummockVersionSafePoint,
    start_time: Instant,
}

impl BackupJobHandle {
    pub fn new(job_id: u64, hummock_version_safe_point: HummockVersionSafePoint) -> Self {
        Self {
            job_id,
            hummock_version_safe_point,
            start_time: Instant::now(),
        }
    }
}

pub type BackupManagerRef<S> = Arc<BackupManager<S>>;

/// `BackupManager` manages lifecycle of all existent backups and the running backup job.
pub struct BackupManager<S: MetaStore> {
    env: MetaSrvEnv<S>,
    hummock_manager: HummockManagerRef<S>,
    backup_store: MetaSnapshotStorageRef,
    /// Tracks the running backup job. Concurrent jobs is not supported.
    running_backup_job: tokio::sync::Mutex<Option<BackupJobHandle>>,
    metrics: BackupManagerMetrics,
}

impl<S: MetaStore> BackupManager<S> {
    pub fn new(
        env: MetaSrvEnv<S>,
        hummock_manager: HummockManagerRef<S>,
        backup_store: MetaSnapshotStorageRef,
        registry: Registry,
    ) -> Self {
        Self {
            env,
            hummock_manager,
            backup_store,
            running_backup_job: tokio::sync::Mutex::new(None),
            metrics: BackupManagerMetrics::new(registry),
        }
    }

    #[cfg(test)]
    pub fn for_test(env: MetaSrvEnv<S>, hummock_manager: HummockManagerRef<S>) -> Self {
        Self::new(
            env,
            hummock_manager,
            Arc::new(risingwave_backup::storage::DummyMetaSnapshotStorage::default()),
            Registry::new(),
        )
    }

    /// Starts a backup job in background. It's non-blocking.
    /// Returns job id.
    pub async fn start_backup_job(self: &Arc<Self>) -> MetaResult<MetaBackupJobId> {
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
        // Ideally `BackupWorker` and its r/w IO can be made external to meta node.
        // The justification of keeping `BackupWorker` in meta node are:
        // - It makes meta node the only writer of backup storage, which eases implementation.
        // - It's likely meta store is deployed in the same node with meta node.
        // - IO volume of metadata snapshot is not expected to be large.
        // - Backup job is not expected to be frequent.
        BackupWorker::new(self.clone()).start(job_id);
        let job_handle = BackupJobHandle::new(job_id, hummock_version_safe_point);
        *guard = Some(job_handle);
        self.metrics.job_count.inc();
        Ok(job_id)
    }

    pub async fn get_backup_job_status(
        &self,
        job_id: MetaBackupJobId,
    ) -> MetaResult<BackupJobStatus> {
        if let Some(running_job) = self.running_backup_job.lock().await.as_ref() {
            if running_job.job_id == job_id {
                return Ok(BackupJobStatus::Running);
            }
        }
        if self
            .backup_store
            .manifest()
            .snapshot_metadata
            .iter()
            .any(|m| m.id == job_id)
        {
            return Ok(BackupJobStatus::Succeeded);
        }
        Ok(BackupJobStatus::NotFound)
    }

    async fn finish_backup_job(&self, job_id: MetaBackupJobId, job_result: BackupJobResult) {
        // `job_handle` holds `hummock_version_safe_point` until the job is completed.
        let job_handle = self
            .take_job_handle_by_job_id(job_id)
            .await
            .expect("job id should match");
        let job_latency = job_handle.start_time.elapsed().as_secs_f64();
        match job_result {
            BackupJobResult::Succeeded => {
                self.metrics.job_latency_success.observe(job_latency);
                tracing::info!("succeeded backup job {}", job_id);
                self.env
                    .notification_manager()
                    .notify_hummock_without_version(
                        Operation::Update,
                        Info::MetaBackupManifestId(MetaBackupManifestId {
                            id: self.backup_store.manifest().manifest_id,
                        }),
                    );
            }
            BackupJobResult::Failed(e) => {
                self.metrics.job_latency_failure.observe(job_latency);
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

    /// Deletes existent backups from backup storage.
    pub async fn delete_backups(&self, ids: &[MetaSnapshotId]) -> MetaResult<()> {
        self.backup_store.delete(ids).await?;
        self.env
            .notification_manager()
            .notify_hummock_without_version(
                Operation::Update,
                Info::MetaBackupManifestId(MetaBackupManifestId {
                    id: self.backup_store.manifest().manifest_id,
                }),
            );
        Ok(())
    }

    /// List all `SSTables` required by backups.
    pub fn list_pinned_ssts(&self) -> Vec<HummockSstableId> {
        self.backup_store
            .manifest()
            .snapshot_metadata
            .iter()
            .flat_map(|s| s.ssts.clone())
            .dedup()
            .collect_vec()
    }

    pub fn manifest(&self) -> Arc<MetaSnapshotManifest> {
        self.backup_store.manifest()
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
            let mut snapshot_builder =
                MetaSnapshotBuilder::new(backup_manager_clone.env.meta_store_ref());
            // Reuse job id as snapshot id.
            snapshot_builder.build(job_id).await?;
            let snapshot = snapshot_builder.finish()?;
            backup_manager_clone.backup_store.create(&snapshot).await?;
            Ok(BackupJobResult::Succeeded)
        };
        tokio::spawn(async move {
            let job_result = job.await.unwrap_or_else(BackupJobResult::Failed);
            self.backup_manager
                .finish_backup_job(job_id, job_result)
                .await;
        })
    }
}
