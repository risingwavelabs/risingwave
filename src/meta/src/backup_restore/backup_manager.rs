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

use arc_swap::ArcSwap;
use itertools::Itertools;
use risingwave_backup::error::BackupError;
use risingwave_backup::storage::{BoxedMetaSnapshotStorage, ObjectStoreMetaSnapshotStorage};
use risingwave_backup::{MetaBackupJobId, MetaSnapshotId, MetaSnapshotManifest};
use risingwave_common::bail;
use risingwave_hummock_sdk::HummockSstableObjectId;
use risingwave_object_store::object::object_metrics::ObjectStoreMetrics;
use risingwave_object_store::object::parse_remote_object_store;
use risingwave_pb::backup_service::{BackupJobStatus, MetaBackupManifestId};
use risingwave_pb::meta::subscribe_response::{Info, Operation};
use tokio::task::JoinHandle;

use crate::backup_restore::meta_snapshot_builder::MetaSnapshotBuilder;
use crate::backup_restore::metrics::BackupManagerMetrics;
use crate::hummock::{HummockManagerRef, HummockVersionSafePoint};
use crate::manager::{IdCategory, LocalNotification, MetaSrvEnv};
use crate::rpc::metrics::MetaMetrics;
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
/// (url, dir)
type StoreConfig = (String, String);

/// `BackupManager` manages lifecycle of all existent backups and the running backup job.
pub struct BackupManager<S: MetaStore> {
    env: MetaSrvEnv<S>,
    hummock_manager: HummockManagerRef<S>,
    backup_store: ArcSwap<(BoxedMetaSnapshotStorage, StoreConfig)>,
    /// Tracks the running backup job. Concurrent jobs is not supported.
    running_backup_job: tokio::sync::Mutex<Option<BackupJobHandle>>,
    metrics: BackupManagerMetrics,
    meta_metrics: Arc<MetaMetrics>,
}

impl<S: MetaStore> BackupManager<S> {
    pub async fn new(
        env: MetaSrvEnv<S>,
        hummock_manager: HummockManagerRef<S>,
        metrics: Arc<MetaMetrics>,
        store_url: &str,
        store_dir: &str,
    ) -> MetaResult<Arc<Self>> {
        let store_config = (store_url.to_string(), store_dir.to_string());
        let store =
            create_snapshot_store(&store_config, metrics.object_store_metric.clone()).await?;
        tracing::info!(
            "backup manager initialized: url={}, dir={}",
            store_config.0,
            store_config.1
        );
        let instance = Arc::new(Self::with_store(
            env.clone(),
            hummock_manager,
            metrics,
            (store, store_config),
        ));
        let (local_notification_tx, mut local_notification_rx) =
            tokio::sync::mpsc::unbounded_channel();
        env.notification_manager()
            .insert_local_sender(local_notification_tx)
            .await;
        let this = instance.clone();
        tokio::spawn(async move {
            loop {
                match local_notification_rx.recv().await {
                    Some(notification) => {
                        if let LocalNotification::SystemParamsChange(p) = notification {
                            let new_config = (
                                p.backup_storage_url().to_string(),
                                p.backup_storage_directory().to_string(),
                            );
                            this.handle_new_config(new_config).await;
                        }
                    }
                    None => {
                        return;
                    }
                }
            }
        });
        Ok(instance)
    }

    async fn handle_new_config(&self, new_config: StoreConfig) {
        if self.backup_store.load().1 == new_config {
            return;
        }
        if let Err(e) = self.set_store(new_config.clone()).await {
            // Retry is driven by periodic system params notification.
            tracing::warn!(
                "failed to apply new backup config: url={}, dir={}, {:#?}",
                new_config.0,
                new_config.1,
                e
            );
        }
    }

    fn with_store(
        env: MetaSrvEnv<S>,
        hummock_manager: HummockManagerRef<S>,
        meta_metrics: Arc<MetaMetrics>,
        backup_store: (BoxedMetaSnapshotStorage, StoreConfig),
    ) -> Self {
        Self {
            env,
            hummock_manager,
            backup_store: ArcSwap::from_pointee(backup_store),
            running_backup_job: tokio::sync::Mutex::new(None),
            metrics: BackupManagerMetrics::new(meta_metrics.registry.clone()),
            meta_metrics,
        }
    }

    pub async fn set_store(&self, config: StoreConfig) -> MetaResult<()> {
        let new_store =
            create_snapshot_store(&config, self.meta_metrics.object_store_metric.clone()).await?;
        tracing::info!(
            "new backup config is applied: url={}, dir={}",
            config.0,
            config.1
        );
        self.backup_store.store(Arc::new((new_store, config)));
        Ok(())
    }

    #[cfg(test)]
    pub fn for_test(env: MetaSrvEnv<S>, hummock_manager: HummockManagerRef<S>) -> Self {
        Self::with_store(
            env,
            hummock_manager,
            Arc::new(MetaMetrics::new()),
            (
                Box::<risingwave_backup::storage::DummyMetaSnapshotStorage>::default(),
                StoreConfig::default(),
            ),
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
        // The reasons to limit number of meta snapshot are:
        // 1. limit size of `MetaSnapshotManifest`, which is kept in memory by
        // `ObjectStoreMetaSnapshotStorage`.
        // 2. limit number of pinned SSTs returned by
        // `list_pinned_ssts`, which subsequently is used by GC.
        const MAX_META_SNAPSHOT_NUM: usize = 100;
        let current_number = self
            .backup_store
            .load()
            .0
            .manifest()
            .snapshot_metadata
            .len();
        if current_number > MAX_META_SNAPSHOT_NUM {
            bail!(format!(
                "too many existent meta snapshots, expect at most {}",
                MAX_META_SNAPSHOT_NUM
            ))
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
            .load()
            .0
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
                            id: self.backup_store.load().0.manifest().manifest_id,
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
        self.backup_store.load().0.delete(ids).await?;
        self.env
            .notification_manager()
            .notify_hummock_without_version(
                Operation::Update,
                Info::MetaBackupManifestId(MetaBackupManifestId {
                    id: self.backup_store.load().0.manifest().manifest_id,
                }),
            );
        Ok(())
    }

    /// List all `SSTables` required by backups.
    pub fn list_pinned_ssts(&self) -> Vec<HummockSstableObjectId> {
        self.backup_store
            .load()
            .0
            .manifest()
            .snapshot_metadata
            .iter()
            .flat_map(|s| s.ssts.clone())
            .dedup()
            .collect_vec()
    }

    pub fn manifest(&self) -> Arc<MetaSnapshotManifest> {
        self.backup_store.load().0.manifest()
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
            let hummock_manager = backup_manager_clone.hummock_manager.clone();
            snapshot_builder
                .build(job_id, async move {
                    hummock_manager.get_current_version().await
                })
                .await?;
            let snapshot = snapshot_builder.finish()?;
            backup_manager_clone
                .backup_store
                .load()
                .0
                .create(&snapshot)
                .await?;
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

async fn create_snapshot_store(
    config: &StoreConfig,
    metric: Arc<ObjectStoreMetrics>,
) -> MetaResult<BoxedMetaSnapshotStorage> {
    let object_store = Arc::new(parse_remote_object_store(&config.0, metric, "Meta Backup").await);
    let store = ObjectStoreMetaSnapshotStorage::new(&config.1, object_store).await?;
    Ok(Box::new(store))
}
