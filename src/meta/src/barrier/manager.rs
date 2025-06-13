// Copyright 2025 RisingWave Labs
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

use std::collections::hash_map::Entry;
use std::sync::Arc;

use anyhow::Context;
use arc_swap::ArcSwap;
use risingwave_common::bail;
use risingwave_hummock_sdk::HummockVersionId;
use risingwave_meta_model::DatabaseId;
use risingwave_pb::ddl_service::DdlProgress;
use risingwave_pb::meta::PbRecoveryStatus;
use tokio::sync::mpsc::unbounded_channel;
use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinHandle;
use tracing::warn;

use crate::MetaResult;
use crate::barrier::worker::GlobalBarrierWorker;
use crate::barrier::{BarrierManagerRequest, BarrierManagerStatus, RecoveryReason, schedule};
use crate::hummock::HummockManagerRef;
use crate::manager::sink_coordination::SinkCoordinatorManager;
use crate::manager::{MetaSrvEnv, MetadataManager};
use crate::stream::{ScaleControllerRef, SourceManagerRef};

pub struct GlobalBarrierManager {
    status: Arc<ArcSwap<BarrierManagerStatus>>,
    hummock_manager: HummockManagerRef,
    request_tx: mpsc::UnboundedSender<BarrierManagerRequest>,
    metadata_manager: MetadataManager,
}

pub type BarrierManagerRef = Arc<GlobalBarrierManager>;

impl GlobalBarrierManager {
    /// Serving `SHOW JOBS / SELECT * FROM rw_ddl_progress`
    pub async fn get_ddl_progress(&self) -> MetaResult<Vec<DdlProgress>> {
        let mut ddl_progress = {
            let (tx, rx) = oneshot::channel();
            self.request_tx
                .send(BarrierManagerRequest::GetDdlProgress(tx))
                .context("failed to send get ddl progress request")?;
            rx.await.context("failed to receive get ddl progress")?
        };
        // If not in tracker, means the first barrier not collected yet.
        // In that case just return progress 0.
        let mviews = self
            .metadata_manager
            .catalog_controller
            .list_background_creating_mviews(true)
            .await?;
        for mview in mviews {
            if let Entry::Vacant(e) = ddl_progress.entry(mview.table_id as _) {
                warn!(
                    job_id = mview.table_id,
                    "background job has no ddl progress"
                );
                e.insert(DdlProgress {
                    id: mview.table_id as u64,
                    statement: mview.definition,
                    progress: "0.0%".into(),
                });
            }
        }

        Ok(ddl_progress.into_values().collect())
    }

    pub async fn adhoc_recovery(&self) -> MetaResult<()> {
        let (tx, rx) = oneshot::channel();
        self.request_tx
            .send(BarrierManagerRequest::AdhocRecovery(tx))
            .context("failed to send adhoc recovery request")?;
        rx.await.context("failed to wait adhoc recovery")?;
        Ok(())
    }

    pub fn update_database_barrier(
        &self,
        database_id: DatabaseId,
        barrier_interval_ms: Option<u32>,
        checkpoint_frequency: Option<u64>,
    ) -> MetaResult<()> {
        self.request_tx
            .send(BarrierManagerRequest::UpdateDatabaseBarrier {
                database_id: (database_id as u32).into(),
                barrier_interval_ms,
                checkpoint_frequency,
            })
            .context("failed to send update database barrier request")?;
        Ok(())
    }

    pub async fn get_hummock_version_id(&self) -> HummockVersionId {
        self.hummock_manager.get_version_id().await
    }
}

impl GlobalBarrierManager {
    /// Check the status of barrier manager, return error if it is not `Running`.
    pub fn check_status_running(&self) -> MetaResult<()> {
        let status = self.status.load();
        match &**status {
            BarrierManagerStatus::Starting
            | BarrierManagerStatus::Recovering(RecoveryReason::Bootstrap) => {
                bail!("The cluster is bootstrapping")
            }
            BarrierManagerStatus::Recovering(RecoveryReason::Failover(e)) => {
                Err(anyhow::anyhow!(e.clone()).context("The cluster is recovering"))?
            }
            BarrierManagerStatus::Recovering(RecoveryReason::Adhoc) => {
                bail!("The cluster is recovering-adhoc")
            }
            BarrierManagerStatus::Running => Ok(()),
        }
    }

    pub fn get_recovery_status(&self) -> PbRecoveryStatus {
        (&**self.status.load()).into()
    }
}

impl GlobalBarrierManager {
    pub async fn start(
        scheduled_barriers: schedule::ScheduledBarriers,
        env: MetaSrvEnv,
        metadata_manager: MetadataManager,
        hummock_manager: HummockManagerRef,
        source_manager: SourceManagerRef,
        sink_manager: SinkCoordinatorManager,
        scale_controller: ScaleControllerRef,
    ) -> (Arc<Self>, JoinHandle<()>, oneshot::Sender<()>) {
        let (request_tx, request_rx) = unbounded_channel();
        let hummock_manager_clone = hummock_manager.clone();
        let metadata_manager_clone = metadata_manager.clone();
        let barrier_worker = GlobalBarrierWorker::new(
            scheduled_barriers,
            env,
            metadata_manager,
            hummock_manager,
            source_manager,
            sink_manager,
            scale_controller,
            request_rx,
        )
        .await;
        let manager = Self {
            status: barrier_worker.context.status(),
            hummock_manager: hummock_manager_clone,
            request_tx,
            metadata_manager: metadata_manager_clone,
        };
        let (join_handle, shutdown_tx) = barrier_worker.start();
        (Arc::new(manager), join_handle, shutdown_tx)
    }
}
