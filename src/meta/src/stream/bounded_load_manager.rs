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

use std::sync::Arc;

use risingwave_common::id::{DatabaseId, JobId, SourceId};
use thiserror_ext::AsReport;
use tokio::sync::{Notify, oneshot};
use tokio::task::JoinHandle;

use crate::MetaResult;
use crate::barrier::bounded_progress::{
    load_startable_bounded_jobs, mark_bounded_job_complete, mark_bounded_job_failed,
};
use crate::barrier::{BarrierScheduler, Command};
use crate::manager::MetaSrvEnv;

pub type BoundedLoadManagerRef = Arc<BoundedLoadManager>;

pub struct BoundedLoadManager {
    env: MetaSrvEnv,
    barrier_scheduler: BarrierScheduler,
    scheduler_wakeup: Notify,
}

impl BoundedLoadManager {
    pub fn start(
        env: MetaSrvEnv,
        barrier_scheduler: BarrierScheduler,
    ) -> (BoundedLoadManagerRef, JoinHandle<()>, oneshot::Sender<()>) {
        let manager = Arc::new(Self {
            env,
            barrier_scheduler,
            scheduler_wakeup: Notify::new(),
        });
        let (shutdown_tx, shutdown_rx) = oneshot::channel();
        let join_handle = tokio::spawn(manager.clone().run_scheduler(shutdown_rx));
        (manager, join_handle, shutdown_tx)
    }

    pub fn notify_scheduler(&self) {
        self.scheduler_wakeup.notify_one();
    }

    async fn run_scheduler(self: Arc<Self>, mut shutdown_rx: oneshot::Receiver<()>) {
        loop {
            tokio::select! {
                biased;
                _ = &mut shutdown_rx => {
                    tracing::info!("bounded load scheduler shutting down");
                    break;
                }
                _ = self.scheduler_wakeup.notified() => {}
            }
            self.tick().await;
        }
    }

    async fn tick(&self) {
        let jobs = match load_startable_bounded_jobs(&self.env.meta_store_ref().conn).await {
            Ok(jobs) => jobs,
            Err(error) => {
                tracing::warn!(error = %error.as_report(), "failed to list bounded loads to start");
                return;
            }
        };
        for (job_id, source_id, database_id) in jobs {
            if let Err(error) = self
                .barrier_scheduler
                .run_command_no_wait(database_id, Command::StartBoundedLoad { job_id, source_id })
            {
                tracing::warn!(%job_id, %source_id, error = %error.as_report(), "failed to start bounded load");
            }
        }
    }

    /// Schedules `ListFinish` for the load of `source_id` once every list actor has reported
    /// `ListFinished`.
    pub fn list_finish(&self, database_id: DatabaseId, source_id: SourceId) {
        if let Err(error) = self
            .barrier_scheduler
            .run_command_no_wait(database_id, Command::ListFinish { source_id })
        {
            tracing::warn!(
                database_id = %database_id,
                source_id = %source_id,
                error = %error.as_report(),
                "skip bounded list finish during recovery"
            );
        }
    }

    /// Persists completion once every reader actor has reported `ReaderDone`.
    pub async fn complete(&self, job_id: JobId, source_id: SourceId) -> MetaResult<()> {
        mark_bounded_job_complete(&self.env.meta_store_ref().conn, job_id, source_id).await
    }

    /// Persists the failure once any actor has reported `LoadFailed`.
    pub async fn fail(&self, job_id: JobId, source_id: SourceId) -> MetaResult<()> {
        tracing::warn!(%job_id, %source_id, "bounded load failed, see compute logs for the error");
        mark_bounded_job_failed(&self.env.meta_store_ref().conn, job_id, source_id).await
    }
}
