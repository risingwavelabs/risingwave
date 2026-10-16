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
use risingwave_pb::stream_plan::bounded_load_mutation::{
    Action as PbBoundedLoadAction, ListSealed,
};
use thiserror_ext::AsReport;

use crate::MetaResult;
use crate::barrier::bounded_progress::mark_bounded_job_complete;
use crate::barrier::{BarrierScheduler, Command};
use crate::manager::MetaSrvEnv;

pub type BoundedLoadManagerRef = Arc<BoundedLoadManager>;

/// Applies the lifecycle steps of bounded loads that the barrier worker decides after a
/// checkpoint commits. Progress is tracked inside the barrier worker; this manager only
/// schedules the follow-up commands and persists completion.
pub struct BoundedLoadManager {
    env: MetaSrvEnv,
    barrier_scheduler: BarrierScheduler,
}

impl BoundedLoadManager {
    pub fn new(env: MetaSrvEnv, barrier_scheduler: BarrierScheduler) -> Self {
        Self {
            env,
            barrier_scheduler,
        }
    }

    /// Schedules the seal of the load of `source_id` once every list actor has reported
    /// `ListSealed`.
    pub fn seal_listing(&self, database_id: DatabaseId, source_id: SourceId) {
        if let Err(error) = self.barrier_scheduler.run_command_no_wait(
            database_id,
            Command::BoundedLoad {
                source_id,
                action: PbBoundedLoadAction::ListSealed(ListSealed {}),
            },
        ) {
            tracing::warn!(
                database_id = %database_id,
                source_id = %source_id,
                error = %error.as_report(),
                "skip bounded list seal during recovery"
            );
        }
    }

    /// Persists completion once every reader actor has reported `ReaderDone`.
    pub async fn complete(&self, job_id: JobId, source_id: SourceId) -> MetaResult<()> {
        mark_bounded_job_complete(&self.env.meta_store_ref().conn, job_id, source_id).await
    }
}
