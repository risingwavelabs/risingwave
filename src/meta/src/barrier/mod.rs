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

use std::collections::HashMap;

use anyhow::anyhow;
use risingwave_common::catalog::{DatabaseId, TableId};
use risingwave_connector::source::SplitImpl;
use risingwave_pb::catalog::Database;
use risingwave_pb::ddl_service::DdlProgress;
use risingwave_pb::hummock::HummockVersionStats;
use risingwave_pb::meta::PbRecoveryStatus;
use tokio::sync::oneshot::Sender;

use self::notifier::Notifier;
use crate::barrier::info::{BarrierInfo, InflightStreamingJobInfo};
use crate::manager::ActiveStreamingWorkerNodes;
use crate::model::{ActorId, FragmentDownstreamRelation, StreamActor, StreamJobFragments};
use crate::{MetaError, MetaResult};

mod backfill_order_control;
mod checkpoint;
mod command;
mod complete_task;
mod context;
mod edge_builder;
mod info;
mod manager;
mod notifier;
mod progress;
mod rpc;
mod schedule;
#[cfg(test)]
mod tests;
mod trace;
mod utils;
mod worker;

pub use backfill_order_control::{BackfillNode, BackfillOrderState};
pub use worker::SharedInflightDatabaseInfo;

pub use self::command::{
    BarrierKind, Command, CreateStreamingJobCommandInfo, CreateStreamingJobType,
    ReplaceStreamJobPlan, Reschedule, SnapshotBackfillInfo,
};
pub use self::info::InflightSubscriptionInfo;
pub use self::manager::{BarrierManagerRef, GlobalBarrierManager};
pub use self::schedule::BarrierScheduler;
pub use self::trace::TracedEpoch;

/// The reason why the cluster is recovering.
enum RecoveryReason {
    /// After bootstrap.
    Bootstrap,
    /// After failure.
    Failover(MetaError),
    /// Manually triggered
    Adhoc,
}

/// Status of barrier manager.
enum BarrierManagerStatus {
    /// Barrier manager is starting.
    Starting,
    /// Barrier manager is under recovery.
    Recovering(RecoveryReason),
    /// Barrier manager is running.
    Running,
}

/// Scheduled command with its notifiers.
struct Scheduled {
    database_id: DatabaseId,
    command: Command,
    notifiers: Vec<Notifier>,
    span: tracing::Span,
}

impl From<&BarrierManagerStatus> for PbRecoveryStatus {
    fn from(status: &BarrierManagerStatus) -> Self {
        match status {
            BarrierManagerStatus::Starting => Self::StatusStarting,
            BarrierManagerStatus::Recovering(reason) => match reason {
                RecoveryReason::Bootstrap => Self::StatusStarting,
                RecoveryReason::Failover(_) | RecoveryReason::Adhoc => Self::StatusRecovering,
            },
            BarrierManagerStatus::Running => Self::StatusRunning,
        }
    }
}

pub(crate) enum BarrierManagerRequest {
    GetDdlProgress(Sender<HashMap<u32, DdlProgress>>),
    AdhocRecovery(Sender<()>),
    UpdateDatabaseBarrier {
        database_id: DatabaseId,
        barrier_interval_ms: Option<u32>,
        checkpoint_frequency: Option<u64>,
        sender: Sender<()>,
    },
}

#[derive(Debug)]
struct BarrierWorkerRuntimeInfoSnapshot {
    active_streaming_nodes: ActiveStreamingWorkerNodes,
    database_job_infos: HashMap<DatabaseId, HashMap<TableId, InflightStreamingJobInfo>>,
    state_table_committed_epochs: HashMap<TableId, u64>,
    /// `table_id` -> (`Vec<non-checkpoint epoch>`, checkpoint epoch)
    state_table_log_epochs: HashMap<TableId, Vec<(Vec<u64>, u64)>>,
    subscription_infos: HashMap<DatabaseId, InflightSubscriptionInfo>,
    stream_actors: HashMap<ActorId, StreamActor>,
    fragment_relations: FragmentDownstreamRelation,
    source_splits: HashMap<ActorId, Vec<SplitImpl>>,
    background_jobs: HashMap<TableId, (String, StreamJobFragments)>,
    hummock_version_stats: HummockVersionStats,
    database_infos: Vec<Database>,
}

impl BarrierWorkerRuntimeInfoSnapshot {
    fn validate_database_info(
        database_id: DatabaseId,
        database_jobs: &HashMap<TableId, InflightStreamingJobInfo>,
        active_streaming_nodes: &ActiveStreamingWorkerNodes,
        stream_actors: &HashMap<ActorId, StreamActor>,
        state_table_committed_epochs: &HashMap<TableId, u64>,
    ) -> MetaResult<()> {
        {
            for fragment in database_jobs.values().flat_map(|job| job.fragment_infos()) {
                for (actor_id, actor) in &fragment.actors {
                    if !active_streaming_nodes
                        .current()
                        .contains_key(&actor.worker_id)
                    {
                        return Err(anyhow!(
                            "worker_id {} of actor {} do not exist",
                            actor.worker_id,
                            actor_id
                        )
                        .into());
                    }
                    if !stream_actors.contains_key(actor_id) {
                        return Err(anyhow!("cannot find StreamActor of actor {}", actor_id).into());
                    }
                }
                for state_table_id in &fragment.state_table_ids {
                    if !state_table_committed_epochs.contains_key(state_table_id) {
                        return Err(anyhow!(
                            "state table {} is not registered to hummock",
                            state_table_id
                        )
                        .into());
                    }
                }
            }
            for (job_id, job) in database_jobs {
                let mut committed_epochs = job.existing_table_ids().map(|table_id| {
                    (
                        table_id,
                        *state_table_committed_epochs
                            .get(&table_id)
                            .expect("checked exist"),
                    )
                });
                let (first_table, first_epoch) = committed_epochs.next().ok_or_else(|| {
                    anyhow!(
                        "job {} in database {} has no state table after recovery",
                        job_id,
                        database_id
                    )
                })?;
                for (table_id, epoch) in committed_epochs {
                    if epoch != first_epoch {
                        return Err(anyhow!(
                            "job {} in database {} has tables with different table ids. {}:{}, {}:{}",
                            job_id,
                            database_id,
                            first_table,
                            first_epoch,
                            table_id,
                            epoch
                        )
                        .into());
                    }
                }
            }
        }
        Ok(())
    }

    fn validate(&self) -> MetaResult<()> {
        for (database_id, job_infos) in &self.database_job_infos {
            Self::validate_database_info(
                *database_id,
                job_infos,
                &self.active_streaming_nodes,
                &self.stream_actors,
                &self.state_table_committed_epochs,
            )?
        }
        Ok(())
    }
}

#[derive(Debug)]
struct DatabaseRuntimeInfoSnapshot {
    job_infos: HashMap<TableId, InflightStreamingJobInfo>,
    state_table_committed_epochs: HashMap<TableId, u64>,
    /// `table_id` -> (`Vec<non-checkpoint epoch>`, checkpoint epoch)
    state_table_log_epochs: HashMap<TableId, Vec<(Vec<u64>, u64)>>,
    subscription_info: InflightSubscriptionInfo,
    stream_actors: HashMap<ActorId, StreamActor>,
    fragment_relations: FragmentDownstreamRelation,
    source_splits: HashMap<ActorId, Vec<SplitImpl>>,
    background_jobs: HashMap<TableId, (String, StreamJobFragments)>,
}

impl DatabaseRuntimeInfoSnapshot {
    fn validate(
        &self,
        database_id: DatabaseId,
        active_streaming_nodes: &ActiveStreamingWorkerNodes,
    ) -> MetaResult<()> {
        BarrierWorkerRuntimeInfoSnapshot::validate_database_info(
            database_id,
            &self.job_infos,
            active_streaming_nodes,
            &self.stream_actors,
            &self.state_table_committed_epochs,
        )
    }
}
