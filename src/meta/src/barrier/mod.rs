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

use std::collections::{BTreeMap, HashMap};
use std::sync::atomic::AtomicU32;

use anyhow::anyhow;
use risingwave_common::catalog::{DatabaseId, TableId};
use risingwave_common::system_param::AdaptiveParallelismStrategy;
use risingwave_connector::source::SplitImpl;
use risingwave_meta_model::{
    FragmentId as ModelFragmentId, ObjectId as ModelObjectId, WorkerId, fragment, streaming_job,
};
use risingwave_pb::catalog::Database;
use risingwave_pb::ddl_service::DdlProgress;
use risingwave_pb::hummock::HummockVersionStats;
use risingwave_pb::meta::PbRecoveryStatus;
use tokio::sync::oneshot::Sender;

use self::notifier::Notifier;
use crate::barrier::info::BarrierInfo;
use crate::controller::fragment::{InflightActorInfo, InflightFragmentInfo};
use crate::controller::scale::{
    NoShuffleEnsemble, RenderActorsOwnedContext, WorkerInfo, render_actors_with_context,
};
use crate::controller::utils::StreamingJobExtraInfo;
use crate::manager::ActiveStreamingWorkerNodes;
use crate::model::{
    ActorId, FragmentDownstreamRelation, FragmentId, StreamActor, StreamContext, SubscriptionId,
};
use crate::rpc::ddl_controller::refill_upstream_sink_union_in_table;
use crate::stream::UpstreamSinkInfo;
use crate::{MetaError, MetaResult};

mod backfill_order_control;
pub mod cdc_progress;
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
use risingwave_connector::source::cdc::CdcTableSnapshotSplitAssignmentWithGeneration;

pub use self::command::{
    BarrierKind, Command, CreateStreamingJobCommandInfo, CreateStreamingJobType,
    ReplaceStreamJobPlan, Reschedule, SnapshotBackfillInfo,
};
pub(crate) use self::info::{SharedActorInfos, SharedFragmentInfo};
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
    database_job_infos:
        HashMap<DatabaseId, HashMap<TableId, HashMap<FragmentId, InflightFragmentInfo>>>,
    state_table_committed_epochs: HashMap<TableId, u64>,
    /// `table_id` -> (`Vec<non-checkpoint epoch>`, checkpoint epoch)
    state_table_log_epochs: HashMap<TableId, Vec<(Vec<u64>, u64)>>,
    mv_depended_subscriptions: HashMap<TableId, HashMap<SubscriptionId, u64>>,
    stream_actors: HashMap<ActorId, StreamActor>,
    fragment_relations: FragmentDownstreamRelation,
    source_splits: HashMap<ActorId, Vec<SplitImpl>>,
    background_jobs: HashMap<TableId, String>,
    hummock_version_stats: HummockVersionStats,
    database_infos: Vec<Database>,
    cdc_table_snapshot_split_assignment: CdcTableSnapshotSplitAssignmentWithGeneration,
}

impl BarrierWorkerRuntimeInfoSnapshot {
    fn validate_database_info(
        database_id: DatabaseId,
        database_jobs: &HashMap<TableId, HashMap<FragmentId, InflightFragmentInfo>>,
        active_streaming_nodes: &ActiveStreamingWorkerNodes,
        stream_actors: &HashMap<ActorId, StreamActor>,
        state_table_committed_epochs: &HashMap<TableId, u64>,
    ) -> MetaResult<()> {
        {
            for fragment in database_jobs.values().flat_map(|job| job.values()) {
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
            for (job_id, fragments) in database_jobs {
                let mut committed_epochs =
                    InflightFragmentInfo::existing_table_ids(fragments.values()).map(|table_id| {
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
struct DatabaseRenderContext {
    ensembles: Vec<NoShuffleEnsemble>,
    fragment_map: HashMap<ModelFragmentId, fragment::Model>,
    job_map: HashMap<ModelObjectId, streaming_job::Model>,
    owned_context: RenderActorsOwnedContext,
    job_extra_info: HashMap<ModelObjectId, StreamingJobExtraInfo>,
    upstream_sink_infos: HashMap<TableId, (FragmentId, Vec<UpstreamSinkInfo>)>,
}

impl DatabaseRenderContext {
    fn build_stream_actors(
        &self,
        job_infos: &HashMap<TableId, HashMap<FragmentId, InflightFragmentInfo>>,
    ) -> MetaResult<HashMap<ActorId, StreamActor>> {
        let mut stream_actors = HashMap::new();
        for (job_id, fragments) in job_infos {
            let job_id_raw = job_id.table_id as ModelObjectId;
            let StreamingJobExtraInfo {
                timezone,
                job_definition,
            } = self
                .job_extra_info
                .get(&job_id_raw)
                .cloned()
                .ok_or_else(|| anyhow!("no streaming job info for {}", job_id.table_id))?;

            let expr_context = Some(StreamContext { timezone }.to_expr_context());

            for (fragment_id, fragment_info) in fragments {
                for (actor_id, InflightActorInfo { vnode_bitmap, .. }) in &fragment_info.actors {
                    stream_actors.insert(
                        *actor_id,
                        StreamActor {
                            actor_id: *actor_id as _,
                            fragment_id: *fragment_id as _,
                            vnode_bitmap: vnode_bitmap.clone(),
                            mview_definition: job_definition.clone(),
                            expr_context: expr_context.clone(),
                        },
                    );
                }
            }
        }
        Ok(stream_actors)
    }
}

#[derive(Debug)]
struct DatabaseRuntimeInfoSnapshot {
    job_infos: HashMap<TableId, HashMap<FragmentId, InflightFragmentInfo>>,
    state_table_committed_epochs: HashMap<TableId, u64>,
    /// `table_id` -> (`Vec<non-checkpoint epoch>`, checkpoint epoch)
    state_table_log_epochs: HashMap<TableId, Vec<(Vec<u64>, u64)>>,
    mv_depended_subscriptions: HashMap<TableId, HashMap<SubscriptionId, u64>>,
    stream_actors: HashMap<ActorId, StreamActor>,
    fragment_relations: FragmentDownstreamRelation,
    source_splits: HashMap<ActorId, Vec<SplitImpl>>,
    background_jobs: HashMap<TableId, String>,
    cdc_table_snapshot_split_assignment: CdcTableSnapshotSplitAssignmentWithGeneration,
    render_context: Option<DatabaseRenderContext>,
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

    fn re_render(
        &mut self,
        database_id: DatabaseId,
        actor_id_counter: &AtomicU32,
        worker_map: &BTreeMap<WorkerId, WorkerInfo>,
        adaptive_parallelism_strategy: AdaptiveParallelismStrategy,
    ) -> MetaResult<()> {
        let Some(context) = self.render_context.take() else {
            return Ok(());
        };

        let rendered = render_actors_with_context(
            actor_id_counter,
            &context.ensembles,
            &context.fragment_map,
            &context.job_map,
            worker_map,
            adaptive_parallelism_strategy,
            &context.owned_context,
        )?;
        let mut rendered = rendered
            .into_iter()
            .map(|(db_id, job_infos)| {
                (
                    DatabaseId::new(db_id as _),
                    job_infos
                        .into_iter()
                        .map(|(job_id, fragments)| {
                            (
                                TableId::new(job_id as _),
                                fragments
                                    .into_iter()
                                    .map(|(fragment_id, info)| (fragment_id as _, info))
                                    .collect(),
                            )
                        })
                        .collect(),
                )
            })
            .collect::<HashMap<_, _>>();
        let job_infos = rendered
            .remove(&database_id)
            .ok_or_else(|| anyhow!("rendered database {} not found", database_id.database_id))?;

        self.job_infos = job_infos;

        for (table_id, (fragment_id, upstream_infos)) in &context.upstream_sink_infos {
            if let Some(fragments) = self.job_infos.get_mut(table_id) {
                if let Some(fragment) = fragments.get_mut(fragment_id) {
                    refill_upstream_sink_union_in_table(&mut fragment.nodes, upstream_infos);
                }
            }
        }

        self.source_splits.clear();
        for fragments in self.job_infos.values() {
            for fragment in fragments.values() {
                for (actor_id, info) in &fragment.actors {
                    self.source_splits.insert(*actor_id, info.splits.clone());
                }
            }
        }

        self.stream_actors = context.build_stream_actors(&self.job_infos)?;

        Ok(())
    }
}
