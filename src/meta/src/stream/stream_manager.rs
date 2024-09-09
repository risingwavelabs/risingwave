// Copyright 2024 RisingWave Labs
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

use std::collections::{BTreeSet, HashMap, HashSet};
use std::sync::Arc;

use futures::future::join_all;
use itertools::Itertools;
use risingwave_common::bail;
use risingwave_common::catalog::TableId;
use risingwave_meta_model_v2::ObjectId;
use risingwave_pb::catalog::{CreateType, Subscription, Table};
use risingwave_pb::stream_plan::update_mutation::MergeUpdate;
use risingwave_pb::stream_plan::Dispatcher;
use thiserror_ext::AsReport;
use tokio::sync::mpsc::Sender;
use tokio::sync::{oneshot, Mutex};
use tracing::Instrument;

use super::{Locations, RescheduleOptions, ScaleControllerRef, TableResizePolicy};
use crate::barrier::{
    BarrierScheduler, Command, CreateStreamingJobCommandInfo, CreateStreamingJobType,
    ReplaceTablePlan, SnapshotBackfillInfo,
};
use crate::manager::{DdlType, MetaSrvEnv, MetadataManager, NotificationVersion, StreamingJob};
use crate::model::{ActorId, FragmentId, MetadataModel, TableFragments, TableParallelism};
use crate::stream::SourceManagerRef;
use crate::{MetaError, MetaResult};

pub type GlobalStreamManagerRef = Arc<GlobalStreamManager>;

#[derive(Default)]
pub struct CreateStreamingJobOption {
    // leave empty as a place holder for future option if there is any
}

/// [`CreateStreamingJobContext`] carries one-time infos for creating a streaming job.
///
/// Note: for better readability, keep this struct complete and immutable once created.
pub struct CreateStreamingJobContext {
    /// New dispatchers to add from upstream actors to downstream actors.
    pub dispatchers: HashMap<ActorId, Vec<Dispatcher>>,

    /// Upstream root fragments' actor ids grouped by table id.
    ///
    /// Refer to the doc on [`MetadataManager::get_upstream_root_fragments`] for the meaning of "root".
    pub upstream_root_actors: HashMap<TableId, Vec<ActorId>>,

    /// Internal tables in the streaming job.
    pub internal_tables: HashMap<u32, Table>,

    /// The locations of the actors to build in the streaming job.
    pub building_locations: Locations,

    /// The locations of the existing actors, essentially the upstream mview actors to update.
    pub existing_locations: Locations,

    /// DDL definition.
    pub definition: String,

    pub mv_table_id: Option<u32>,

    pub create_type: CreateType,

    pub ddl_type: DdlType,

    /// Context provided for potential replace table, typically used when sinking into a table.
    pub replace_table_job_info: Option<(StreamingJob, ReplaceTableContext, TableFragments)>,

    pub snapshot_backfill_info: Option<SnapshotBackfillInfo>,

    pub option: CreateStreamingJobOption,

    pub streaming_job: StreamingJob,
}

impl CreateStreamingJobContext {
    pub fn internal_tables(&self) -> Vec<Table> {
        self.internal_tables.values().cloned().collect()
    }
}

pub enum CreatingState {
    Failed { reason: MetaError },
    // sender is used to notify the canceling result.
    Canceling { finish_tx: oneshot::Sender<()> },
    Created { version: NotificationVersion },
}

struct StreamingJobExecution {
    id: TableId,
    shutdown_tx: Option<Sender<CreatingState>>,
}

impl StreamingJobExecution {
    fn new(id: TableId, shutdown_tx: Sender<CreatingState>) -> Self {
        Self {
            id,
            shutdown_tx: Some(shutdown_tx),
        }
    }
}

#[derive(Default)]
struct CreatingStreamingJobInfo {
    streaming_jobs: Mutex<HashMap<TableId, StreamingJobExecution>>,
}

impl CreatingStreamingJobInfo {
    async fn add_job(&self, job: StreamingJobExecution) {
        let mut jobs = self.streaming_jobs.lock().await;
        jobs.insert(job.id, job);
    }

    async fn delete_job(&self, job_id: TableId) {
        let mut jobs = self.streaming_jobs.lock().await;
        jobs.remove(&job_id);
    }

    async fn cancel_jobs(
        &self,
        job_ids: Vec<TableId>,
    ) -> (HashMap<TableId, oneshot::Receiver<()>>, Vec<TableId>) {
        let mut jobs = self.streaming_jobs.lock().await;
        let mut receivers = HashMap::new();
        let mut recovered_job_ids = vec![];
        for job_id in job_ids {
            if let Some(job) = jobs.get_mut(&job_id) {
                if let Some(shutdown_tx) = job.shutdown_tx.take() {
                    let (tx, rx) = oneshot::channel();
                    if shutdown_tx
                        .send(CreatingState::Canceling { finish_tx: tx })
                        .await
                        .is_ok()
                    {
                        receivers.insert(job_id, rx);
                    } else {
                        tracing::warn!(id=?job_id, "failed to send canceling state");
                    }
                }
            } else {
                // If these job ids do not exist in streaming_jobs,
                // we can infer they either:
                // 1. are entirely non-existent,
                // 2. OR they are recovered streaming jobs, and managed by BarrierManager.
                recovered_job_ids.push(job_id);
            }
        }
        (receivers, recovered_job_ids)
    }
}

type CreatingStreamingJobInfoRef = Arc<CreatingStreamingJobInfo>;

/// [`ReplaceTableContext`] carries one-time infos for replacing the plan of an existing table.
///
/// Note: for better readability, keep this struct complete and immutable once created.
pub struct ReplaceTableContext {
    /// The old table fragments to be replaced.
    pub old_table_fragments: TableFragments,

    /// The updates to be applied to the downstream chain actors. Used for schema change.
    pub merge_updates: Vec<MergeUpdate>,

    /// New dispatchers to add from upstream actors to downstream actors.
    pub dispatchers: HashMap<ActorId, Vec<Dispatcher>>,

    /// The locations of the actors to build in the new table to replace.
    pub building_locations: Locations,

    /// The locations of the existing actors, essentially the downstream chain actors to update.
    pub existing_locations: Locations,

    pub streaming_job: StreamingJob,

    pub dummy_id: u32,
}

/// `GlobalStreamManager` manages all the streams in the system.
pub struct GlobalStreamManager {
    pub env: MetaSrvEnv,

    pub metadata_manager: MetadataManager,

    /// Broadcasts and collect barriers
    pub barrier_scheduler: BarrierScheduler,

    /// Maintains streaming sources from external system like kafka
    pub source_manager: SourceManagerRef,

    /// Creating streaming job info.
    creating_job_info: CreatingStreamingJobInfoRef,

    pub scale_controller: ScaleControllerRef,
}

impl GlobalStreamManager {
    pub fn new(
        env: MetaSrvEnv,
        metadata_manager: MetadataManager,
        barrier_scheduler: BarrierScheduler,
        source_manager: SourceManagerRef,
        scale_controller: ScaleControllerRef,
    ) -> MetaResult<Self> {
        Ok(Self {
            env,
            metadata_manager,
            barrier_scheduler,
            source_manager,
            creating_job_info: Arc::new(CreatingStreamingJobInfo::default()),
            scale_controller,
        })
    }

    /// Create streaming job, it works as follows:
    ///
    /// 1. Broadcast the actor info based on the scheduling result in the context, build the hanging
    ///    channels in upstream worker nodes.
    /// 2. (optional) Get the split information of the `StreamSource` via source manager and patch
    ///    actors.
    /// 3. Notify related worker nodes to update and build the actors.
    /// 4. Store related meta data.
    pub async fn create_streaming_job(
        self: &Arc<Self>,
        table_fragments: TableFragments,
        ctx: CreateStreamingJobContext,
    ) -> MetaResult<NotificationVersion> {
        let table_id = table_fragments.table_id();
        let (sender, mut receiver) = tokio::sync::mpsc::channel(10);
        let execution = StreamingJobExecution::new(table_id, sender.clone());
        self.creating_job_info.add_job(execution).await;

        let stream_manager = self.clone();
        let fut = async move {
            let res = stream_manager
                .create_streaming_job_impl(table_fragments, ctx)
                .await;
            match res {
                Ok(version) => {
                    let _ = sender
                        .send(CreatingState::Created { version })
                        .await
                        .inspect_err(|_| tracing::warn!("failed to notify created: {table_id}"));
                }
                Err(err) => {
                    let _ = sender
                        .send(CreatingState::Failed {
                            reason: err.clone(),
                        })
                        .await
                        .inspect_err(|_| {
                            tracing::warn!(error = %err.as_report(), "failed to notify failed: {table_id}")
                        });
                }
            }
        }
        .in_current_span();
        tokio::spawn(fut);

        while let Some(state) = receiver.recv().await {
            match state {
                CreatingState::Failed { reason } => {
                    tracing::debug!(id=?table_id, "stream job failed");
                    // FIXME(kwannoel): For creating stream jobs
                    // we need to clean up the resources in the stream manager.
                    self.creating_job_info.delete_job(table_id).await;
                    return Err(reason);
                }
                CreatingState::Canceling { finish_tx } => {
                    tracing::debug!(id=?table_id, "cancelling streaming job");
                    if let Ok(table_fragments) = self
                        .metadata_manager
                        .get_job_fragments_by_id(&table_id)
                        .await
                    {
                        // try to cancel buffered creating command.
                        if self.barrier_scheduler.try_cancel_scheduled_create(table_id) {
                            tracing::debug!("cancelling streaming job {table_id} in buffer queue.");

                            if let MetadataManager::V1(mgr) = &self.metadata_manager {
                                mgr.fragment_manager
                                    .drop_table_fragments_vec(&HashSet::from_iter(std::iter::once(
                                        table_id,
                                    )))
                                    .await?;
                            }
                        } else if !table_fragments.is_created() {
                            tracing::debug!(
                                "cancelling streaming job {table_id} by issue cancel command."
                            );

                            self.barrier_scheduler
                                .run_command(Command::CancelStreamingJob(table_fragments))
                                .await?;
                        } else {
                            // streaming job is already completed.
                            continue;
                        }
                        let _ = finish_tx.send(()).inspect_err(|_| {
                            tracing::warn!("failed to notify cancelled: {table_id}")
                        });
                        self.creating_job_info.delete_job(table_id).await;
                        return Err(MetaError::cancelled("create"));
                    }
                }
                CreatingState::Created { version } => {
                    self.creating_job_info.delete_job(table_id).await;
                    return Ok(version);
                }
            }
        }
        self.creating_job_info.delete_job(table_id).await;
        bail!("receiver failed to get notification version for finished stream job")
    }

    async fn create_streaming_job_impl(
        &self,
        table_fragments: TableFragments,
        CreateStreamingJobContext {
            streaming_job,
            dispatchers,
            upstream_root_actors,
            definition,
            create_type,
            ddl_type,
            replace_table_job_info,
            internal_tables,
            snapshot_backfill_info,
            ..
        }: CreateStreamingJobContext,
    ) -> MetaResult<NotificationVersion> {
        let mut replace_table_command = None;
        let mut replace_table_id = None;

        tracing::debug!(
            table_id = %table_fragments.table_id(),
            "built actors finished"
        );

        let need_pause = replace_table_job_info.is_some();

        if let Some((streaming_job, context, table_fragments)) = replace_table_job_info {
            match &self.metadata_manager {
                MetadataManager::V1(mgr) => {
                    // Add table fragments to meta store with state: `State::Initial`.
                    mgr.fragment_manager
                        .start_create_table_fragments(table_fragments.clone())
                        .await?
                }
                MetadataManager::V2(mgr) => {
                    mgr.catalog_controller
                        .prepare_streaming_job(table_fragments.to_protobuf(), &streaming_job, true)
                        .await?
                }
            }

            let dummy_table_id = table_fragments.table_id();
            let init_split_assignment =
                self.source_manager.allocate_splits(&dummy_table_id).await?;

            replace_table_id = Some(dummy_table_id);

            replace_table_command = Some(ReplaceTablePlan {
                old_table_fragments: context.old_table_fragments,
                new_table_fragments: table_fragments,
                merge_updates: context.merge_updates,
                dispatchers: context.dispatchers,
                init_split_assignment,
                streaming_job,
                dummy_id: dummy_table_id.table_id,
            });
        }

        let table_id = table_fragments.table_id();

        // Here we need to consider:
        // - Shared source
        // - Table with connector
        // - MV on shared source
        let mut init_split_assignment = self.source_manager.allocate_splits(&table_id).await?;
        init_split_assignment.extend(
            self.source_manager
                .allocate_splits_for_backfill(&table_id, &dispatchers)
                .await?,
        );

        let info = CreateStreamingJobCommandInfo {
            table_fragments,
            upstream_root_actors,
            dispatchers,
            init_split_assignment,
            definition: definition.to_string(),
            streaming_job: streaming_job.clone(),
            internal_tables: internal_tables.into_values().collect_vec(),
            ddl_type,
            create_type,
        };

        let command = if let Some(snapshot_backfill_info) = snapshot_backfill_info {
            tracing::debug!(
                ?snapshot_backfill_info,
                "sending Command::CreateSnapshotBackfillStreamingJob"
            );
            Command::CreateStreamingJob {
                info,
                job_type: CreateStreamingJobType::SnapshotBackfill(snapshot_backfill_info),
            }
        } else {
            tracing::debug!("sending Command::CreateStreamingJob");
            if let Some(replace_table_command) = replace_table_command {
                Command::CreateStreamingJob {
                    info,
                    job_type: CreateStreamingJobType::SinkIntoTable(replace_table_command),
                }
            } else {
                Command::CreateStreamingJob {
                    info,
                    job_type: CreateStreamingJobType::Normal,
                }
            }
        };
        let result: MetaResult<NotificationVersion> = try {
            if need_pause {
                // Special handling is required when creating sink into table, we need to pause the stream to avoid data loss.
                self.barrier_scheduler
                    .run_config_change_command_with_pause(command)
                    .await?;
            } else {
                self.barrier_scheduler.run_command(command).await?;
            }

            tracing::debug!(?streaming_job, "first barrier collected for stream job");
            let result = self
                .metadata_manager
                .wait_streaming_job_finished(&streaming_job)
                .await?;
            tracing::debug!(?streaming_job, "stream job finish");
            result
        };
        match result {
            Err(err) => {
                if create_type == CreateType::Foreground || err.is_cancelled() {
                    let mut table_ids = HashSet::from_iter(std::iter::once(table_id));
                    if let Some(dummy_table_id) = replace_table_id {
                        table_ids.insert(dummy_table_id);
                    }
                    if let MetadataManager::V1(mgr) = &self.metadata_manager {
                        mgr.fragment_manager
                            .drop_table_fragments_vec(&table_ids)
                            .await?;
                    }
                }

                Err(err)
            }
            Ok(version) => Ok(version),
        }
    }

    pub async fn replace_table(
        &self,
        table_fragments: TableFragments,
        ReplaceTableContext {
            old_table_fragments,
            merge_updates,
            dispatchers,
            dummy_id,
            streaming_job,
            ..
        }: ReplaceTableContext,
    ) -> MetaResult<()> {
        let dummy_table_id = table_fragments.table_id();
        let init_split_assignment = self.source_manager.allocate_splits(&dummy_table_id).await?;

        if let Err(err) = self
            .barrier_scheduler
            .run_config_change_command_with_pause(Command::ReplaceTable(ReplaceTablePlan {
                old_table_fragments,
                new_table_fragments: table_fragments,
                merge_updates,
                dispatchers,
                init_split_assignment,
                dummy_id,
                streaming_job,
            }))
            .await
            && let MetadataManager::V1(mgr) = &self.metadata_manager
        {
            mgr.fragment_manager
                .drop_table_fragments_vec(&HashSet::from_iter(std::iter::once(dummy_table_id)))
                .await?;
            return Err(err);
        }

        Ok(())
    }

    /// Drop streaming jobs by barrier manager, and clean up all related resources. The error will
    /// be ignored because the recovery process will take over it in cleaning part. Check
    /// [`Command::DropStreamingJobs`] for details.
    pub async fn drop_streaming_jobs(&self, streaming_job_ids: Vec<TableId>) {
        if !streaming_job_ids.is_empty() {
            let _ = self
                .drop_streaming_jobs_impl(streaming_job_ids)
                .await
                .inspect_err(|err| {
                    tracing::error!(error = ?err.as_report(), "Failed to drop streaming jobs");
                });
        }
    }

    pub async fn drop_streaming_jobs_v2(
        &self,
        removed_actors: Vec<ActorId>,
        streaming_job_ids: Vec<ObjectId>,
        state_table_ids: Vec<risingwave_meta_model_v2::TableId>,
        fragment_ids: HashSet<FragmentId>,
    ) {
        if !removed_actors.is_empty()
            || !streaming_job_ids.is_empty()
            || !state_table_ids.is_empty()
        {
            let _ = self
                .barrier_scheduler
                .run_command(Command::DropStreamingJobs {
                    actors: removed_actors,
                    unregistered_state_table_ids: state_table_ids
                        .into_iter()
                        .map(|table_id| TableId::new(table_id as _))
                        .collect(),
                    unregistered_fragment_ids: fragment_ids,
                })
                .await
                .inspect_err(|err| {
                    tracing::error!(error = ?err.as_report(), "failed to run drop command");
                });
        }
    }

    pub async fn drop_streaming_jobs_impl(&self, table_ids: Vec<TableId>) -> MetaResult<()> {
        let mgr = self.metadata_manager.as_v1_ref();
        let table_fragments_vec = mgr
            .fragment_manager
            .select_table_fragments_by_ids(&table_ids)
            .await?;

        self.source_manager
            .drop_source_fragments(&table_fragments_vec)
            .await;

        // Drop table fragments directly.
        let unregister_table_ids = mgr
            .fragment_manager
            .drop_table_fragments_vec(&table_ids.iter().cloned().collect())
            .await?;

        // Issues a drop barrier command.
        let dropped_actors = table_fragments_vec
            .iter()
            .flat_map(|tf| tf.actor_ids().into_iter())
            .collect_vec();
        let _ = self
            .barrier_scheduler
            .run_command(Command::DropStreamingJobs {
                actors: dropped_actors,
                unregistered_state_table_ids: unregister_table_ids
                    .into_iter()
                    .map(TableId::new)
                    .collect(),
                unregistered_fragment_ids: table_fragments_vec
                    .iter()
                    .flat_map(|fragments| fragments.fragments.keys().cloned())
                    .collect(),
            })
            .await
            .inspect_err(|err| {
                tracing::error!(error = ?err.as_report(), "failed to run drop command");
            });

        Ok(())
    }

    /// Cancel streaming jobs and return the canceled table ids.
    /// 1. Send cancel message to stream jobs (via `cancel_jobs`).
    /// 2. Send cancel message to recovered stream jobs (via `barrier_scheduler`).
    ///
    /// Cleanup of their state will be cleaned up after the `CancelStreamJob` command succeeds,
    /// by the barrier manager for both of them.
    pub async fn cancel_streaming_jobs(&self, table_ids: Vec<TableId>) -> Vec<TableId> {
        if table_ids.is_empty() {
            return vec![];
        }

        let _reschedule_job_lock = self.reschedule_lock_read_guard().await;
        let (receivers, recovered_job_ids) = self.creating_job_info.cancel_jobs(table_ids).await;

        let futures = receivers.into_iter().map(|(id, receiver)| async move {
            if receiver.await.is_ok() {
                tracing::info!("canceled streaming job {id}");
                Some(id)
            } else {
                tracing::warn!("failed to cancel streaming job {id}");
                None
            }
        });
        let mut cancelled_ids = join_all(futures).await.into_iter().flatten().collect_vec();

        // NOTE(kwannoel): For recovered stream jobs, we can directly cancel them by running the barrier command,
        // since Barrier manager manages the recovered stream jobs.
        let futures = recovered_job_ids.into_iter().map(|id| async move {
            tracing::debug!(?id, "cancelling recovered streaming job");
            let result: MetaResult<()> = try {
                let fragment = self
                    .metadata_manager.get_job_fragments_by_id(&id)
                    .await?;
                if fragment.is_created() {
                    Err(MetaError::invalid_parameter(format!(
                        "streaming job {} is already created",
                        id
                    )))?;
                }
                if let MetadataManager::V1(mgr) = &self.metadata_manager {
                    mgr.catalog_manager.cancel_create_materialized_view_procedure(id.into(), fragment.internal_table_ids()).await?;
                }

                self.barrier_scheduler
                    .run_command(Command::CancelStreamingJob(fragment))
                    .await?;
            };
            match result {
                Ok(_) => {
                    tracing::info!(?id, "cancelled recovered streaming job");
                    Some(id)
                }
                Err(err) => {
                    tracing::error!(error=?err.as_report(), "failed to cancel recovered streaming job {id}, does it correspond to any jobs in `SHOW JOBS`?");
                    None
                }
            }
        });
        let cancelled_recovered_ids = join_all(futures).await.into_iter().flatten().collect_vec();

        cancelled_ids.extend(cancelled_recovered_ids);
        cancelled_ids
    }

    pub(crate) async fn alter_table_parallelism(
        &self,
        table_id: u32,
        parallelism: TableParallelism,
        deferred: bool,
    ) -> MetaResult<()> {
        let _reschedule_job_lock = self.reschedule_lock_write_guard().await;

        let worker_nodes = self
            .metadata_manager
            .list_active_streaming_compute_nodes()
            .await?;

        let worker_ids = worker_nodes
            .iter()
            .filter(|w| w.property.as_ref().map_or(true, |p| !p.is_unschedulable))
            .map(|node| node.id)
            .collect::<BTreeSet<_>>();

        let table_parallelism_assignment = HashMap::from([(TableId::new(table_id), parallelism)]);

        if deferred {
            tracing::debug!(
                "deferred mode enabled for job {}, set the parallelism directly to {:?}",
                table_id,
                parallelism
            );
            self.scale_controller
                .post_apply_reschedule(&HashMap::new(), &table_parallelism_assignment)
                .await?;
        } else {
            let reschedules = self
                .scale_controller
                .generate_table_resize_plan(TableResizePolicy {
                    worker_ids,
                    table_parallelisms: table_parallelism_assignment
                        .iter()
                        .map(|(id, parallelism)| (id.table_id, *parallelism))
                        .collect(),
                })
                .await?;

            if reschedules.is_empty() {
                tracing::debug!("empty reschedule plan generated for job {}, set the parallelism directly to {:?}", table_id, parallelism);
                self.scale_controller
                    .post_apply_reschedule(&HashMap::new(), &table_parallelism_assignment)
                    .await?;
            } else {
                self.reschedule_actors(
                    reschedules,
                    RescheduleOptions {
                        resolve_no_shuffle_upstream: false,
                        skip_create_new_actors: false,
                    },
                    Some(table_parallelism_assignment),
                )
                .await?;
            }
        };

        Ok(())
    }

    // Dont need add actor, just send a command
    pub async fn create_subscription(
        self: &Arc<Self>,
        subscription: &Subscription,
    ) -> MetaResult<()> {
        let command = Command::CreateSubscription {
            subscription_id: subscription.id,
            upstream_mv_table_id: TableId::new(subscription.dependent_table_id),
            retention_second: subscription.retention_seconds,
        };

        tracing::debug!("sending Command::CreateSubscription");
        self.barrier_scheduler.run_command(command).await?;
        Ok(())
    }

    // Dont need add actor, just send a command
    pub async fn drop_subscription(self: &Arc<Self>, subscription_id: u32, table_id: u32) {
        let command = Command::DropSubscription {
            subscription_id,
            upstream_mv_table_id: TableId::new(table_id),
        };

        tracing::debug!("sending Command::DropSubscriptions");
        let _ = self
            .barrier_scheduler
            .run_command(command)
            .await
            .inspect_err(|err| {
                tracing::error!(error = ?err.as_report(), "failed to run drop command");
            });
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::net::SocketAddr;
    use std::sync::Mutex;
    use std::time::Duration;

    use futures::{Stream, TryStreamExt};
    use risingwave_common::hash::{self, ActorMapping, VirtualNode, WorkerSlotId};
    use risingwave_common::system_param::reader::SystemParamsRead;
    use risingwave_pb::common::{HostAddress, WorkerType};
    use risingwave_pb::meta::add_worker_node_request::Property;
    use risingwave_pb::meta::table_fragments::fragment::FragmentDistributionType;
    use risingwave_pb::meta::table_fragments::Fragment;
    use risingwave_pb::stream_plan::stream_node::NodeBody;
    use risingwave_pb::stream_plan::*;
    use risingwave_pb::stream_service::stream_service_server::{
        StreamService, StreamServiceServer,
    };
    use risingwave_pb::stream_service::streaming_control_stream_response::InitResponse;
    use risingwave_pb::stream_service::*;
    use tokio::spawn;
    use tokio::sync::mpsc::unbounded_channel;
    use tokio::sync::oneshot::Sender;
    #[cfg(feature = "failpoints")]
    use tokio::sync::Notify;
    use tokio::task::JoinHandle;
    use tokio_stream::wrappers::UnboundedReceiverStream;
    use tonic::{Request, Response, Status, Streaming};

    use super::*;
    use crate::barrier::GlobalBarrierManager;
    use crate::hummock::{CompactorManager, HummockManager};
    use crate::manager::sink_coordination::SinkCoordinatorManager;
    use crate::manager::{
        CatalogManager, CatalogManagerRef, ClusterManager, FragmentManager, FragmentManagerRef,
        RelationIdEnum, StreamingClusterInfo, WorkerId,
    };
    use crate::rpc::ddl_controller::DropMode;
    use crate::rpc::metrics::MetaMetrics;
    use crate::stream::{ScaleController, SourceManager};
    use crate::MetaOpts;

    struct FakeFragmentState {
        actor_streams: Mutex<HashMap<ActorId, StreamActor>>,
        actor_ids: Mutex<HashSet<ActorId>>,
        actor_infos: Mutex<HashMap<ActorId, HostAddress>>,
    }

    struct FakeStreamService {
        worker_id: WorkerId,
        inner: Arc<FakeFragmentState>,
    }

    #[async_trait::async_trait]
    impl StreamService for FakeStreamService {
        type StreamingControlStreamStream =
            impl Stream<Item = std::result::Result<StreamingControlStreamResponse, tonic::Status>>;

        async fn streaming_control_stream(
            &self,
            request: Request<Streaming<StreamingControlStreamRequest>>,
        ) -> Result<Response<Self::StreamingControlStreamStream>, Status> {
            let (tx, rx) = unbounded_channel();
            let mut request_stream = request.into_inner();
            let inner = self.inner.clone();
            let worker_id = self.worker_id;
            let _join_handle = spawn(async move {
                while let Ok(Some(request)) = request_stream.try_next().await {
                    match request.request.unwrap() {
                        streaming_control_stream_request::Request::Init(_) => {
                            inner.actor_streams.lock().unwrap().clear();
                            inner.actor_ids.lock().unwrap().clear();
                            inner.actor_infos.lock().unwrap().clear();
                            let _ = tx.send(Ok(StreamingControlStreamResponse {
                                response: Some(streaming_control_stream_response::Response::Init(
                                    InitResponse {},
                                )),
                            }));
                        }
                        streaming_control_stream_request::Request::InjectBarrier(req) => {
                            {
                                let mut guard = inner.actor_infos.lock().unwrap();
                                for info in req.broadcast_info {
                                    guard.insert(
                                        info.get_actor_id(),
                                        info.get_host().unwrap().clone(),
                                    );
                                }
                            }
                            {
                                let mut guard = inner.actor_streams.lock().unwrap();
                                let mut actor_ids = inner.actor_ids.lock().unwrap();
                                for actor in req.actors_to_build {
                                    let actor = actor.actor.as_ref().unwrap();
                                    assert!(actor_ids.insert(actor.actor_id));
                                    guard.insert(actor.get_actor_id(), actor.clone());
                                }
                            }
                            let _ = tx.send(Ok(StreamingControlStreamResponse {
                                response: Some(
                                    streaming_control_stream_response::Response::CompleteBarrier(
                                        BarrierCompleteResponse {
                                            epoch: req.barrier.unwrap().epoch.unwrap().prev,
                                            worker_id,
                                            partial_graph_id: req.partial_graph_id,
                                            ..BarrierCompleteResponse::default()
                                        },
                                    ),
                                ),
                            }));
                        }
                        streaming_control_stream_request::Request::RemovePartialGraph(..) => {}
                    }
                }
            });
            Ok(Response::new(UnboundedReceiverStream::new(rx)))
        }

        async fn wait_epoch_commit(
            &self,
            _request: Request<WaitEpochCommitRequest>,
        ) -> std::result::Result<Response<WaitEpochCommitResponse>, Status> {
            Ok(Response::new(WaitEpochCommitResponse::default()))
        }
    }

    struct MockServices {
        global_stream_manager: GlobalStreamManagerRef,
        catalog_manager: CatalogManagerRef,
        fragment_manager: FragmentManagerRef,
        state: Arc<FakeFragmentState>,
        join_handle_shutdown_txs: Vec<(JoinHandle<()>, Sender<()>)>,
    }

    impl MockServices {
        async fn start(host: &str, port: u16, enable_recovery: bool) -> MetaResult<Self> {
            let addr = SocketAddr::new(host.parse().unwrap(), port);
            let state = Arc::new(FakeFragmentState {
                actor_streams: Mutex::new(HashMap::new()),
                actor_ids: Mutex::new(HashSet::new()),
                actor_infos: Mutex::new(HashMap::new()),
            });

            let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel::<()>();

            let env = MetaSrvEnv::for_test_opts(MetaOpts::test(enable_recovery)).await;
            let system_params = env.system_params_reader().await;
            let meta_metrics = Arc::new(MetaMetrics::default());
            let cluster_manager =
                Arc::new(ClusterManager::new(env.clone(), Duration::from_secs(3600)).await?);
            let host = HostAddress {
                host: host.to_string(),
                port: port as i32,
            };
            let fake_parallelism = 4;
            let worker_node = cluster_manager
                .add_worker_node(
                    WorkerType::ComputeNode,
                    host.clone(),
                    Property {
                        worker_node_parallelism: fake_parallelism,
                        is_streaming: true,
                        is_serving: true,
                        is_unschedulable: false,
                        internal_rpc_host_addr: "".to_string(),
                    },
                    Default::default(),
                )
                .await?;
            cluster_manager.activate_worker_node(host).await?;

            let fake_service = FakeStreamService {
                worker_id: worker_node.id,
                inner: state.clone(),
            };
            let stream_srv = StreamServiceServer::new(fake_service);
            let join_handle = tokio::spawn(async move {
                tonic::transport::Server::builder()
                    .add_service(stream_srv)
                    .serve_with_shutdown(addr, async move { shutdown_rx.await.unwrap() })
                    .await
                    .unwrap();
            });

            let catalog_manager = Arc::new(CatalogManager::new(env.clone()).await?);
            let fragment_manager = Arc::new(FragmentManager::new(env.clone()).await?);

            let compactor_manager =
                Arc::new(CompactorManager::with_meta(env.clone()).await.unwrap());

            let (tx, _rx) = tokio::sync::mpsc::unbounded_channel();

            let metadata_manager = MetadataManager::new_v1(
                cluster_manager.clone(),
                catalog_manager.clone(),
                fragment_manager.clone(),
            );

            let hummock_manager = HummockManager::new(
                env.clone(),
                metadata_manager.clone(),
                meta_metrics.clone(),
                compactor_manager.clone(),
                tx,
            )
            .await?;

            let (barrier_scheduler, scheduled_barriers) = BarrierScheduler::new_pair(
                hummock_manager.clone(),
                meta_metrics.clone(),
                system_params.checkpoint_frequency() as usize,
            );

            let source_manager = Arc::new(
                SourceManager::new(
                    barrier_scheduler.clone(),
                    metadata_manager.clone(),
                    meta_metrics.clone(),
                )
                .await?,
            );

            let (sink_manager, _) = SinkCoordinatorManager::start_worker();

            let scale_controller = Arc::new(ScaleController::new(
                &metadata_manager,
                source_manager.clone(),
                env.clone(),
            ));

            let barrier_manager = GlobalBarrierManager::new(
                scheduled_barriers,
                env.clone(),
                metadata_manager.clone(),
                hummock_manager,
                source_manager.clone(),
                sink_manager,
                meta_metrics.clone(),
                scale_controller.clone(),
            )
            .await;

            let stream_manager = GlobalStreamManager::new(
                env.clone(),
                metadata_manager,
                barrier_scheduler.clone(),
                source_manager.clone(),
                scale_controller.clone(),
            )?;

            let (join_handle_2, shutdown_tx_2) = GlobalBarrierManager::start(barrier_manager);

            // Wait until the bootstrap recovery is done.
            loop {
                tokio::time::sleep(Duration::from_millis(100)).await;
                if barrier_scheduler.flush(false).await.is_ok() {
                    break;
                }
            }

            Ok(Self {
                global_stream_manager: Arc::new(stream_manager),
                catalog_manager,
                fragment_manager,
                state,
                join_handle_shutdown_txs: vec![
                    (join_handle_2, shutdown_tx_2),
                    (join_handle, shutdown_tx),
                ],
            })
        }

        async fn create_materialized_view(
            &self,
            table_id: TableId,
            fragments: BTreeMap<FragmentId, Fragment>,
        ) -> MetaResult<()> {
            // Create fake locations where all actors are scheduled to the same worker.
            let locations = {
                let StreamingClusterInfo { worker_nodes, .. }: StreamingClusterInfo = self
                    .global_stream_manager
                    .metadata_manager
                    .get_streaming_cluster_info()
                    .await?;

                let (worker_id, _worker_node) = worker_nodes.iter().exactly_one().unwrap();

                let actor_locations = fragments
                    .values()
                    .flat_map(|f| &f.actors)
                    .sorted_by(|a, b| a.actor_id.cmp(&b.actor_id))
                    .enumerate()
                    .map(|(idx, a)| (a.actor_id, WorkerSlotId::new(*worker_id, idx)))
                    .collect();

                Locations {
                    actor_locations,
                    worker_locations: worker_nodes,
                }
            };

            let table = Table {
                id: table_id.table_id(),
                ..Default::default()
            };
            let table_fragments = TableFragments::new(
                table_id,
                fragments,
                &locations.actor_locations,
                Default::default(),
                TableParallelism::Adaptive,
            );
            let ctx = CreateStreamingJobContext {
                building_locations: locations,
                streaming_job: StreamingJob::MaterializedView(table.clone()),
                mv_table_id: Some(table_fragments.table_id().table_id),
                dispatchers: Default::default(),
                upstream_root_actors: Default::default(),
                internal_tables: Default::default(),
                existing_locations: Default::default(),
                definition: "".to_string(),
                create_type: Default::default(),
                ddl_type: Default::default(),
                replace_table_job_info: None,
                snapshot_backfill_info: None,
                option: Default::default(),
            };

            self.catalog_manager
                .start_create_table_procedure(&table)
                .await?;
            self.fragment_manager
                .start_create_table_fragments(table_fragments.clone())
                .await?;
            self.global_stream_manager
                .create_streaming_job(table_fragments, ctx)
                .await?;
            self.catalog_manager
                .finish_create_materialized_view_procedure(vec![], table)
                .await?;
            Ok(())
        }

        async fn drop_materialized_views(&self, table_ids: Vec<TableId>) -> MetaResult<()> {
            for table_id in &table_ids {
                self.catalog_manager
                    .drop_relation(
                        RelationIdEnum::Table(table_id.table_id),
                        self.fragment_manager.clone(),
                        DropMode::Restrict,
                    )
                    .await?;
            }
            self.global_stream_manager
                .drop_streaming_jobs_impl(table_ids)
                .await?;
            Ok(())
        }

        async fn stop(self) {
            for (join_handle, shutdown_tx) in self.join_handle_shutdown_txs {
                shutdown_tx.send(()).unwrap();
                join_handle.await.unwrap();
            }
        }
    }

    fn make_mview_stream_actors(table_id: &TableId, count: usize) -> Vec<StreamActor> {
        let mut actor_bitmaps: HashMap<_, _> = ActorMapping::new_uniform(
            (0..count).map(|i| i as hash::ActorId),
            VirtualNode::COUNT_FOR_TEST,
        )
        .to_bitmaps()
        .into_iter()
        .map(|(actor_id, bitmap)| (actor_id, bitmap.to_protobuf()))
        .collect();

        (0..count)
            .map(|i| StreamActor {
                actor_id: i as u32,
                vnode_bitmap: actor_bitmaps.remove(&(i as u32)),
                // A dummy node to avoid panic.
                nodes: Some(StreamNode {
                    node_body: Some(NodeBody::Materialize(MaterializeNode {
                        table_id: table_id.table_id(),
                        ..Default::default()
                    })),
                    operator_id: 1,
                    ..Default::default()
                }),
                ..Default::default()
            })
            .collect_vec()
    }

    #[tokio::test]
    async fn test_drop_materialized_view() -> MetaResult<()> {
        let services = MockServices::start("127.0.0.1", 12334, false).await?;

        let table_id = TableId::new(0);
        let actors = make_mview_stream_actors(&table_id, 4);

        let mut fragments = BTreeMap::default();

        fragments.insert(
            0,
            Fragment {
                fragment_id: 0,
                fragment_type_mask: FragmentTypeFlag::Mview as u32,
                distribution_type: FragmentDistributionType::Hash as i32,
                actors: actors.clone(),
                state_table_ids: vec![0],
                ..Default::default()
            },
        );
        services
            .create_materialized_view(table_id, fragments)
            .await?;

        let actor_len = services.state.actor_streams.lock().unwrap().len();
        assert_eq!(actor_len, 4); // assert that actors are created

        let mview_actor_ids = services
            .fragment_manager
            .get_table_mview_actor_ids(&table_id)
            .await?;
        let actor_ids = services
            .fragment_manager
            .get_table_actor_ids(&HashSet::from([table_id]))
            .await?;
        assert_eq!(mview_actor_ids, (0..=3).collect::<Vec<u32>>());
        assert_eq!(actor_ids, (0..=3).collect::<Vec<u32>>());

        // test drop materialized_view
        services.drop_materialized_views(vec![table_id]).await?;

        // test get table_fragment;
        let select_err_1 = services
            .global_stream_manager
            .metadata_manager
            .get_job_fragments_by_id(&table_id)
            .await
            .unwrap_err();

        assert_eq!(select_err_1.to_string(), "table_fragment not exist: id=0");

        services.stop().await;
        Ok(())
    }

    #[tokio::test]
    #[cfg(all(test, feature = "failpoints"))]
    async fn test_failpoints_drop_mv_recovery() {
        let inject_barrier_err = "inject_barrier_err";
        let inject_barrier_err_success = "inject_barrier_err_success";
        let services = MockServices::start("127.0.0.1", 12335, true).await.unwrap();

        let table_id = TableId::new(0);
        let actors = make_mview_stream_actors(&table_id, 4);

        let mut fragments = BTreeMap::default();
        fragments.insert(
            0,
            Fragment {
                fragment_id: 0,
                fragment_type_mask: FragmentTypeFlag::Mview as u32,
                distribution_type: FragmentDistributionType::Hash as i32,
                actors: actors.clone(),
                state_table_ids: vec![0],
                ..Default::default()
            },
        );

        services
            .create_materialized_view(table_id, fragments)
            .await
            .unwrap();

        let actor_len = services.state.actor_streams.lock().unwrap().len();
        assert_eq!(actor_len, 4); // assert that actors are created

        let mview_actor_ids = services
            .fragment_manager
            .get_table_mview_actor_ids(&table_id)
            .await
            .unwrap();
        let actor_ids = services
            .fragment_manager
            .get_table_actor_ids(&HashSet::from([table_id]))
            .await
            .unwrap();
        assert_eq!(mview_actor_ids, (0..=3).collect::<Vec<u32>>());
        assert_eq!(actor_ids, (0..=3).collect::<Vec<u32>>());
        let notify = Arc::new(Notify::new());
        let notify1 = notify.clone();

        // test recovery.
        fail::cfg(inject_barrier_err, "return").unwrap();
        tokio::spawn(async move {
            fail::cfg_callback(inject_barrier_err_success, move || {
                fail::remove(inject_barrier_err);
                fail::remove(inject_barrier_err_success);
                notify.notify_one();
            })
            .unwrap();
        });
        notify1.notified().await;

        let table_fragments = services
            .global_stream_manager
            .metadata_manager
            .get_job_fragments_by_id(&table_id)
            .await
            .unwrap();
        assert_eq!(table_fragments.actor_ids(), (0..=3).collect_vec());

        // test drop materialized_view
        tokio::time::sleep(Duration::from_secs(2)).await;
        services
            .drop_materialized_views(vec![table_id])
            .await
            .unwrap();

        // test get table_fragment;
        let select_err_1 = services
            .global_stream_manager
            .metadata_manager
            .get_job_fragments_by_id(&table_fragments.table_id())
            .await
            .unwrap_err();

        assert_eq!(select_err_1.to_string(), "table_fragment not exist: id=0");

        services.stop().await;
    }
}
