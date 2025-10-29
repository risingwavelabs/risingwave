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

use std::collections::{BTreeMap, HashMap, HashSet};
use std::iter;
use std::sync::Arc;

use await_tree::span;
use futures::future::join_all;
use itertools::Itertools;
use risingwave_common::bail;
use risingwave_common::catalog::{DatabaseId, Field, FragmentTypeFlag, FragmentTypeMask, TableId};
use risingwave_common::hash::VnodeCountCompat;
use risingwave_connector::source::cdc::CdcTableSnapshotSplitAssignmentWithGeneration;
use risingwave_meta_model::prelude::Fragment as FragmentModel;
use risingwave_meta_model::{ObjectId, StreamingParallelism, fragment};
use risingwave_pb::catalog::{CreateType, PbSink, PbTable, Subscription};
use risingwave_pb::expr::PbExprNode;
use risingwave_pb::meta::table_fragments::ActorStatus;
use risingwave_pb::plan_common::{PbColumnCatalog, PbField};
use sea_orm::{ColumnTrait, EntityTrait, QueryFilter, QuerySelect};
use thiserror_ext::AsReport;
use tokio::sync::{Mutex, OwnedSemaphorePermit, oneshot};
use tracing::Instrument;

use super::{FragmentBackfillOrder, Locations, ReschedulePolicy, ScaleControllerRef};
use crate::barrier::{
    BarrierScheduler, Command, CreateStreamingJobCommandInfo, CreateStreamingJobType,
    ReplaceStreamJobPlan, SnapshotBackfillInfo,
};
use crate::controller::catalog::DropTableConnectorContext;
use crate::controller::fragment::{InflightActorInfo, InflightFragmentInfo};
use crate::error::bail_invalid_parameter;
use crate::manager::{
    MetaSrvEnv, MetadataManager, NotificationVersion, StreamingJob, StreamingJobType,
};
use crate::model::{
    ActorId, DownstreamFragmentRelation, Fragment, FragmentDownstreamRelation, FragmentId,
    FragmentNewNoShuffle, FragmentReplaceUpstream, StreamJobFragments, StreamJobFragmentsToCreate,
};
use crate::stream::SourceManagerRef;
use crate::stream::cdc::{
    assign_cdc_table_snapshot_splits, is_parallelized_backfill_enabled_cdc_scan_fragment,
};
use crate::{MetaError, MetaResult};

pub type GlobalStreamManagerRef = Arc<GlobalStreamManager>;

#[derive(Default)]
pub struct CreateStreamingJobOption {
    // leave empty as a placeholder for future option if there is any
}

#[derive(Debug, Clone)]
pub struct UpstreamSinkInfo {
    pub sink_id: ObjectId,
    pub sink_fragment_id: FragmentId,
    pub sink_output_fields: Vec<PbField>,
    // for backwards compatibility
    pub sink_original_target_columns: Vec<PbColumnCatalog>,
    pub project_exprs: Vec<PbExprNode>,
    pub new_sink_downstream: DownstreamFragmentRelation,
}

/// [`CreateStreamingJobContext`] carries one-time infos for creating a streaming job.
///
/// Note: for better readability, keep this struct complete and immutable once created.
pub struct CreateStreamingJobContext {
    /// New fragment relation to add from upstream fragments to downstream fragments.
    pub upstream_fragment_downstreams: FragmentDownstreamRelation,
    pub new_no_shuffle: FragmentNewNoShuffle,
    pub upstream_actors: HashMap<FragmentId, HashSet<ActorId>>,

    /// The locations of the actors to build in the streaming job.
    pub building_locations: Locations,

    /// DDL definition.
    pub definition: String,

    pub mv_table_id: Option<u32>,

    pub create_type: CreateType,

    pub job_type: StreamingJobType,

    /// Used for sink-into-table.
    pub new_upstream_sink: Option<UpstreamSinkInfo>,

    pub snapshot_backfill_info: Option<SnapshotBackfillInfo>,
    pub cross_db_snapshot_backfill_info: SnapshotBackfillInfo,

    pub option: CreateStreamingJobOption,

    pub streaming_job: StreamingJob,

    pub fragment_backfill_ordering: FragmentBackfillOrder,

    pub locality_fragment_state_table_mapping: HashMap<FragmentId, Vec<TableId>>,
}

struct StreamingJobExecution {
    id: TableId,
    shutdown_tx: Option<oneshot::Sender<oneshot::Sender<bool>>>,
    _permit: OwnedSemaphorePermit,
}

impl StreamingJobExecution {
    fn new(
        id: TableId,
        shutdown_tx: oneshot::Sender<oneshot::Sender<bool>>,
        permit: OwnedSemaphorePermit,
    ) -> Self {
        Self {
            id,
            shutdown_tx: Some(shutdown_tx),
            _permit: permit,
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
    ) -> (HashMap<TableId, oneshot::Receiver<bool>>, Vec<TableId>) {
        let mut jobs = self.streaming_jobs.lock().await;
        let mut receivers = HashMap::new();
        let mut background_job_ids = vec![];
        for job_id in job_ids {
            if let Some(job) = jobs.get_mut(&job_id) {
                if let Some(shutdown_tx) = job.shutdown_tx.take() {
                    let (tx, rx) = oneshot::channel();
                    if shutdown_tx.send(tx).is_ok() {
                        receivers.insert(job_id, rx);
                    } else {
                        tracing::warn!(id=?job_id, "failed to send canceling state");
                    }
                }
            } else {
                // If these job ids do not exist in streaming_jobs, they should be background creating jobs.
                background_job_ids.push(job_id);
            }
        }
        (receivers, background_job_ids)
    }

    async fn check_job_exists(&self, job_id: TableId) -> bool {
        let jobs = self.streaming_jobs.lock().await;
        jobs.contains_key(&job_id)
    }
}

type CreatingStreamingJobInfoRef = Arc<CreatingStreamingJobInfo>;

#[derive(Debug, Clone)]
pub struct AutoRefreshSchemaSinkContext {
    pub tmp_sink_id: ObjectId,
    pub original_sink: PbSink,
    pub original_fragment: Fragment,
    pub new_schema: Vec<PbColumnCatalog>,
    pub newly_add_fields: Vec<Field>,
    pub new_fragment: Fragment,
    pub new_log_store_table: Option<PbTable>,
    pub actor_status: BTreeMap<ActorId, ActorStatus>,
}

impl AutoRefreshSchemaSinkContext {
    pub fn new_fragment_info(&self) -> InflightFragmentInfo {
        InflightFragmentInfo {
            fragment_id: self.new_fragment.fragment_id,
            distribution_type: self.new_fragment.distribution_type.into(),
            fragment_type_mask: self.new_fragment.fragment_type_mask,
            vnode_count: self.new_fragment.vnode_count(),
            nodes: self.new_fragment.nodes.clone(),
            actors: self
                .new_fragment
                .actors
                .iter()
                .map(|actor| {
                    (
                        actor.actor_id as _,
                        InflightActorInfo {
                            worker_id: self.actor_status[&actor.actor_id]
                                .location
                                .as_ref()
                                .unwrap()
                                .worker_node_id as _,
                            vnode_bitmap: actor.vnode_bitmap.clone(),
                            splits: vec![],
                        },
                    )
                })
                .collect(),
            state_table_ids: self
                .new_fragment
                .state_table_ids
                .iter()
                .map(|table| (*table).into())
                .collect(),
        }
    }
}

/// [`ReplaceStreamJobContext`] carries one-time infos for replacing the plan of an existing stream job.
///
/// Note: for better readability, keep this struct complete and immutable once created.
pub struct ReplaceStreamJobContext {
    /// The old job fragments to be replaced.
    pub old_fragments: StreamJobFragments,

    /// The updates to be applied to the downstream chain actors. Used for schema change.
    pub replace_upstream: FragmentReplaceUpstream,
    pub new_no_shuffle: FragmentNewNoShuffle,

    /// New fragment relation to add from existing upstream fragment to downstream fragment.
    pub upstream_fragment_downstreams: FragmentDownstreamRelation,

    /// The locations of the actors to build in the new job to replace.
    pub building_locations: Locations,

    pub streaming_job: StreamingJob,

    pub tmp_id: u32,

    /// Used for dropping an associated source. Dropping source and related internal tables.
    pub drop_table_connector_ctx: Option<DropTableConnectorContext>,

    pub auto_refresh_schema_sinks: Option<Vec<AutoRefreshSchemaSinkContext>>,
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
    ///
    /// This function is a wrapper over [`Self::run_create_streaming_job_command`].
    #[await_tree::instrument]
    pub async fn create_streaming_job(
        self: &Arc<Self>,
        stream_job_fragments: StreamJobFragmentsToCreate,
        ctx: CreateStreamingJobContext,
        permit: OwnedSemaphorePermit,
    ) -> MetaResult<NotificationVersion> {
        let await_tree_key = format!("Create Streaming Job Worker ({})", ctx.streaming_job.id());
        let await_tree_span = span!(
            "{:?}({})",
            ctx.streaming_job.job_type(),
            ctx.streaming_job.name()
        );

        let table_id = stream_job_fragments.stream_job_id();
        let database_id = ctx.streaming_job.database_id().into();

        let (cancel_tx, cancel_rx) = oneshot::channel();
        let execution = StreamingJobExecution::new(table_id, cancel_tx, permit);
        self.creating_job_info.add_job(execution).await;

        let stream_manager = self.clone();
        let fut = async move {
            let create_type = ctx.create_type;
            let streaming_job = stream_manager
                .run_create_streaming_job_command(stream_job_fragments, ctx)
                .await?;
            let version = match create_type {
                CreateType::Background => {
                    stream_manager
                        .env
                        .notification_manager_ref()
                        .current_version()
                        .await
                }
                CreateType::Foreground => {
                    stream_manager
                        .metadata_manager
                        .wait_streaming_job_finished(database_id, streaming_job.id() as _)
                        .await?
                }
                CreateType::Unspecified => unreachable!(),
            };

            tracing::debug!(?streaming_job, "stream job finish");
            Ok(version)
        }
        .in_current_span();

        let create_fut = (self.env.await_tree_reg())
            .register(await_tree_key, await_tree_span)
            .instrument(Box::pin(fut));

        let result = tokio::select! {
            biased;

            res = create_fut => res,
            notifier = cancel_rx => {
                let notifier = notifier.expect("sender should not be dropped");
                tracing::debug!(id=?table_id, "cancelling streaming job");

                if let Ok(job_fragments) = self.metadata_manager.get_job_fragments_by_id(&table_id)
                    .await {
                    // try to cancel buffered creating command.
                    if self.barrier_scheduler.try_cancel_scheduled_create(database_id, table_id) {
                        tracing::debug!("cancelling streaming job {table_id} in buffer queue.");
                    } else if !job_fragments.is_created() {
                        tracing::debug!("cancelling streaming job {table_id} by issue cancel command.");

                        let cancel_command = self.metadata_manager.catalog_controller
                            .build_cancel_command(&job_fragments)
                            .await?;
                        self.metadata_manager.catalog_controller
                            .try_abort_creating_streaming_job(table_id.table_id as _, true)
                            .await?;

                        self.barrier_scheduler.run_command(database_id, cancel_command).await?;
                    } else {
                        // streaming job is already completed
                        let _ = notifier.send(false).inspect_err(|err| tracing::warn!("failed to notify cancellation result: {err}"));
                        return self.metadata_manager.wait_streaming_job_finished(database_id, table_id.table_id as _).await;
                    }
                }
                notifier.send(true).expect("receiver should not be dropped");
                Err(MetaError::cancelled("create"))
            }
        };

        tracing::info!("cleaning creating job info: {}", table_id);
        self.creating_job_info.delete_job(table_id).await;
        result
    }

    /// The function will return after barrier collected
    /// ([`crate::manager::MetadataManager::wait_streaming_job_finished`]).
    #[await_tree::instrument]
    async fn run_create_streaming_job_command(
        &self,
        stream_job_fragments: StreamJobFragmentsToCreate,
        CreateStreamingJobContext {
            streaming_job,
            upstream_fragment_downstreams,
            new_no_shuffle,
            upstream_actors,
            definition,
            create_type,
            job_type,
            new_upstream_sink,
            snapshot_backfill_info,
            cross_db_snapshot_backfill_info,
            fragment_backfill_ordering,
            locality_fragment_state_table_mapping,
            ..
        }: CreateStreamingJobContext,
    ) -> MetaResult<StreamingJob> {
        tracing::debug!(
            table_id = %stream_job_fragments.stream_job_id(),
            "built actors finished"
        );

        // Here we need to consider:
        // - Shared source
        // - Table with connector
        // - MV on shared source
        let mut init_split_assignment = self
            .source_manager
            .allocate_splits(&stream_job_fragments)
            .await?;

        init_split_assignment.extend(
            self.source_manager
                .allocate_splits_for_backfill(
                    &stream_job_fragments,
                    &new_no_shuffle,
                    &upstream_actors,
                )
                .await?,
        );

        let cdc_table_snapshot_split_assignment = assign_cdc_table_snapshot_splits(
            stream_job_fragments.stream_job_id.table_id,
            &stream_job_fragments,
            self.env.meta_store_ref(),
        )
        .await?;
        let cdc_table_snapshot_split_assignment = if !cdc_table_snapshot_split_assignment.is_empty()
        {
            self.env.cdc_table_backfill_tracker.track_new_job(
                stream_job_fragments.stream_job_id.table_id,
                cdc_table_snapshot_split_assignment
                    .values()
                    .map(|s| u64::try_from(s.len()).unwrap())
                    .sum(),
            );
            self.env
                .cdc_table_backfill_tracker
                .add_fragment_table_mapping(
                    stream_job_fragments
                        .fragments
                        .values()
                        .filter(|f| is_parallelized_backfill_enabled_cdc_scan_fragment(f))
                        .map(|f| f.fragment_id),
                    stream_job_fragments.stream_job_id.table_id,
                );
            CdcTableSnapshotSplitAssignmentWithGeneration::new(
                cdc_table_snapshot_split_assignment,
                self.env
                    .cdc_table_backfill_tracker
                    .next_generation(iter::once(stream_job_fragments.stream_job_id.table_id)),
            )
        } else {
            CdcTableSnapshotSplitAssignmentWithGeneration::empty()
        };

        let info = CreateStreamingJobCommandInfo {
            stream_job_fragments,
            upstream_fragment_downstreams,
            init_split_assignment,
            definition: definition.clone(),
            streaming_job: streaming_job.clone(),
            job_type,
            create_type,
            fragment_backfill_ordering,
            cdc_table_snapshot_split_assignment,
            locality_fragment_state_table_mapping,
        };

        let job_type = if let Some(snapshot_backfill_info) = snapshot_backfill_info {
            tracing::debug!(
                ?snapshot_backfill_info,
                "sending Command::CreateSnapshotBackfillStreamingJob"
            );
            CreateStreamingJobType::SnapshotBackfill(snapshot_backfill_info)
        } else {
            tracing::debug!("sending Command::CreateStreamingJob");
            if let Some(new_upstream_sink) = new_upstream_sink {
                CreateStreamingJobType::SinkIntoTable(new_upstream_sink)
            } else {
                CreateStreamingJobType::Normal
            }
        };

        let command = Command::CreateStreamingJob {
            info,
            job_type,
            cross_db_snapshot_backfill_info,
        };

        self.barrier_scheduler
            .run_command(streaming_job.database_id().into(), command)
            .await?;

        tracing::debug!(?streaming_job, "first barrier collected for stream job");

        Ok(streaming_job)
    }

    /// Send replace job command to barrier scheduler.
    pub async fn replace_stream_job(
        &self,
        new_fragments: StreamJobFragmentsToCreate,
        ReplaceStreamJobContext {
            old_fragments,
            replace_upstream,
            new_no_shuffle,
            upstream_fragment_downstreams,
            tmp_id,
            streaming_job,
            drop_table_connector_ctx,
            auto_refresh_schema_sinks,
            ..
        }: ReplaceStreamJobContext,
    ) -> MetaResult<()> {
        let init_split_assignment = if streaming_job.is_source() {
            self.source_manager
                .allocate_splits_for_replace_source(
                    &new_fragments,
                    &replace_upstream,
                    &new_no_shuffle,
                )
                .await?
        } else {
            self.source_manager.allocate_splits(&new_fragments).await?
        };
        tracing::info!(
            "replace_stream_job - allocate split: {:?}",
            init_split_assignment
        );

        let cdc_table_snapshot_split_assignment = assign_cdc_table_snapshot_splits(
            old_fragments.stream_job_id.table_id,
            &new_fragments.inner,
            self.env.meta_store_ref(),
        )
        .await?;

        self.barrier_scheduler
            .run_command(
                streaming_job.database_id().into(),
                Command::ReplaceStreamJob(ReplaceStreamJobPlan {
                    old_fragments,
                    new_fragments,
                    replace_upstream,
                    upstream_fragment_downstreams,
                    init_split_assignment,
                    streaming_job,
                    tmp_id,
                    to_drop_state_table_ids: {
                        if let Some(drop_table_connector_ctx) = &drop_table_connector_ctx {
                            vec![TableId::new(
                                drop_table_connector_ctx.to_remove_state_table_id as _,
                            )]
                        } else {
                            Vec::new()
                        }
                    },
                    auto_refresh_schema_sinks,
                    cdc_table_snapshot_split_assignment,
                }),
            )
            .await?;

        Ok(())
    }

    /// Drop streaming jobs by barrier manager, and clean up all related resources. The error will
    /// be ignored because the recovery process will take over it in cleaning part. Check
    /// [`Command::DropStreamingJobs`] for details.
    pub async fn drop_streaming_jobs(
        &self,
        database_id: DatabaseId,
        removed_actors: Vec<ActorId>,
        streaming_job_ids: Vec<ObjectId>,
        state_table_ids: Vec<risingwave_meta_model::TableId>,
        fragment_ids: HashSet<FragmentId>,
        dropped_sink_fragment_by_targets: HashMap<FragmentId, Vec<FragmentId>>,
    ) {
        // TODO(august): This is a workaround for canceling SITT via drop, remove it after refactoring SITT.
        for &job_id in &streaming_job_ids {
            if self
                .creating_job_info
                .check_job_exists(TableId::new(job_id as _))
                .await
            {
                tracing::info!(
                    ?job_id,
                    "streaming job is creating, cancel it with drop directly"
                );
                self.metadata_manager
                    .notify_cancelled(database_id, job_id)
                    .await;
            }
        }

        if !removed_actors.is_empty()
            || !streaming_job_ids.is_empty()
            || !state_table_ids.is_empty()
        {
            let _ = self
                .barrier_scheduler
                .run_command(
                    database_id,
                    Command::DropStreamingJobs {
                        streaming_job_ids: streaming_job_ids
                            .iter()
                            .map(|job_id| TableId::new(*job_id as _))
                            .collect(),
                        actors: removed_actors,
                        unregistered_state_table_ids: state_table_ids
                            .iter()
                            .map(|table_id| TableId::new(*table_id as _))
                            .collect(),
                        unregistered_fragment_ids: fragment_ids,
                        dropped_sink_fragment_by_targets,
                    },
                )
                .await
                .inspect_err(|err| {
                    tracing::error!(error = ?err.as_report(), "failed to run drop command");
                });
        }
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
        let (receivers, background_job_ids) = self.creating_job_info.cancel_jobs(table_ids).await;

        let futures = receivers.into_iter().map(|(id, receiver)| async move {
            if let Ok(cancelled) = receiver.await
                && cancelled
            {
                tracing::info!("canceled streaming job {id}");
                Some(id)
            } else {
                tracing::warn!("failed to cancel streaming job {id}");
                None
            }
        });
        let mut cancelled_ids = join_all(futures).await.into_iter().flatten().collect_vec();

        // NOTE(kwannoel): For background_job_ids stream jobs that not tracked in streaming manager,
        // we can directly cancel them by running the barrier command.
        let futures = background_job_ids.into_iter().map(|id| async move {
            tracing::debug!(?id, "cancelling background streaming job");
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

                let cancel_command = self
                    .metadata_manager
                    .catalog_controller
                    .build_cancel_command(&fragment)
                    .await?;

                let (_, database_id) = self.metadata_manager
                    .catalog_controller
                    .try_abort_creating_streaming_job(id.table_id as _, true)
                    .await?;

                if let Some(database_id) = database_id {
                    self.barrier_scheduler
                        .run_command(DatabaseId::new(database_id as _), cancel_command)
                        .await?;
                }
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

    pub(crate) async fn reschedule_streaming_job(
        &self,
        job_id: u32,
        policy: ReschedulePolicy,
        deferred: bool,
    ) -> MetaResult<()> {
        let _reschedule_job_lock = self.reschedule_lock_write_guard().await;

        let background_jobs = self
            .metadata_manager
            .list_background_creating_jobs()
            .await?;

        if !background_jobs.is_empty() {
            let related_jobs = self
                .scale_controller
                .resolve_related_no_shuffle_jobs(&background_jobs)
                .await?;

            if related_jobs.contains(&(job_id as ObjectId)) {
                bail!(
                    "Cannot alter the job {} because the related job {:?} is currently being created",
                    job_id,
                    background_jobs,
                );
            }
        }

        let job_id = TableId::new(job_id);

        let worker_nodes = self
            .metadata_manager
            .list_active_streaming_compute_nodes()
            .await?
            .into_iter()
            .filter(|w| w.is_streaming_schedulable())
            .collect_vec();
        let workers = worker_nodes.into_iter().map(|x| (x.id as i32, x)).collect();

        let commands = self
            .scale_controller
            .reschedule_inplace(HashMap::from([(job_id.table_id as _, policy)]), workers)
            .await?;

        if !deferred {
            let _source_pause_guard = self.source_manager.pause_tick().await;

            for (database_id, command) in commands {
                self.barrier_scheduler
                    .run_command(database_id, command)
                    .await?;
            }
        }

        Ok(())
    }

    /// This method is copied from `GlobalStreamManager::reschedule_streaming_job` and modified to handle reschedule CDC table backfill.
    pub(crate) async fn reschedule_cdc_table_backfill(
        &self,
        job_id: u32,
        target: ReschedulePolicy,
    ) -> MetaResult<()> {
        let _reschedule_job_lock = self.reschedule_lock_write_guard().await;

        let job_id = TableId::new(job_id);

        let parallelism_policy = match target {
            ReschedulePolicy::Parallelism(policy)
                if matches!(policy.parallelism, StreamingParallelism::Fixed(_)) =>
            {
                policy
            }
            _ => bail_invalid_parameter!(
                "CDC backfill reschedule only supports fixed parallelism targets"
            ),
        };

        let worker_nodes = self
            .metadata_manager
            .list_active_streaming_compute_nodes()
            .await?
            .into_iter()
            .filter(|w| w.is_streaming_schedulable())
            .collect_vec();
        let workers = worker_nodes.into_iter().map(|x| (x.id as i32, x)).collect();

        let cdc_fragment_id = {
            let inner = self.metadata_manager.catalog_controller.inner.read().await;
            let fragments: Vec<(risingwave_meta_model::FragmentId, i32)> = FragmentModel::find()
                .select_only()
                .columns([
                    fragment::Column::FragmentId,
                    fragment::Column::FragmentTypeMask,
                ])
                .filter(fragment::Column::JobId.eq(job_id.table_id as i32))
                .into_tuple()
                .all(&inner.db)
                .await?;

            let cdc_fragments = fragments
                .into_iter()
                .filter_map(|(fragment_id, mask)| {
                    FragmentTypeMask::from(mask)
                        .contains(FragmentTypeFlag::StreamCdcScan)
                        .then_some(fragment_id)
                })
                .collect_vec();

            match cdc_fragments.len() {
                0 => bail_invalid_parameter!(
                    "no StreamCdcScan fragments found for job {}",
                    job_id.table_id
                ),
                1 => cdc_fragments[0],
                _ => bail_invalid_parameter!(
                    "multiple StreamCdcScan fragments found for job {}; expected exactly one",
                    job_id.table_id
                ),
            }
        };

        let fragment_policy = HashMap::from([(cdc_fragment_id, parallelism_policy.clone())]);

        let commands = self
            .scale_controller
            .reschedule_fragment_inplace(fragment_policy, workers)
            .await?;

        let _source_pause_guard = self.source_manager.pause_tick().await;

        for (database_id, command) in commands {
            self.barrier_scheduler
                .run_command(database_id, command)
                .await?;
        }

        Ok(())
    }

    // Don't need to add actor, just send a command
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
        self.barrier_scheduler
            .run_command(subscription.database_id.into(), command)
            .await?;
        Ok(())
    }

    // Don't need to add actor, just send a command
    pub async fn drop_subscription(
        self: &Arc<Self>,
        database_id: DatabaseId,
        subscription_id: u32,
        table_id: u32,
    ) {
        let command = Command::DropSubscription {
            subscription_id,
            upstream_mv_table_id: TableId::new(table_id),
        };

        tracing::debug!("sending Command::DropSubscriptions");
        let _ = self
            .barrier_scheduler
            .run_command(database_id, command)
            .await
            .inspect_err(|err| {
                tracing::error!(error = ?err.as_report(), "failed to run drop command");
            });
    }
}
