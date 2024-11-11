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

use std::collections::HashMap;
use std::mem::{replace, take};
use std::sync::Arc;
use std::time::Duration;

use arc_swap::ArcSwap;
use itertools::Itertools;
use risingwave_common::system_param::reader::SystemParamsRead;
use risingwave_common::system_param::PAUSE_ON_NEXT_BOOTSTRAP_KEY;
use risingwave_common::util::epoch::Epoch;
use risingwave_meta_model::WorkerId;
use risingwave_pb::meta::subscribe_response::{Info, Operation};
use risingwave_pb::meta::{PausedReason, Recovery};
use risingwave_pb::stream_plan::barrier_mutation::Mutation;
use risingwave_pb::stream_plan::AddMutation;
use thiserror_ext::AsReport;
use tokio::sync::mpsc;
use tokio::sync::oneshot::{Receiver, Sender};
use tokio::task::JoinHandle;
use tokio::time::Instant;
use tokio_retry::strategy::{jitter, ExponentialBackoff};
use tracing::{debug, error, info, warn, Instrument};

use crate::barrier::checkpoint::{
    BarrierWorkerState, CheckpointControl, DatabaseCheckpointControl,
};
use crate::barrier::complete_task::{BarrierCompleteOutput, CompletingTask};
use crate::barrier::context::{GlobalBarrierWorkerContext, GlobalBarrierWorkerContextImpl};
use crate::barrier::info::BarrierInfo;
use crate::barrier::progress::CreateMviewProgressTracker;
use crate::barrier::rpc::{merge_node_rpc_errors, ControlStreamManager};
use crate::barrier::schedule::PeriodicBarriers;
use crate::barrier::{
    schedule, BarrierKind, BarrierManagerRequest, BarrierManagerStatus,
    BarrierWorkerRuntimeInfoSnapshot, RecoveryReason, TracedEpoch,
};
use crate::error::MetaErrorInner;
use crate::hummock::HummockManagerRef;
use crate::manager::sink_coordination::SinkCoordinatorManager;
use crate::manager::{
    ActiveStreamingWorkerChange, ActiveStreamingWorkerNodes, LocalNotification, MetaSrvEnv,
    MetadataManager,
};
use crate::model::ActorId;
use crate::rpc::metrics::GLOBAL_META_METRICS;
use crate::stream::{build_actor_connector_splits, ScaleControllerRef, SourceManagerRef};
use crate::{MetaError, MetaResult};

/// [`crate::barrier::GlobalBarrierWorker`] sends barriers to all registered compute nodes and
/// collect them, with monotonic increasing epoch numbers. On compute nodes, `LocalBarrierManager`
/// in `risingwave_stream` crate will serve these requests and dispatch them to source actors.
///
/// Configuration change in our system is achieved by the mutation in the barrier. Thus,
/// [`crate::barrier::GlobalBarrierWorker`] provides a set of interfaces like a state machine,
/// accepting [`Command`] that carries info to build `Mutation`. To keep the consistency between
/// barrier manager and meta store, some actions like "drop materialized view" or "create mv on mv"
/// must be done in barrier manager transactional using [`Command`].
pub(super) struct GlobalBarrierWorker<C> {
    /// Enable recovery or not when failover.
    enable_recovery: bool,

    /// The queue of scheduled barriers.
    periodic_barriers: PeriodicBarriers,

    /// The max barrier nums in flight
    in_flight_barrier_nums: usize,

    pub(super) context: Arc<C>,

    env: MetaSrvEnv,

    checkpoint_control: CheckpointControl,

    /// Command that has been collected but is still completing.
    /// The join handle of the completing future is stored.
    completing_task: CompletingTask,

    request_rx: mpsc::UnboundedReceiver<BarrierManagerRequest>,

    active_streaming_nodes: ActiveStreamingWorkerNodes,

    sink_manager: SinkCoordinatorManager,

    control_stream_manager: ControlStreamManager,
}

impl GlobalBarrierWorker<GlobalBarrierWorkerContextImpl> {
    /// Create a new [`crate::barrier::GlobalBarrierWorker`].
    pub async fn new(
        scheduled_barriers: schedule::ScheduledBarriers,
        env: MetaSrvEnv,
        metadata_manager: MetadataManager,
        hummock_manager: HummockManagerRef,
        source_manager: SourceManagerRef,
        sink_manager: SinkCoordinatorManager,
        scale_controller: ScaleControllerRef,
        request_rx: mpsc::UnboundedReceiver<BarrierManagerRequest>,
    ) -> Self {
        let enable_recovery = env.opts.enable_recovery;
        let in_flight_barrier_nums = env.opts.in_flight_barrier_nums;

        let active_streaming_nodes =
            ActiveStreamingWorkerNodes::uninitialized(metadata_manager.clone());

        let status = Arc::new(ArcSwap::new(Arc::new(BarrierManagerStatus::Starting)));

        let context = Arc::new(GlobalBarrierWorkerContextImpl {
            scheduled_barriers,
            status,
            metadata_manager,
            hummock_manager,
            source_manager,
            scale_controller,
            env: env.clone(),
        });

        let control_stream_manager = ControlStreamManager::new(env.clone());

        let checkpoint_frequency = env.system_params_reader().await.checkpoint_frequency() as _;
        let interval =
            Duration::from_millis(env.system_params_reader().await.barrier_interval_ms() as u64);
        let periodic_barriers = PeriodicBarriers::new(interval, checkpoint_frequency);
        tracing::info!(
            "Starting barrier scheduler with: checkpoint_frequency={:?}",
            checkpoint_frequency,
        );

        Self {
            enable_recovery,
            periodic_barriers,
            in_flight_barrier_nums,
            context,
            env,
            checkpoint_control: CheckpointControl::default(),
            completing_task: CompletingTask::None,
            request_rx,
            active_streaming_nodes,
            sink_manager,
            control_stream_manager,
        }
    }

    pub fn start(self) -> (JoinHandle<()>, Sender<()>) {
        let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel();
        let join_handle = tokio::spawn(async move {
            self.run(shutdown_rx).await;
        });

        (join_handle, shutdown_tx)
    }

    /// Check whether we should pause on bootstrap from the system parameter and reset it.
    async fn take_pause_on_bootstrap(&mut self) -> MetaResult<bool> {
        let paused = self
            .env
            .system_params_reader()
            .await
            .pause_on_next_bootstrap();
        if paused {
            warn!(
                "The cluster will bootstrap with all data sources paused as specified by the system parameter `{}`. \
                 It will now be reset to `false`. \
                 To resume the data sources, either restart the cluster again or use `risectl meta resume`.",
                PAUSE_ON_NEXT_BOOTSTRAP_KEY
            );
            self.env
                .system_params_manager_impl_ref()
                .set_param(PAUSE_ON_NEXT_BOOTSTRAP_KEY, Some("false".to_owned()))
                .await?;
        }
        Ok(paused)
    }

    /// Start an infinite loop to take scheduled barriers and send them.
    async fn run(mut self, shutdown_rx: Receiver<()>) {
        tracing::info!(
            "Starting barrier manager with: enable_recovery={}, in_flight_barrier_nums={}",
            self.enable_recovery,
            self.in_flight_barrier_nums,
        );

        if !self.enable_recovery {
            let job_exist = self
                .context
                .metadata_manager
                .catalog_controller
                .has_any_streaming_jobs()
                .await
                .unwrap();
            if job_exist {
                panic!(
                    "Some streaming jobs already exist in meta, please start with recovery enabled \
                or clean up the metadata using `./risedev clean-data`"
                );
            }
        }

        {
            // Bootstrap recovery. Here we simply trigger a recovery process to achieve the
            // consistency.
            // Even if there's no actor to recover, we still go through the recovery process to
            // inject the first `Initial` barrier.
            let span = tracing::info_span!("bootstrap_recovery");
            crate::telemetry::report_event(
                risingwave_pb::telemetry::TelemetryEventStage::Recovery,
                "normal_recovery",
                0,
                None,
                None,
                None,
            );

            let paused = self.take_pause_on_bootstrap().await.unwrap_or(false);
            let paused_reason = paused.then_some(PausedReason::Manual);

            self.recovery(paused_reason, None, RecoveryReason::Bootstrap)
                .instrument(span)
                .await;
        }

        self.run_inner(shutdown_rx).await
    }

    async fn run_inner(mut self, mut shutdown_rx: Receiver<()>) {
        let (local_notification_tx, mut local_notification_rx) =
            tokio::sync::mpsc::unbounded_channel();
        self.env
            .notification_manager()
            .insert_local_sender(local_notification_tx)
            .await;

        // Start the event loop.
        loop {
            tokio::select! {
                biased;

                // Shutdown
                _ = &mut shutdown_rx => {
                    tracing::info!("Barrier manager is stopped");
                    break;
                }

                request = self.request_rx.recv() => {
                    if let Some(request) = request {
                        match request {
                            BarrierManagerRequest::GetDdlProgress(result_tx) => {
                                let mut progress = HashMap::new();
                                for database_checkpoint_control in self.checkpoint_control.databases.values() {
                                    // Progress of normal backfill
                                    progress.extend(database_checkpoint_control.create_mview_tracker.gen_ddl_progress());
                                    // Progress of snapshot backfill
                                    for creating_job in database_checkpoint_control.creating_streaming_job_controls.values() {
                                        progress.extend([(creating_job.info.table_fragments.table_id().table_id, creating_job.gen_ddl_progress())]);
                                    }
                                }
                                if result_tx.send(progress).is_err() {
                                    error!("failed to send get ddl progress");
                                }
                            }
                        }
                    } else {
                        tracing::info!("end of request stream. meta node may be shutting down. Stop global barrier manager");
                        return;
                    }
                }

                changed_worker = self.active_streaming_nodes.changed() => {
                    #[cfg(debug_assertions)]
                    {
                        self.active_streaming_nodes.validate_change().await;
                    }

                    info!(?changed_worker, "worker changed");

                    if let ActiveStreamingWorkerChange::Add(node) | ActiveStreamingWorkerChange::Update(node) = changed_worker {
                        self.control_stream_manager.add_worker(node, self.checkpoint_control.databases.values().flat_map(|database| &database.state.inflight_subscription_info), &*self.context).await;
                    }
                }

                notification = local_notification_rx.recv() => {
                    let notification = notification.unwrap();
                    match notification {
                        // Handle barrier interval and checkpoint frequency changes.
                        LocalNotification::SystemParamsChange(p) => {
                            self.periodic_barriers.set_min_interval(Duration::from_millis(p.barrier_interval_ms() as u64));
                            self.periodic_barriers
                                .set_checkpoint_frequency(p.checkpoint_frequency() as usize)
                        },
                        // Handle adhoc recovery triggered by user.
                        LocalNotification::AdhocRecovery => {
                            self.adhoc_recovery().await;
                        }
                        _ => {}
                    }
                }
                complete_result = self
                    .completing_task
                    .next_completed_barrier(
                        &mut self.periodic_barriers,
                        &mut self.checkpoint_control,
                        &mut self.control_stream_manager,
                        &self.context,
                        &self.env,
                ) => {
                    match complete_result {
                        Ok(output) => {
                            self.checkpoint_control.ack_completed(output);
                        }
                        Err(e) => {
                            self.failure_recovery(e).await;
                        }
                    }
                },
                (worker_id, resp_result) = self.control_stream_manager.next_collect_barrier_response() => {
                    if let Err(e) = resp_result.and_then(|resp| self.checkpoint_control.barrier_collected(resp, &mut self.control_stream_manager)) {
                        {
                            let mut err = None;
                            for database_checkpoint_control in self.checkpoint_control.databases.values() {
                                let failed_barrier = database_checkpoint_control.barrier_wait_collect_from_worker(worker_id as _);
                                if failed_barrier.is_some()
                                    || database_checkpoint_control.state.inflight_graph_info.contains_worker(worker_id as _)
                                    || database_checkpoint_control.creating_streaming_job_controls.values().any(|job| job.is_wait_on_worker(worker_id)) {

                                    err = Some((e, failed_barrier));
                                    break;
                                }
                            }
                            if let Some((e, failed_barrier)) = err {
                                let errors = self.control_stream_manager.collect_errors(worker_id, e).await;
                                let err = merge_node_rpc_errors("get error from control stream", errors);
                                if let Some(failed_barrier) = failed_barrier {
                                    self.report_collect_failure(failed_barrier, &err);
                                }
                                self.failure_recovery(err).await;
                            }  else {
                                warn!(worker_id, "no barrier to collect from worker, ignore err");
                            }
                        }
                    }
                }
                new_barrier = self.periodic_barriers.next_barrier(&*self.context),
                    if self
                        .checkpoint_control
                        .can_inject_barrier(self.in_flight_barrier_nums) => {
                    if let Err(e) = self.checkpoint_control.handle_new_barrier(new_barrier, &mut self.control_stream_manager, &self.active_streaming_nodes) {
                        self.failure_recovery(e).await;
                    }
                }
            }
            self.checkpoint_control.update_barrier_nums_metrics();
        }
    }
}

impl<C: GlobalBarrierWorkerContext> GlobalBarrierWorker<C> {
    /// We need to make sure there are no changes when doing recovery
    pub async fn clear_on_err(&mut self, err: &MetaError) {
        // join spawned completing command to finish no matter it succeeds or not.
        let is_err = match replace(&mut self.completing_task, CompletingTask::None) {
            CompletingTask::None => false,
            CompletingTask::Completing {
                epochs_to_ack,
                join_handle,
                ..
            } => {
                info!("waiting for completing command to finish in recovery");
                match join_handle.await {
                    Err(e) => {
                        warn!(err = ?e.as_report(), "failed to join completing task");
                        true
                    }
                    Ok(Err(e)) => {
                        warn!(err = ?e.as_report(), "failed to complete barrier during clear");
                        true
                    }
                    Ok(Ok(hummock_version_stats)) => {
                        self.checkpoint_control
                            .ack_completed(BarrierCompleteOutput {
                                epochs_to_ack,
                                hummock_version_stats,
                            });
                        false
                    }
                }
            }
            CompletingTask::Err(_) => true,
        };
        if !is_err {
            // continue to finish the pending collected barrier.
            while let Some(task) = self.checkpoint_control.next_complete_barrier_task(None) {
                let epochs_to_ack = task.epochs_to_ack();
                match task
                    .complete_barrier(&*self.context, self.env.clone())
                    .await
                {
                    Ok(hummock_version_stats) => {
                        self.checkpoint_control
                            .ack_completed(BarrierCompleteOutput {
                                epochs_to_ack,
                                hummock_version_stats,
                            });
                    }
                    Err(e) => {
                        error!(
                            err = ?e.as_report(),
                            "failed to complete barrier during recovery"
                        );
                        break;
                    }
                }
            }
        }
        for (_, node) in self
            .checkpoint_control
            .databases
            .values_mut()
            .flat_map(|database| take(&mut database.command_ctx_queue))
        {
            for notifier in node.notifiers {
                notifier.notify_failed(err.clone());
            }
            node.enqueue_time.observe_duration();
        }
        self.checkpoint_control
            .databases
            .values_mut()
            .for_each(|database| database.create_mview_tracker.abort_all());
    }
}

impl<C: GlobalBarrierWorkerContext> GlobalBarrierWorker<C> {
    /// Set barrier manager status.
    async fn failure_recovery(&mut self, err: MetaError) {
        self.clear_on_err(&err).await;

        if self.enable_recovery {
            let span = tracing::info_span!(
                "failure_recovery",
                error = %err.as_report(),
            );

            crate::telemetry::report_event(
                risingwave_pb::telemetry::TelemetryEventStage::Recovery,
                "failure_recovery",
                0,
                None,
                None,
                None,
            );

            let reason = RecoveryReason::Failover(err.clone());

            // No need to clean dirty tables for barrier recovery,
            // The foreground stream job should cleanup their own tables.
            self.recovery(None, Some(err), reason)
                .instrument(span)
                .await;
        } else {
            panic!(
                "a streaming error occurred while recovery is disabled, aborting: {:?}",
                err.as_report()
            );
        }
    }

    async fn adhoc_recovery(&mut self) {
        let err = MetaErrorInner::AdhocRecovery.into();
        self.clear_on_err(&err).await;

        let span = tracing::info_span!(
            "adhoc_recovery",
            error = %err.as_report(),
        );

        crate::telemetry::report_event(
            risingwave_pb::telemetry::TelemetryEventStage::Recovery,
            "adhoc_recovery",
            0,
            None,
            None,
            None,
        );

        // No need to clean dirty tables for barrier recovery,
        // The foreground stream job should cleanup their own tables.
        self.recovery(None, Some(err), RecoveryReason::Adhoc)
            .instrument(span)
            .await;
    }
}

impl<C> GlobalBarrierWorker<C> {
    /// Send barrier-complete-rpc and wait for responses from all CNs
    pub(super) fn report_collect_failure(&self, barrier_info: &BarrierInfo, error: &MetaError) {
        // Record failure in event log.
        use risingwave_pb::meta::event_log;
        let event = event_log::EventCollectBarrierFail {
            prev_epoch: barrier_info.prev_epoch(),
            cur_epoch: barrier_info.curr_epoch.value().0,
            error: error.to_report_string(),
        };
        self.env
            .event_log_manager_ref()
            .add_event_logs(vec![event_log::Event::CollectBarrierFail(event)]);
    }
}

impl<C> GlobalBarrierWorker<C> {
    // Retry base interval in milliseconds.
    const RECOVERY_RETRY_BASE_INTERVAL: u64 = 20;
    // Retry max interval.
    const RECOVERY_RETRY_MAX_INTERVAL: Duration = Duration::from_secs(5);

    #[inline(always)]
    /// Initialize a retry strategy for operation in recovery.
    fn get_retry_strategy() -> impl Iterator<Item = Duration> {
        ExponentialBackoff::from_millis(Self::RECOVERY_RETRY_BASE_INTERVAL)
            .max_delay(Self::RECOVERY_RETRY_MAX_INTERVAL)
            .map(jitter)
    }
}

impl<C: GlobalBarrierWorkerContext> GlobalBarrierWorker<C> {
    /// Recovery the whole cluster from the latest epoch.
    ///
    /// If `paused_reason` is `Some`, all data sources (including connectors and DMLs) will be
    /// immediately paused after recovery, until the user manually resume them either by restarting
    /// the cluster or `risectl` command. Used for debugging purpose.
    ///
    /// Returns the new state of the barrier manager after recovery.
    pub async fn recovery(
        &mut self,
        paused_reason: Option<PausedReason>,
        err: Option<MetaError>,
        recovery_reason: RecoveryReason,
    ) {
        self.context.abort_and_mark_blocked(recovery_reason);
        // Clear all control streams to release resources (connections to compute nodes) first.
        self.control_stream_manager.clear();

        self.recovery_inner(paused_reason, err).await;
        self.context.mark_ready();
    }

    async fn recovery_inner(
        &mut self,
        paused_reason: Option<PausedReason>,
        err: Option<MetaError>,
    ) {
        tracing::info!("recovery start!");
        let retry_strategy = Self::get_retry_strategy();

        // We take retry into consideration because this is the latency user sees for a cluster to
        // get recovered.
        let recovery_timer = GLOBAL_META_METRICS.recovery_latency.start_timer();

        let new_state = tokio_retry::Retry::spawn(retry_strategy, || async {
            if let Some(err) = &err {
                self.context.notify_creating_job_failed(err).await;
            };
            let runtime_info_snapshot = self
                .context
                .reload_runtime_info()
                .await?;
            runtime_info_snapshot.validate().inspect_err(|e| {
                warn!(err = ?e.as_report(), ?runtime_info_snapshot, "reloaded runtime info failed to validate");
            })?;
            let BarrierWorkerRuntimeInfoSnapshot {
                active_streaming_nodes,
                database_fragment_infos,
                mut state_table_committed_epochs,
                mut subscription_infos,
                mut stream_actors,
                mut source_splits,
                mut background_jobs,
                hummock_version_stats,
            } = runtime_info_snapshot;

            self.sink_manager.reset().await;

            let mut control_stream_manager = ControlStreamManager::new(self.env.clone());
            let reset_start_time = Instant::now();
            control_stream_manager
                .reset(
                    subscription_infos.values(),
                    active_streaming_nodes.current(),
                    &*self.context,
                )
                .await
                .inspect_err(|err| {
                    warn!(error = %err.as_report(), "reset compute nodes failed");
                })?;
            info!(elapsed=?reset_start_time.elapsed(), "control stream reset");

            let mut databases = HashMap::new();

            let recovery_result: MetaResult<_> = try {
                for (database_id, info) in database_fragment_infos {
                    let source_split_assignments = info
                        .fragment_infos()
                        .flat_map(|info| info.actors.keys())
                        .filter_map(|actor_id| {
                            let actor_id = *actor_id as ActorId;
                            source_splits
                                .remove(&actor_id)
                                .map(|splits| (actor_id, splits))
                        })
                        .collect();
                    let mutation = Mutation::Add(AddMutation {
                        // Actors built during recovery is not treated as newly added actors.
                        actor_dispatchers: Default::default(),
                        added_actors: Default::default(),
                        actor_splits: build_actor_connector_splits(&source_split_assignments),
                        pause: paused_reason.is_some(),
                        subscriptions_to_add: Default::default(),
                    });

                    let new_epoch = {
                        let mut epochs = info.existing_table_ids().map(|table_id| {
                            (
                                table_id,
                                state_table_committed_epochs
                                    .remove(&table_id)
                                    .expect("should exist"),
                            )
                        });
                        let (first_table_id, prev_epoch) = epochs.next().expect("non-empty");
                        for (table_id, epoch) in epochs {
                            assert_eq!(
                                prev_epoch, epoch,
                                "{} has different committed epoch to {}",
                                first_table_id, table_id
                            );
                        }
                        let prev_epoch = TracedEpoch::new(Epoch(prev_epoch));
                        // Use a different `curr_epoch` for each recovery attempt.
                        let curr_epoch = prev_epoch.next();
                        let barrier_info = BarrierInfo {
                            prev_epoch,
                            curr_epoch,
                            kind: BarrierKind::Initial,
                        };

                        let mut node_actors: HashMap<_, Vec<_>> = HashMap::new();
                        for (actor_id, worker_id) in
                            info.fragment_infos().flat_map(|info| info.actors.iter())
                        {
                            let worker_id = *worker_id as WorkerId;
                            let actor_id = *actor_id as ActorId;
                            let stream_actor =
                                stream_actors.remove(&actor_id).expect("should exist");
                            node_actors.entry(worker_id).or_default().push(stream_actor);
                        }

                        let mut node_to_collect = control_stream_manager.inject_barrier(
                            database_id,
                            None,
                            Some(mutation),
                            &barrier_info,
                            info.fragment_infos(),
                            info.fragment_infos(),
                            Some(node_actors),
                            vec![],
                            vec![],
                        )?;
                        debug!(?node_to_collect, "inject initial barrier");
                        while !node_to_collect.is_empty() {
                            let (worker_id, result) =
                                control_stream_manager.next_collect_barrier_response().await;
                            let resp = result?;
                            assert_eq!(resp.epoch, barrier_info.prev_epoch());
                            assert!(node_to_collect.remove(&worker_id));
                        }
                        debug!("collected initial barrier");
                        barrier_info.curr_epoch
                    };

                    let background_mviews = info
                        .job_ids()
                        .filter_map(|job_id| {
                            background_jobs.remove(&job_id).map(|mview| (job_id, mview))
                        })
                        .collect();
                    let tracker = CreateMviewProgressTracker::recover(
                        background_mviews,
                        &hummock_version_stats,
                    );
                    let state = BarrierWorkerState::recovery(
                        new_epoch,
                        info,
                        subscription_infos.remove(&database_id).unwrap_or_default(),
                        paused_reason,
                    );
                    databases.insert(
                        database_id,
                        DatabaseCheckpointControl::recovery(database_id, tracker, state),
                    );
                }
                if !stream_actors.is_empty() {
                    warn!(actor_ids = ?stream_actors.keys().collect_vec(), "unused stream actors in recovery");
                }
                if !source_splits.is_empty() {
                    warn!(actor_ids = ?source_splits.keys().collect_vec(), "unused actor source splits in recovery");
                }
                if !background_jobs.is_empty() {
                    warn!(job_ids = ?background_jobs.keys().collect_vec(), "unused recovered background mview in recovery");
                }
                if !subscription_infos.is_empty() {
                    warn!(?subscription_infos, "unused subscription infos in recovery");
                }
                if !state_table_committed_epochs.is_empty() {
                    warn!(?state_table_committed_epochs, "unused state table committed epoch in recovery");
                }
                (
                    active_streaming_nodes,
                    control_stream_manager,
                    CheckpointControl {
                        databases,
                        hummock_version_stats,
                    },
                )
            };
            if recovery_result.is_err() {
                GLOBAL_META_METRICS.recovery_failure_cnt.inc();
            }
            recovery_result
        })
            .instrument(tracing::info_span!("recovery_attempt"))
            .await
            .expect("Retry until recovery success.");

        recovery_timer.observe_duration();

        (
            self.active_streaming_nodes,
            self.control_stream_manager,
            self.checkpoint_control,
        ) = new_state;

        tracing::info!("recovery success");

        self.env
            .notification_manager()
            .notify_frontend_without_version(Operation::Update, Info::Recovery(Recovery {}));
    }
}
