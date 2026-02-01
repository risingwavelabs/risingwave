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

use std::collections::hash_map::Entry;
use std::collections::{HashMap, HashSet};
use std::mem::replace;
use std::pin::pin;
use std::sync::Arc;
use std::time::Duration;

use anyhow::anyhow;
use arc_swap::ArcSwap;
use futures::{TryFutureExt, pin_mut};
use itertools::Itertools;
use risingwave_common::catalog::DatabaseId;
use risingwave_common::system_param::reader::SystemParamsRead;
use risingwave_common::system_param::{AdaptiveParallelismStrategy, PAUSE_ON_NEXT_BOOTSTRAP_KEY};
use risingwave_meta_model::WorkerId;
use risingwave_pb::common::WorkerNode;
use risingwave_pb::meta::Recovery;
use risingwave_pb::meta::subscribe_response::{Info, Operation};
use risingwave_pb::stream_service::streaming_control_stream_response::Response;
use sea_orm::TransactionTrait;
use thiserror_ext::AsReport;
use tokio::select;
use tokio::sync::mpsc;
use tokio::sync::oneshot::{Receiver, Sender};
use tokio::task::JoinHandle;
use tonic::Status;
use tracing::{Instrument, debug, error, info, warn};
use uuid::Uuid;

use crate::barrier::checkpoint::{CheckpointControl, CheckpointControlEvent};
use crate::barrier::complete_task::{BarrierCompleteOutput, CompletingTask};
use crate::barrier::context::recovery::{RenderedDatabaseRuntimeInfo, render_runtime_info};
use crate::barrier::context::{GlobalBarrierWorkerContext, GlobalBarrierWorkerContextImpl};
use crate::barrier::rpc::{
    ControlStreamManager, WorkerNodeEvent, from_partial_graph_id, merge_node_rpc_errors,
};
use crate::barrier::schedule::{MarkReadyOptions, PeriodicBarriers};
use crate::barrier::{
    BarrierManagerRequest, BarrierManagerStatus, BarrierWorkerRuntimeInfoSnapshot, Command,
    RecoveryReason, RescheduleIntent, UpdateDatabaseBarrierRequest, schedule,
};
use crate::controller::scale::{
    find_fragment_no_shuffle_dags_detailed, render_fragments, render_jobs,
};
use crate::error::MetaErrorInner;
use crate::hummock::HummockManagerRef;
use crate::manager::sink_coordination::SinkCoordinatorManager;
use crate::manager::{
    ActiveStreamingWorkerChange, ActiveStreamingWorkerNodes, LocalNotification, MetaSrvEnv,
    MetadataManager,
};
use crate::rpc::metrics::GLOBAL_META_METRICS;
use crate::stream::{
    GlobalRefreshManagerRef, ScaleControllerRef, SourceManagerRef, build_reschedule_commands,
};
use crate::{MetaError, MetaResult};

/// [`crate::barrier::worker::GlobalBarrierWorker`] sends barriers to all registered compute nodes and
/// collect them, with monotonic increasing epoch numbers. On compute nodes, `LocalBarrierManager`
/// in `risingwave_stream` crate will serve these requests and dispatch them to source actors.
///
/// Configuration change in our system is achieved by the mutation in the barrier. Thus,
/// [`crate::barrier::worker::GlobalBarrierWorker`] provides a set of interfaces like a state machine,
/// accepting [`crate::barrier::command::Command`] that carries info to build `Mutation`. To keep the consistency between
/// barrier manager and meta store, some actions like "drop materialized view" or "create mv on mv"
/// must be done in barrier manager transactional using [`crate::barrier::command::Command`].
pub(super) struct GlobalBarrierWorker<C> {
    /// Enable recovery or not when failover.
    enable_recovery: bool,

    /// The queue of scheduled barriers.
    periodic_barriers: PeriodicBarriers,

    /// Whether per database failure isolation is enabled in system parameters.
    system_enable_per_database_isolation: bool,

    adaptive_parallelism_strategy: AdaptiveParallelismStrategy,

    pub(super) context: Arc<C>,

    env: MetaSrvEnv,

    checkpoint_control: CheckpointControl,

    /// Command that has been collected but is still completing.
    /// The join handle of the completing future is stored.
    completing_task: CompletingTask,

    request_rx: mpsc::UnboundedReceiver<BarrierManagerRequest>,

    active_streaming_nodes: ActiveStreamingWorkerNodes,

    control_stream_manager: ControlStreamManager,

    term_id: String,
}

#[cfg(test)]
mod tests {
    use std::collections::{HashMap, HashSet};

    use risingwave_common::id::JobId;
    use tokio::sync::oneshot;

    use super::*;
    use crate::barrier::notifier::Notifier;

    #[tokio::test]
    async fn test_reschedule_intent_without_workers_notifies_start_failed() {
        let env = MetaSrvEnv::for_test().await;
        let (started_tx, started_rx) = oneshot::channel();
        let (_collected_tx, _collected_rx) = oneshot::channel();

        let notifier = Notifier {
            started: Some(started_tx),
            collected: Some(_collected_tx),
        };

        let new_barrier = schedule::NewBarrier {
            database_id: DatabaseId::new(1),
            command: Some((
                Command::RescheduleIntent {
                    intent: RescheduleIntent::Jobs(HashSet::from([JobId::new(1)])),
                },
                vec![notifier],
            )),
            span: tracing::Span::none(),
            checkpoint: false,
        };

        let result = resolve_reschedule_intent(
            env,
            HashMap::new(),
            AdaptiveParallelismStrategy::default(),
            new_barrier,
        )
        .await;

        assert!(matches!(result, Ok(None)));
        let started = started_rx.await.expect("started notifier dropped");
        assert!(started.is_err());
    }
}

impl<C: GlobalBarrierWorkerContext> GlobalBarrierWorker<C> {
    pub(super) async fn new_inner(
        env: MetaSrvEnv,
        request_rx: mpsc::UnboundedReceiver<BarrierManagerRequest>,
        context: Arc<C>,
    ) -> Self {
        let enable_recovery = env.opts.enable_recovery;

        let active_streaming_nodes = ActiveStreamingWorkerNodes::uninitialized();

        let control_stream_manager = ControlStreamManager::new(env.clone());

        let reader = env.system_params_reader().await;
        let system_enable_per_database_isolation = reader.per_database_isolation();
        // Load config will be performed in bootstrap phase.
        let periodic_barriers = PeriodicBarriers::default();
        let adaptive_parallelism_strategy = reader.adaptive_parallelism_strategy();

        let checkpoint_control = CheckpointControl::new(env.clone());
        Self {
            enable_recovery,
            periodic_barriers,
            system_enable_per_database_isolation,
            adaptive_parallelism_strategy,
            context,
            env,
            checkpoint_control,
            completing_task: CompletingTask::None,
            request_rx,
            active_streaming_nodes,
            control_stream_manager,
            term_id: "uninitialized".into(),
        }
    }
}

async fn resolve_reschedule_intent(
    env: MetaSrvEnv,
    worker_nodes: HashMap<WorkerId, WorkerNode>,
    adaptive_parallelism_strategy: AdaptiveParallelismStrategy,
    mut new_barrier: schedule::NewBarrier,
) -> MetaResult<Option<schedule::NewBarrier>> {
    let Some((command, notifiers)) = new_barrier.command.take() else {
        return Ok(Some(new_barrier));
    };

    match command {
        Command::RescheduleIntent { intent } => {
            let span = tracing::info_span!(
                "resolve_reschedule_intent",
                database_id = %new_barrier.database_id
            );
            let resolved = build_reschedule_from_intent(
                &env,
                worker_nodes,
                adaptive_parallelism_strategy,
                new_barrier.database_id,
                intent,
            )
            .instrument(span)
            .await;
            match resolved {
                Ok(Some(command)) => {
                    new_barrier.command = Some((command, notifiers));
                    Ok(Some(new_barrier))
                }
                Ok(None) => {
                    for mut notifier in notifiers {
                        notifier.notify_started();
                        notifier.notify_collected();
                    }
                    Ok(None)
                }
                Err(err) => {
                    for notifier in notifiers {
                        notifier.notify_start_failed(err.clone());
                    }
                    Ok(None)
                }
            }
        }
        _ => {
            new_barrier.command = Some((command, notifiers));
            Ok(Some(new_barrier))
        }
    }
}

async fn build_reschedule_from_intent(
    env: &MetaSrvEnv,
    worker_nodes: HashMap<WorkerId, WorkerNode>,
    adaptive_parallelism_strategy: AdaptiveParallelismStrategy,
    database_id: DatabaseId,
    intent: RescheduleIntent,
) -> MetaResult<Option<Command>> {
    if worker_nodes.is_empty() {
        return Err(anyhow!("no active streaming workers for reschedule").into());
    }

    let txn = env.meta_store().conn.begin().await?;
    let actor_id_counter = env.actor_id_generator();

    let rendered = match intent {
        RescheduleIntent::Jobs(job_ids) => {
            render_jobs(
                &txn,
                actor_id_counter,
                job_ids,
                &worker_nodes,
                adaptive_parallelism_strategy,
            )
            .await?
        }
        RescheduleIntent::Fragments(fragment_ids) => {
            let fragment_ids = fragment_ids.into_iter().collect_vec();
            if fragment_ids.is_empty() {
                return Ok(None);
            }
            let ensembles = find_fragment_no_shuffle_dags_detailed(&txn, &fragment_ids).await?;
            render_fragments(
                &txn,
                actor_id_counter,
                ensembles,
                &worker_nodes,
                adaptive_parallelism_strategy,
            )
            .await?
        }
    };

    let mut commands = build_reschedule_commands(env, &txn, rendered.fragments).await?;
    Ok(commands.remove(&database_id))
}

impl GlobalBarrierWorker<GlobalBarrierWorkerContextImpl> {
    /// Create a new [`crate::barrier::worker::GlobalBarrierWorker`].
    pub async fn new(
        scheduled_barriers: schedule::ScheduledBarriers,
        env: MetaSrvEnv,
        metadata_manager: MetadataManager,
        hummock_manager: HummockManagerRef,
        source_manager: SourceManagerRef,
        sink_manager: SinkCoordinatorManager,
        scale_controller: ScaleControllerRef,
        request_rx: mpsc::UnboundedReceiver<BarrierManagerRequest>,
        barrier_scheduler: schedule::BarrierScheduler,
        refresh_manager: GlobalRefreshManagerRef,
    ) -> Self {
        let status = Arc::new(ArcSwap::new(Arc::new(BarrierManagerStatus::Starting)));

        let context = Arc::new(GlobalBarrierWorkerContextImpl::new(
            scheduled_barriers,
            status,
            metadata_manager,
            hummock_manager,
            source_manager,
            scale_controller,
            env.clone(),
            barrier_scheduler,
            refresh_manager,
            sink_manager,
        ));

        Self::new_inner(env, request_rx, context).await
    }

    pub fn start(self) -> (JoinHandle<()>, Sender<()>) {
        let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel();
        let fut = (self.env.await_tree_reg())
            .register_derived_root("Global Barrier Worker")
            .instrument(self.run(shutdown_rx));
        let join_handle = tokio::spawn(fut);

        (join_handle, shutdown_tx)
    }

    /// Check whether we should pause on bootstrap from the system parameter and reset it.
    async fn take_pause_on_bootstrap(&mut self) -> MetaResult<bool> {
        let paused = self
            .env
            .system_params_reader()
            .await
            .pause_on_next_bootstrap()
            || self.env.opts.pause_on_next_bootstrap_offline;

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
            self.checkpoint_control.in_flight_barrier_nums,
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

            self.recovery(paused, RecoveryReason::Bootstrap)
                .instrument(span)
                .await;
        }

        self.run_inner(shutdown_rx).await
    }
}

impl<C: GlobalBarrierWorkerContext> GlobalBarrierWorker<C> {
    fn enable_per_database_isolation(&self) -> bool {
        self.system_enable_per_database_isolation && {
            if let Err(e) =
                risingwave_common::license::Feature::DatabaseFailureIsolation.check_available()
            {
                warn!(error = %e.as_report(), "DatabaseFailureIsolation disabled by license");
                false
            } else {
                true
            }
        }
    }

    pub(super) async fn run_inner(mut self, mut shutdown_rx: Receiver<()>) {
        let (local_notification_tx, mut local_notification_rx) =
            tokio::sync::mpsc::unbounded_channel();
        self.env
            .notification_manager()
            .insert_local_sender(local_notification_tx);

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
                            BarrierManagerRequest::GetBackfillProgress(result_tx) => {
                                let progress = self.checkpoint_control.gen_backfill_progress();
                                if result_tx.send(Ok(progress)).is_err() {
                                    error!("failed to send get ddl progress");
                                }
                            }
                            BarrierManagerRequest::GetFragmentBackfillProgress(result_tx) => {
                                let progress =
                                    self.checkpoint_control.gen_fragment_backfill_progress();
                                if result_tx.send(Ok(progress)).is_err() {
                                    error!("failed to send get fragment backfill progress");
                                }
                            }
                            BarrierManagerRequest::GetCdcProgress(result_tx) => {
                                let progress = self.checkpoint_control.gen_cdc_progress();
                                if result_tx.send(Ok(progress)).is_err() {
                                    error!("failed to send get ddl progress");
                                }
                            }
                            // Handle adhoc recovery triggered by user.
                            BarrierManagerRequest::AdhocRecovery(sender) => {
                                self.adhoc_recovery().await;
                                if sender.send(()).is_err() {
                                    warn!("failed to notify finish of adhoc recovery");
                                }
                            }
                            BarrierManagerRequest::UpdateDatabaseBarrier( UpdateDatabaseBarrierRequest {
                                database_id,
                                barrier_interval_ms,
                                checkpoint_frequency,
                                sender,
                            }) => {
                                self.periodic_barriers
                                    .update_database_barrier(
                                        database_id,
                                        barrier_interval_ms,
                                        checkpoint_frequency,
                                    );
                                if sender.send(()).is_err() {
                                    warn!("failed to notify finish of update database barrier");
                                }
                            }
                            BarrierManagerRequest::MayHaveSnapshotBackfillingJob(tx) => {
                                if tx.send(self.checkpoint_control.may_have_snapshot_backfilling_jobs()).is_err() {
                                    warn!("failed to may have snapshot backfill job");
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

                    match changed_worker {
                        ActiveStreamingWorkerChange::Add(node) | ActiveStreamingWorkerChange::Update(node) => {
                            self.control_stream_manager.add_worker(node, self.checkpoint_control.inflight_infos(), self.term_id.clone(), &*self.context).await;
                        }
                        ActiveStreamingWorkerChange::Remove(node) => {
                            self.control_stream_manager.remove_worker(node);
                        }
                    }
                }

                notification = local_notification_rx.recv() => {
                    let notification = notification.unwrap();
                    if let LocalNotification::SystemParamsChange(p) = notification {
                        {
                            self.periodic_barriers.set_sys_barrier_interval(Duration::from_millis(p.barrier_interval_ms() as u64));
                            self.periodic_barriers
                                .set_sys_checkpoint_frequency(p.checkpoint_frequency());
                            self.system_enable_per_database_isolation = p.per_database_isolation();
                            self.adaptive_parallelism_strategy = p.adaptive_parallelism_strategy();
                        }
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
                event = self.checkpoint_control.next_event() => {
                    let result: MetaResult<()> = try {
                        match event {
                            CheckpointControlEvent::EnteringInitializing(entering_initializing) => {
                                let database_id = entering_initializing.database_id();
                                let error = merge_node_rpc_errors(&format!("database {} reset", database_id), entering_initializing.action.0.iter().filter_map(|(worker_id, resp)| {
                                    resp.root_err.as_ref().map(|root_err| {
                                        (*worker_id, ScoredError {
                                            error: Status::internal(&root_err.err_msg),
                                            score: Score(root_err.score)
                                        })
                                    })
                                }));
                                Self::report_collect_failure(&self.env, &error);
                                self.context.notify_creating_job_failed(Some(database_id), format!("{}", error.as_report())).await;
                                let result: MetaResult<_> = try {
                                    let runtime_info = self.context.reload_database_runtime_info(database_id).await.inspect_err(|err| {
                                        warn!(%database_id, err = %err.as_report(), "reload runtime info failed");
                                    })?;
                                    let rendered_info = render_runtime_info(
                                        self.env.actor_id_generator(),
                                        &self.active_streaming_nodes,
                                        self.adaptive_parallelism_strategy,
                                        &runtime_info.recovery_context,
                                        database_id,
                                    )
                                    .inspect_err(|err: &MetaError| {
                                        warn!(%database_id, err = %err.as_report(), "render runtime info failed");
                                    })?;
                                    if let Some(rendered_info) = rendered_info {
                                        BarrierWorkerRuntimeInfoSnapshot::validate_database_info(
                                            database_id,
                                            &rendered_info.job_infos,
                                            &self.active_streaming_nodes,
                                            &rendered_info.stream_actors,
                                            &runtime_info.state_table_committed_epochs,
                                        )
                                        .inspect_err(|err| {
                                            warn!(%database_id, err = ?err.as_report(), "database runtime info failed validation");
                                        })?;
                                        Some((runtime_info, rendered_info))
                                    } else {
                                        None
                                    }
                                };
                                match result {
                                    Ok(Some((runtime_info, rendered_info))) => {
                                        entering_initializing.enter(
                                            runtime_info,
                                            rendered_info,
                                            &mut self.control_stream_manager,
                                        );
                                    }
                                    Ok(None) => {
                                        info!(%database_id, "database removed after reloading empty runtime info");
                                        // mark ready to unblock subsequent request
                                        self.context.mark_ready(MarkReadyOptions::Database(database_id));
                                        entering_initializing.remove();
                                    }
                                    Err(e) => {
                                        entering_initializing.fail_reload_runtime_info(e);
                                    }
                                }
                            }
                            CheckpointControlEvent::EnteringRunning(entering_running) => {
                                self.context.mark_ready(MarkReadyOptions::Database(entering_running.database_id()));
                                entering_running.enter();
                            }
                        }
                    };
                    if let Err(e) = result {
                        self.failure_recovery(e).await;
                    }
                }
                (worker_id, event) = self.control_stream_manager.next_event(&self.term_id, &self.context) => {
                    let resp_result = match event {
                        WorkerNodeEvent::Response(result) => {
                            result
                        }
                        WorkerNodeEvent::Connected(connected) => {
                            connected.initialize(self.checkpoint_control.inflight_infos());
                            continue;
                        }
                    };
                    let result: MetaResult<()> = try {
                        let resp = match resp_result {
                            Err(err) => {
                                let failed_databases = self.checkpoint_control.databases_failed_at_worker_err(worker_id);
                                if !failed_databases.is_empty() {
                                    if !self.enable_recovery {
                                        panic!("control stream to worker {} failed but recovery not enabled: {:?}", worker_id, err.as_report());
                                    }
                                    if !self.enable_per_database_isolation() {
                                        Err(err.clone())?;
                                    }
                                    Self::report_collect_failure(&self.env, &err);
                                    for database_id in failed_databases {
                                        if let Some(entering_recovery) = self.checkpoint_control.on_report_failure(database_id, &mut self.control_stream_manager) {
                                            warn!(%worker_id, %database_id, "database entering recovery on node failure");
                                            self.context.abort_and_mark_blocked(Some(database_id), RecoveryReason::Failover(anyhow!("reset database: {}", database_id).into()));
                                            self.context.notify_creating_job_failed(Some(database_id), format!("database {} reset due to node {} failure: {}", database_id, worker_id, err.as_report())).await;
                                            // TODO: add log on blocking time
                                            let output = self.completing_task.wait_completing_task().await?;
                                            entering_recovery.enter(output, &mut self.control_stream_manager);
                                        }
                                    }
                                }  else {
                                    warn!(%worker_id, "no barrier to collect from worker, ignore err");
                                }
                                continue;
                            }
                            Ok(resp) => resp,
                        };
                        match resp {
                            Response::CompleteBarrier(resp) => {
                                self.checkpoint_control.barrier_collected(resp, &mut self.periodic_barriers)?;
                            },
                            Response::ReportPartialGraphFailure(resp) => {
                                if !self.enable_recovery {
                                    panic!("database failure reported but recovery not enabled: {:?}", resp)
                                }
                                let (database_id, _) = from_partial_graph_id(resp.partial_graph_id);
                                if !self.enable_per_database_isolation() {
                                        Err(anyhow!("database {} reset", database_id))?;
                                    }
                                if let Some(entering_recovery) = self.checkpoint_control.on_report_failure(database_id, &mut self.control_stream_manager) {
                                    warn!(%database_id, "database entering recovery");
                                    self.context.abort_and_mark_blocked(Some(database_id), RecoveryReason::Failover(anyhow!("reset database: {}", database_id).into()));
                                    // TODO: add log on blocking time
                                    let output = self.completing_task.wait_completing_task().await?;
                                    entering_recovery.enter(output, &mut self.control_stream_manager);
                                }
                            }
                            Response::ResetPartialGraph(resp) => {
                                self.checkpoint_control.on_reset_partial_graph_resp(worker_id, resp);
                            }
                            other @ Response::Init(_) | other @ Response::Shutdown(_) => {
                                Err(anyhow!("get expected response: {:?}", other))?;
                            }
                        }
                    };
                    if let Err(e) = result {
                        self.failure_recovery(e).await;
                    }
                }
                new_barrier = self.periodic_barriers.next_barrier(&*self.context) => {
                    let database_id = new_barrier.database_id;
                    let new_barrier = if matches!(
                        new_barrier.command,
                        Some((Command::RescheduleIntent { .. }, _))
                    ) {
                        let env = self.env.clone();
                        let worker_nodes = self
                            .active_streaming_nodes
                            .current()
                            .iter()
                            .map(|(worker_id, worker)| (*worker_id, worker.clone()))
                            .collect();
                        let adaptive_parallelism_strategy = self.adaptive_parallelism_strategy;
                        match resolve_reschedule_intent(
                            env,
                            worker_nodes,
                            adaptive_parallelism_strategy,
                            new_barrier,
                        )
                        .await
                        {
                            Ok(Some(new_barrier)) => new_barrier,
                            Ok(None) => continue,
                            Err(err) => {
                                self.failure_recovery(err).await;
                                continue;
                            }
                        }
                    } else {
                        new_barrier
                    };
                    if let Err(e) = self.checkpoint_control.handle_new_barrier(new_barrier, &mut self.control_stream_manager) {
                        if !self.enable_recovery {
                            panic!(
                                "failed to inject barrier to some databases but recovery not enabled: {:?}", (
                                    database_id,
                                    e.as_report()
                                )
                            );
                        }
                        let result: MetaResult<_> = try {
                            if !self.enable_per_database_isolation() {
                                let err = anyhow!("failed to inject barrier to databases: {:?}", (database_id, e.as_report()));
                                Err(err)?;
                            } else if let Some(entering_recovery) = self.checkpoint_control.on_report_failure(database_id, &mut self.control_stream_manager) {
                                warn!(%database_id, e = %e.as_report(),"database entering recovery on inject failure");
                                self.context.abort_and_mark_blocked(Some(database_id), RecoveryReason::Failover(anyhow!(e).context("inject barrier failure").into()));
                                // TODO: add log on blocking time
                                let output = self.completing_task.wait_completing_task().await?;
                                entering_recovery.enter(output, &mut self.control_stream_manager);
                            }
                        };
                        if let Err(e) = result {
                            self.failure_recovery(e).await;
                        }
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
                        warn!(err = %e.as_report(), "failed to join completing task");
                        true
                    }
                    Ok(Err(e)) => {
                        warn!(
                            err = %e.as_report(),
                            "failed to complete barrier during clear"
                        );
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
                            err = %e.as_report(),
                            "failed to complete barrier during recovery"
                        );
                        break;
                    }
                }
            }
        }
        self.checkpoint_control.clear_on_err(err);
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

            let reason = RecoveryReason::Failover(err);

            // No need to clean dirty tables for barrier recovery,
            // The foreground stream job should cleanup their own tables.
            self.recovery(false, reason).instrument(span).await;
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
        self.recovery(false, RecoveryReason::Adhoc)
            .instrument(span)
            .await;
    }
}

impl<C> GlobalBarrierWorker<C> {
    /// Send barrier-complete-rpc and wait for responses from all CNs
    pub(super) fn report_collect_failure(env: &MetaSrvEnv, error: &MetaError) {
        // Record failure in event log.
        use risingwave_pb::meta::event_log;
        let event = event_log::EventCollectBarrierFail {
            error: error.to_report_string(),
        };
        env.event_log_manager_ref()
            .add_event_logs(vec![event_log::Event::CollectBarrierFail(event)]);
    }
}

mod retry_strategy {
    use std::time::Duration;

    use tokio_retry::strategy::{ExponentialBackoff, jitter};

    // Retry base interval in milliseconds.
    const RECOVERY_RETRY_BASE_INTERVAL: u64 = 20;
    // Retry max interval.
    const RECOVERY_RETRY_MAX_INTERVAL: Duration = Duration::from_secs(5);

    // MrCroxx: Use concrete type here to prevent unsolved compiler issue.
    // Feel free to replace the concrete type with TAIT after fixed.

    // mod retry_backoff_future {
    //     use std::future::Future;
    //     use std::time::Duration;
    //
    //     use tokio::time::sleep;
    //
    //     pub(crate) type RetryBackoffFuture = impl Future<Output = ()> + Unpin + Send + 'static;
    //
    //     #[define_opaque(RetryBackoffFuture)]
    //     pub(super) fn get_retry_backoff_future(duration: Duration) -> RetryBackoffFuture {
    //         Box::pin(sleep(duration))
    //     }
    // }
    // pub(crate) use retry_backoff_future::*;

    pub(crate) type RetryBackoffFuture = std::pin::Pin<Box<tokio::time::Sleep>>;

    pub(crate) fn get_retry_backoff_future(duration: Duration) -> RetryBackoffFuture {
        Box::pin(tokio::time::sleep(duration))
    }

    pub(crate) type RetryBackoffStrategy =
        impl Iterator<Item = RetryBackoffFuture> + Send + 'static;

    /// Initialize a retry strategy for operation in recovery.
    #[inline(always)]
    pub(crate) fn get_retry_strategy() -> impl Iterator<Item = Duration> + Send + 'static {
        ExponentialBackoff::from_millis(RECOVERY_RETRY_BASE_INTERVAL)
            .max_delay(RECOVERY_RETRY_MAX_INTERVAL)
            .map(jitter)
    }

    #[define_opaque(RetryBackoffStrategy)]
    pub(crate) fn get_retry_backoff_strategy() -> RetryBackoffStrategy {
        get_retry_strategy().map(get_retry_backoff_future)
    }
}

pub(crate) use retry_strategy::*;
use risingwave_common::error::tonic::extra::{Score, ScoredError};
use risingwave_pb::meta::event_log::{Event, EventRecovery};

impl<C: GlobalBarrierWorkerContext> GlobalBarrierWorker<C> {
    /// Recovery the whole cluster from the latest epoch.
    ///
    /// If `paused_reason` is `Some`, all data sources (including connectors and DMLs) will be
    /// immediately paused after recovery, until the user manually resume them either by restarting
    /// the cluster or `risectl` command. Used for debugging purpose.
    ///
    /// Returns the new state of the barrier manager after recovery.
    pub async fn recovery(&mut self, is_paused: bool, recovery_reason: RecoveryReason) {
        // Clear all control streams to release resources (connections to compute nodes) first.
        self.control_stream_manager.clear();

        let reason_str = match &recovery_reason {
            RecoveryReason::Bootstrap => "bootstrap".to_owned(),
            RecoveryReason::Failover(err) => {
                format!("failed over: {}", err.as_report())
            }
            RecoveryReason::Adhoc => "adhoc recovery".to_owned(),
        };
        self.context.abort_and_mark_blocked(None, recovery_reason);

        self.recovery_inner(is_paused, reason_str).await;
        self.context.mark_ready(MarkReadyOptions::Global {
            blocked_databases: self.checkpoint_control.recovering_databases().collect(),
        });
    }

    #[await_tree::instrument("recovery({recovery_reason})")]
    async fn recovery_inner(&mut self, is_paused: bool, recovery_reason: String) {
        let event_log_manager_ref = self.env.event_log_manager_ref();

        tracing::info!("recovery start!");
        event_log_manager_ref.add_event_logs(vec![Event::Recovery(
            EventRecovery::global_recovery_start(recovery_reason.clone()),
        )]);

        let retry_strategy = get_retry_strategy();

        // We take retry into consideration because this is the latency user sees for a cluster to
        // get recovered.
        let recovery_timer = GLOBAL_META_METRICS
            .recovery_latency
            .with_label_values(&["global"])
            .start_timer();

        let enable_per_database_isolation = self.enable_per_database_isolation();

        let recovery_future = tokio_retry::Retry::spawn(retry_strategy, || async {
            self.env.stream_client_pool().invalidate_all();
            // We need to notify_creating_job_failed in every recovery retry, because in outer create_streaming_job handler,
            // it holds the reschedule_read_lock and wait for creating job to finish, and caused the following scale_actor fail
            // to acquire the reschedule_write_lock, and then keep recovering, and then deadlock.
            // TODO: refactor and fix this hacky implementation.
            self.context
                .notify_creating_job_failed(None, recovery_reason.clone())
                .await;

            let runtime_info_snapshot = self
                .context
                .reload_runtime_info()
                .await?;
            let BarrierWorkerRuntimeInfoSnapshot {
                active_streaming_nodes,
                recovery_context,
                mut state_table_committed_epochs,
                mut state_table_log_epochs,
                mut mv_depended_subscriptions,
                mut background_jobs,
                hummock_version_stats,
                database_infos,
                mut cdc_table_snapshot_splits,
            } = runtime_info_snapshot;

            let term_id = Uuid::new_v4().to_string();


            let mut control_stream_manager = ControlStreamManager::recover(
                    self.env.clone(),
                    active_streaming_nodes.current(),
                    &term_id,
                    self.context.clone(),
                )
                .await;
            {
                let mut empty_databases = HashSet::new();
                let mut collected_databases = HashMap::new();
                let mut collecting_databases = HashMap::new();
                let mut failed_databases = HashMap::new();
                for &database_id in recovery_context.fragment_context.database_map.keys() {
                    let mut injected_creating_jobs = HashSet::new();
                    let result: MetaResult<_> = try {
                        let Some(rendered_info) = render_runtime_info(
                            self.env.actor_id_generator(),
                            &active_streaming_nodes,
                            self.adaptive_parallelism_strategy,
                            &recovery_context,
                            database_id,
                        )
                            .inspect_err(|err: &MetaError| {
                                warn!(%database_id, err = %err.as_report(), "render runtime info failed");
                            })? else {
                            empty_databases.insert(database_id);
                            continue;
                        };
                        BarrierWorkerRuntimeInfoSnapshot::validate_database_info(
                            database_id,
                            &rendered_info.job_infos,
                            &active_streaming_nodes,
                            &rendered_info.stream_actors,
                            &state_table_committed_epochs,
                        )
                        .inspect_err(|err| {
                            warn!(%database_id, err = %err.as_report(), "rendered runtime info failed validation");
                        })?;
                        let RenderedDatabaseRuntimeInfo {
                            job_infos,
                            stream_actors,
                            mut source_splits,
                        } = rendered_info;
                        control_stream_manager.inject_database_initial_barrier(
                            database_id,
                            job_infos,
                            &recovery_context.job_extra_info,
                            &mut state_table_committed_epochs,
                            &mut state_table_log_epochs,
                            &recovery_context.fragment_relations,
                            &stream_actors,
                            &mut source_splits,
                            &mut background_jobs,
                            &mut mv_depended_subscriptions,
                            is_paused,
                            &hummock_version_stats,
                            &mut cdc_table_snapshot_splits,
                            &mut injected_creating_jobs,
                        )?
                    };
                    let node_to_collect = match result {
                        Ok(info) => {
                            info
                        }
                        Err(e) => {
                            warn!(%database_id, e = %e.as_report(), "failed to inject database initial barrier");
                            assert!(failed_databases.insert(database_id, injected_creating_jobs).is_none(), "non-duplicate");
                            continue;
                        }
                    };
                    if !node_to_collect.is_collected() {
                        assert!(collecting_databases.insert(database_id, node_to_collect).is_none());
                    } else {
                        warn!(%database_id, "database has no node to inject initial barrier");
                        assert!(collected_databases.insert(database_id, node_to_collect.finish()).is_none());
                    }
                }
                if !empty_databases.is_empty() {
                    info!(?empty_databases, "empty database in global recovery");
                }
                while !collecting_databases.is_empty() {
                    let (worker_id, result) =
                        control_stream_manager.next_response(&term_id, &self.context).await;
                    let resp = match result {
                        Err(e) => {
                            warn!(%worker_id, err = %e.as_report(), "worker node failure during recovery");
                            for (failed_database_id, collector) in collecting_databases.extract_if(|_, collector| {
                                !collector.is_valid_after_worker_err(worker_id)
                            }) {
                                warn!(%failed_database_id, %worker_id, "database failed to recovery in global recovery due to worker node err");
                                assert!(failed_databases.insert(failed_database_id, collector.creating_job_ids().collect()).is_none());
                            }
                            continue;
                        }
                        Ok(resp) => {
                            match resp {
                                Response::CompleteBarrier(resp) => {
                                    resp
                                }
                                Response::ReportPartialGraphFailure(resp) => {
                                    let (database_id, _) = from_partial_graph_id(resp.partial_graph_id);
                                    if let Some(collector) = collecting_databases.remove(&database_id) {
                                        warn!(%database_id, %worker_id, "database reset during global recovery");
                                        assert!(failed_databases.insert(database_id, collector.creating_job_ids().collect()).is_none());
                                    } else if let Some(database) = collected_databases.remove(&database_id) {
                                        warn!(%database_id, %worker_id, "database initialized but later reset during global recovery");
                                        assert!(failed_databases.insert(database_id, database.creating_streaming_job_controls.keys().copied().collect()).is_none());
                                    } else {
                                        assert!(failed_databases.contains_key(&database_id));
                                    }
                                    continue;
                                }
                                other @ (Response::Init(_) | Response::Shutdown(_) | Response::ResetPartialGraph(_)) => {
                                    return Err(anyhow!("get unexpected resp {:?}", other).into());
                                }
                            }
                        }
                    };
                    assert_eq!(worker_id, resp.worker_id);
                    let (database_id, _) = from_partial_graph_id(resp.partial_graph_id);
                    if failed_databases.contains_key(&database_id) {
                        assert!(!collecting_databases.contains_key(&database_id));
                        // ignore the lately arrived collect resp of failed database
                        continue;
                    }
                    let Entry::Occupied(mut entry) = collecting_databases.entry(database_id) else {
                        unreachable!("should exist")
                    };
                    let node_to_collect = entry.get_mut();
                    node_to_collect.collect_resp(resp);
                    if node_to_collect.is_collected() {
                        let node_to_collect = entry.remove();
                        assert!(collected_databases.insert(database_id, node_to_collect.finish()).is_none());
                    }
                }
                debug!("collected initial barrier");
                if !background_jobs.is_empty() {
                    warn!(job_ids = ?background_jobs.iter().collect_vec(), "unused recovered background mview in recovery");
                }
                if !mv_depended_subscriptions.is_empty() {
                    warn!(?mv_depended_subscriptions, "unused subscription infos in recovery");
                }
                if !state_table_committed_epochs.is_empty() {
                    warn!(?state_table_committed_epochs, "unused state table committed epoch in recovery");
                }
                if !enable_per_database_isolation && !failed_databases.is_empty() {
                    return Err(anyhow!(
                        "global recovery failed due to failure of databases {:?}",
                        failed_databases.keys().collect_vec()).into()
                    );
                }
                let checkpoint_control = CheckpointControl::recover(
                    collected_databases,
                    failed_databases,
                    &mut control_stream_manager,
                    hummock_version_stats,
                    self.env.clone(),
                );

                let reader = self.env.system_params_reader().await;
                let checkpoint_frequency = reader.checkpoint_frequency();
                let barrier_interval = Duration::from_millis(reader.barrier_interval_ms() as u64);
                let periodic_barriers = PeriodicBarriers::new(
                    barrier_interval,
                    checkpoint_frequency,
                    database_infos,
                );

                Ok((
                    active_streaming_nodes,
                    control_stream_manager,
                    checkpoint_control,
                    term_id,
                    periodic_barriers,
                ))
            }
        }.inspect_err(|err: &MetaError| {
            tracing::error!(error = %err.as_report(), "recovery failed");
            event_log_manager_ref.add_event_logs(vec![Event::Recovery(
                EventRecovery::global_recovery_failure(recovery_reason.clone(), err.to_report_string()),
            )]);
            GLOBAL_META_METRICS.recovery_failure_cnt.with_label_values(&["global"]).inc();
        }))
        .instrument(tracing::info_span!("recovery_attempt"));

        let mut recover_txs = vec![];
        let mut update_barrier_requests = vec![];
        pin_mut!(recovery_future);
        let mut request_rx_closed = false;
        let new_state = loop {
            select! {
                biased;
                new_state = &mut recovery_future => {
                    break new_state.expect("Retry until recovery success.");
                }
                request = pin!(self.request_rx.recv()), if !request_rx_closed => {
                    let Some(request) = request else {
                        warn!("request rx channel closed during recovery");
                        request_rx_closed = true;
                        continue;
                    };
                    match request {
                        BarrierManagerRequest::GetBackfillProgress(tx) => {
                            let _ = tx.send(Err(anyhow!("cluster under recovery[{}]", recovery_reason).into()));
                        }
                        BarrierManagerRequest::GetFragmentBackfillProgress(tx) => {
                            let _ = tx.send(Err(anyhow!("cluster under recovery[{}]", recovery_reason).into()));
                        }
                        BarrierManagerRequest::GetCdcProgress(tx) => {
                            let _ = tx.send(Err(anyhow!("cluster under recovery[{}]", recovery_reason).into()));
                        }
                        BarrierManagerRequest::AdhocRecovery(tx) => {
                            recover_txs.push(tx);
                        }
                        BarrierManagerRequest::UpdateDatabaseBarrier(request) => {
                            update_barrier_requests.push(request);
                        }
                        BarrierManagerRequest::MayHaveSnapshotBackfillingJob(tx) => {
                            // may recover snapshot backfill jobs
                            let _ = tx.send(true);
                        }
                    }
                }
            }
        };

        let duration = recovery_timer.stop_and_record();

        (
            self.active_streaming_nodes,
            self.control_stream_manager,
            self.checkpoint_control,
            self.term_id,
            self.periodic_barriers,
        ) = new_state;

        tracing::info!("recovery success");

        for UpdateDatabaseBarrierRequest {
            database_id,
            barrier_interval_ms,
            checkpoint_frequency,
            sender,
        } in update_barrier_requests
        {
            self.periodic_barriers.update_database_barrier(
                database_id,
                barrier_interval_ms,
                checkpoint_frequency,
            );
            let _ = sender.send(());
        }

        for tx in recover_txs {
            let _ = tx.send(());
        }

        let recovering_databases = self
            .checkpoint_control
            .recovering_databases()
            .map(|database| database.as_raw_id())
            .collect_vec();
        let running_databases = self
            .checkpoint_control
            .running_databases()
            .map(|database| database.as_raw_id())
            .collect_vec();

        event_log_manager_ref.add_event_logs(vec![Event::Recovery(
            EventRecovery::global_recovery_success(
                recovery_reason.clone(),
                duration as f32,
                running_databases,
                recovering_databases,
            ),
        )]);

        self.env
            .notification_manager()
            .notify_frontend_without_version(Operation::Update, Info::Recovery(Recovery {}));
        self.env
            .notification_manager()
            .notify_compute_without_version(Operation::Update, Info::Recovery(Recovery {}));
    }
}
