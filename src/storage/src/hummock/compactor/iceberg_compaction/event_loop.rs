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

use std::collections::hash_map::Entry;
use std::collections::{HashMap, VecDeque};
use std::sync::Arc;
use std::time::Duration;

use futures::{Stream, StreamExt, pin_mut};
use more_asserts::assert_ge;
use risingwave_pb::iceberg_compaction::{
    SubscribeIcebergCompactionEventRequest, SubscribeIcebergCompactionEventResponse,
    subscribe_iceberg_compaction_event_request, subscribe_iceberg_compaction_event_response,
};
use risingwave_rpc_client::HummockMetaClient;
use thiserror_ext::AsReport;
use tokio::sync::mpsc;
use tokio::sync::oneshot::{Receiver, Sender};
use tokio::task::JoinHandle;
use tokio::time::{Instant, Interval};

use super::iceberg_compactor_runner::{
    IcebergCompactionPlanRunner, IcebergCompactorRunnerConfigBuilder,
};
use super::{
    IcebergPlanCompletion, IcebergTaskMeta, IcebergTaskQueue, IcebergTaskReport,
    IcebergTaskTracker, PushResult, ReportSendResult, TaskKey, build_iceberg_task_report,
    create_task_execution, flush_pending_iceberg_task_reports, send_or_buffer_iceberg_task_report,
};
use crate::hummock::compactor::CompactorContext;
use crate::hummock::compactor::event_loop_utils::{
    COMPACTION_HEARTBEAT_LOG_INTERVAL, LogThrottler, StreamEvent, TaskShutdownRegistry,
    decode_stream_event, pending_pull_task_count, unix_timestamp_millis,
};

type IcebergShutdownRegistry = TaskShutdownRegistry<TaskKey>;

const STREAM_RETRY_INTERVAL: Duration = Duration::from_secs(30);
const MAX_PULL_TASK_COUNT: u32 = 4;

#[derive(Debug, Clone, PartialEq, Eq)]
struct IcebergCompactionLogState {
    running_parallelism: u32,
    waiting_parallelism: u32,
    available_parallelism: u32,
    pull_task_ack: bool,
    pending_pull_task_count: u32,
}

enum HandlerAction {
    Continue,
    Reconnect,
}

enum SessionExit {
    Reconnect,
    Shutdown,
}

enum IcebergResponseAction {
    Continue,
    CompactTask(risingwave_pb::iceberg_compaction::IcebergCompactionTask),
}

fn handle_iceberg_response_event(
    event: subscribe_iceberg_compaction_event_response::Event,
    pull_task_ack: &mut bool,
) -> IcebergResponseAction {
    match event {
        subscribe_iceberg_compaction_event_response::Event::CompactTask(task) => {
            IcebergResponseAction::CompactTask(task)
        }
        subscribe_iceberg_compaction_event_response::Event::PullTaskAck(_) => {
            *pull_task_ack = true;
            IcebergResponseAction::Continue
        }
    }
}

fn decode_iceberg_response(
    response: SubscribeIcebergCompactionEventResponse,
    pull_task_ack: &mut bool,
) -> Option<IcebergResponseAction> {
    response
        .event
        .map(|event| handle_iceberg_response_event(event, pull_task_ack))
}

#[must_use]
pub fn start_iceberg_compactor(
    compactor_context: CompactorContext,
    hummock_meta_client: Arc<dyn HummockMetaClient>,
) -> (JoinHandle<()>, Sender<()>) {
    let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel();
    let worker_num = compactor_context.compaction_executor.worker_num();
    let max_task_parallelism: u32 = (worker_num as f32
        * compactor_context.storage_opts.compactor_max_task_multiplier)
        .ceil() as u32;

    assert_ge!(
        compactor_context.storage_opts.compactor_max_task_multiplier,
        0.0
    );

    let pending_parallelism_budget = (max_task_parallelism as f32
        * compactor_context
            .storage_opts
            .iceberg_compaction_pending_parallelism_budget_multiplier)
        .ceil() as u32;
    let (task_completion_tx, task_completion_rx) =
        tokio::sync::mpsc::unbounded_channel::<IcebergPlanCompletion>();
    let runner = IcebergCompactorEventLoop {
        periodic_event_update_interval: Duration::from_millis(
            compactor_context
                .storage_opts
                .iceberg_compaction_pull_interval_ms,
        ),
        compactor_context,
        hummock_meta_client,
        shutdown_rx,
        state: IcebergRuntimeState::new(max_task_parallelism, pending_parallelism_budget),
        shutdown_map: IcebergShutdownRegistry::default(),
        task_completion_tx,
        task_completion_rx,
        plan_runners: HashMap::new(),
        worker_num,
    };

    let join_handle = tokio::spawn(async move { runner.run().await });
    (join_handle, shutdown_tx)
}

struct IcebergCompactorEventLoop {
    compactor_context: CompactorContext,
    hummock_meta_client: Arc<dyn HummockMetaClient>,
    shutdown_rx: Receiver<()>,
    state: IcebergRuntimeState,
    shutdown_map: IcebergShutdownRegistry,
    task_completion_tx: mpsc::UnboundedSender<IcebergPlanCompletion>,
    task_completion_rx: mpsc::UnboundedReceiver<IcebergPlanCompletion>,
    plan_runners: HashMap<TaskKey, IcebergCompactionPlanRunner>,
    periodic_event_update_interval: Duration,
    worker_num: usize,
}

struct IcebergRuntimeState {
    task_queue: IcebergTaskQueue,
    task_trackers: HashMap<u64, IcebergTaskTracker>,
    pending_task_reports: VecDeque<IcebergTaskReport>,
    max_task_parallelism: u32,
    max_pull_task_count: u32,
    pending_parallelism_budget: u32,
}

enum IcebergScheduleDecision {
    Start {
        meta: IcebergTaskMeta,
        runner: IcebergCompactionPlanRunner,
    },
    MissingRunner {
        task_key: TaskKey,
    },
}

impl IcebergRuntimeState {
    fn new(max_task_parallelism: u32, pending_parallelism_budget: u32) -> Self {
        Self {
            task_queue: IcebergTaskQueue::new(max_task_parallelism, pending_parallelism_budget),
            task_trackers: HashMap::new(),
            pending_task_reports: VecDeque::new(),
            max_task_parallelism,
            max_pull_task_count: std::cmp::min(max_task_parallelism, MAX_PULL_TASK_COUNT),
            pending_parallelism_budget,
        }
    }

    fn handle_plan_completion(
        &mut self,
        plan_completion: IcebergPlanCompletion,
        request_sender: &mpsc::UnboundedSender<SubscribeIcebergCompactionEventRequest>,
    ) -> HandlerAction {
        let task_key = plan_completion.task_key;
        let error_message = plan_completion.error_message;
        tracing::debug!(
            task_id = task_key.0,
            plan_index = task_key.1,
            success = error_message.is_none(),
            "Plan completed, updating queue state"
        );
        self.task_queue.finish_running(task_key);

        let completed_task_id = task_key.0;
        let Entry::Occupied(mut tracker_entry) = self.task_trackers.entry(completed_task_id) else {
            return HandlerAction::Continue;
        };
        tracker_entry.get_mut().record_completion(error_message);
        if !tracker_entry.get().is_finished() {
            return HandlerAction::Continue;
        }

        let report = tracker_entry.remove().into_report(completed_task_id);
        self.send_task_report(request_sender, report)
    }

    fn pop_schedulable_task(
        &mut self,
        plan_runners: &mut HashMap<TaskKey, IcebergCompactionPlanRunner>,
    ) -> Option<IcebergScheduleDecision> {
        let popped_task = self.task_queue.pop()?;
        let task_id = popped_task.meta.task_id;
        let plan_index = popped_task.meta.plan_index;
        let task_key = (task_id, plan_index);
        match plan_runners.remove(&task_key) {
            Some(runner) => Some(IcebergScheduleDecision::Start {
                meta: popped_task.meta,
                runner,
            }),
            None => {
                tracing::error!(
                    task_id = task_id,
                    plan_index = plan_index,
                    "Popped task missing runner, dropping queue entry"
                );
                self.task_queue.finish_running(task_key);
                Some(IcebergScheduleDecision::MissingRunner { task_key })
            }
        }
    }

    fn pull_tasks_if_needed(
        &self,
        request_sender: &mpsc::UnboundedSender<SubscribeIcebergCompactionEventRequest>,
        pull_task_ack: &mut bool,
    ) -> Option<u32> {
        pull_iceberg_tasks_if_needed(
            request_sender,
            self.task_queue.running_parallelism_sum(),
            self.max_task_parallelism,
            self.max_pull_task_count,
            pull_task_ack,
        )
    }

    fn log_state(
        &self,
        pull_task_ack: bool,
        pending_pull_task_count: u32,
        log_throttler: &mut LogThrottler<IcebergCompactionLogState>,
    ) {
        let running_count = self.task_queue.running_parallelism_sum();
        let waiting_count = self.task_queue.waiting_parallelism_sum();
        let current_state = IcebergCompactionLogState {
            running_parallelism: running_count,
            waiting_parallelism: waiting_count,
            available_parallelism: self.max_task_parallelism.saturating_sub(running_count),
            pull_task_ack,
            pending_pull_task_count,
        };

        if log_throttler.should_log(&current_state) {
            tracing::info!(
                running_parallelism_count = %current_state.running_parallelism,
                waiting_parallelism_count = %current_state.waiting_parallelism,
                available_parallelism = %current_state.available_parallelism,
                pull_task_ack = %current_state.pull_task_ack,
                pending_pull_task_count = %current_state.pending_pull_task_count
            );
            log_throttler.update(current_state);
        }
    }

    fn send_task_report(
        &mut self,
        request_sender: &mpsc::UnboundedSender<SubscribeIcebergCompactionEventRequest>,
        report: IcebergTaskReport,
    ) -> HandlerAction {
        if matches!(
            send_or_buffer_iceberg_task_report(
                request_sender,
                &mut self.pending_task_reports,
                report,
            ),
            ReportSendResult::RestartStream
        ) {
            return HandlerAction::Reconnect;
        }
        HandlerAction::Continue
    }
}

struct IcebergStreamSession<S> {
    request_sender: mpsc::UnboundedSender<SubscribeIcebergCompactionEventRequest>,
    response_event_stream: S,
    pull_task_ack: bool,
}

enum IcebergSessionStart<S> {
    Ready(IcebergStreamSession<S>),
    Reconnect,
}

fn prepare_iceberg_stream_session<S>(
    request_sender: mpsc::UnboundedSender<SubscribeIcebergCompactionEventRequest>,
    response_event_stream: S,
    pending_task_reports: &mut VecDeque<IcebergTaskReport>,
) -> IcebergSessionStart<S>
where
    S: Stream<Item = Result<SubscribeIcebergCompactionEventResponse, tonic::Status>>,
{
    if matches!(
        flush_pending_iceberg_task_reports(&request_sender, pending_task_reports),
        ReportSendResult::RestartStream
    ) {
        return IcebergSessionStart::Reconnect;
    }

    IcebergSessionStart::Ready(IcebergStreamSession::new(
        request_sender,
        response_event_stream,
    ))
}

impl<S> IcebergStreamSession<S>
where
    S: Stream<Item = Result<SubscribeIcebergCompactionEventResponse, tonic::Status>>,
{
    fn new(
        request_sender: mpsc::UnboundedSender<SubscribeIcebergCompactionEventRequest>,
        response_event_stream: S,
    ) -> Self {
        Self {
            request_sender,
            response_event_stream,
            pull_task_ack: true,
        }
    }

    async fn consume(
        self,
        handler: &mut IcebergCompactorEventLoop,
        periodic_event_interval: &mut Interval,
        log_throttler: &mut LogThrottler<IcebergCompactionLogState>,
    ) -> SessionExit {
        let Self {
            request_sender,
            response_event_stream,
            mut pull_task_ack,
        } = self;
        pin_mut!(response_event_stream);
        let mut event_loop_iteration_now = Instant::now();

        loop {
            handler
                .compactor_context
                .compactor_metrics
                .compaction_event_loop_iteration_latency
                .observe(event_loop_iteration_now.elapsed().as_millis() as _);
            event_loop_iteration_now = Instant::now();

            let event = tokio::select! {
                Some(plan_completion) = handler.task_completion_rx.recv() => {
                    if matches!(
                        handler
                            .state
                            .handle_plan_completion(plan_completion, &request_sender),
                        HandlerAction::Reconnect
                    ) {
                        return SessionExit::Reconnect;
                    }
                    continue;
                }
                _ = handler.state.task_queue.wait_schedulable() => {
                    handler.schedule_queued_tasks();
                    continue;
                }
                _ = periodic_event_interval.tick() => {
                    if matches!(
                        handler.handle_tick(
                            &request_sender,
                            &mut pull_task_ack,
                            log_throttler,
                        ),
                        HandlerAction::Reconnect
                    ) {
                        return SessionExit::Reconnect;
                    }
                    continue;
                }
                event = response_event_stream.next() => {
                    event
                }
                _ = &mut handler.shutdown_rx => {
                    tracing::info!("Iceberg Compactor is shutting down");
                    return SessionExit::Shutdown;
                }
            };

            match decode_stream_event(event, "iceberg compaction event stream") {
                StreamEvent::Response(response) => {
                    if matches!(
                        handler
                            .handle_response(response, &request_sender, &mut pull_task_ack)
                            .await,
                        HandlerAction::Reconnect
                    ) {
                        return SessionExit::Reconnect;
                    }
                }
                StreamEvent::Reconnect => return SessionExit::Reconnect,
            }
        }
    }
}

impl IcebergCompactorEventLoop {
    async fn run(mut self) {
        let mut min_interval = tokio::time::interval(STREAM_RETRY_INTERVAL);
        let mut periodic_event_interval =
            tokio::time::interval(self.periodic_event_update_interval);
        let mut log_throttler =
            LogThrottler::<IcebergCompactionLogState>::new(COMPACTION_HEARTBEAT_LOG_INTERVAL);

        loop {
            tokio::select! {
                _ = min_interval.tick() => {}
                _ = &mut self.shutdown_rx => {
                    tracing::info!("Compactor is shutting down");
                    return;
                }
            }

            let (request_sender, response_event_stream) = match self
                .hummock_meta_client
                .subscribe_iceberg_compaction_event()
                .await
            {
                Ok((request_sender, response_event_stream)) => {
                    tracing::debug!("Succeeded subscribe_iceberg_compaction_event.");
                    (request_sender, response_event_stream)
                }
                Err(e) => {
                    tracing::warn!(
                        error = %e.as_report(),
                        "Subscribing to iceberg compaction tasks failed with error. Will retry.",
                    );
                    continue;
                }
            };

            let session = match prepare_iceberg_stream_session(
                request_sender,
                response_event_stream,
                &mut self.state.pending_task_reports,
            ) {
                IcebergSessionStart::Ready(session) => session,
                IcebergSessionStart::Reconnect => continue,
            };
            match session
                .consume(&mut self, &mut periodic_event_interval, &mut log_throttler)
                .await
            {
                SessionExit::Reconnect => continue,
                SessionExit::Shutdown => return,
            }
        }
    }

    fn schedule_queued_tasks(&mut self) {
        while let Some(decision) = self.state.pop_schedulable_task(&mut self.plan_runners) {
            let (meta, runner) = match decision {
                IcebergScheduleDecision::Start { meta, runner } => (meta, runner),
                IcebergScheduleDecision::MissingRunner { task_key } => {
                    tracing::debug!(
                        task_id = task_key.0,
                        plan_index = task_key.1,
                        "Skipped scheduling iceberg task with missing runner"
                    );
                    continue;
                }
            };
            let task_id = meta.task_id;
            let plan_index = meta.plan_index;
            let task_key = (task_id, plan_index);
            let unique_ident = runner.unique_ident();

            let executor = self.compactor_context.compaction_executor.clone();
            let shutdown_registry = self.shutdown_map.clone();
            let completion_tx = self.task_completion_tx.clone();

            tracing::info!(
                task_id = task_id,
                plan_index = plan_index,
                unique_ident = ?unique_ident,
                required_parallelism = meta.required_parallelism,
                "Starting iceberg compaction task from queue"
            );

            executor.spawn(async move {
                let shutdown_registration = shutdown_registry.register(task_key);
                if shutdown_registration.replaced_existing {
                    tracing::warn!(
                        task_id = task_key.0,
                        plan_index = task_key.1,
                        "Replaced existing iceberg compaction shutdown handle"
                    );
                }
                let _shutdown_guard = shutdown_registration.guard;

                let result = Box::pin(runner.compact(shutdown_registration.shutdown_rx)).await;
                let completion = match result {
                    Ok(_) => IcebergPlanCompletion {
                        task_key,
                        error_message: None,
                    },
                    Err(e) => {
                        tracing::warn!(error = %e.as_report(), task_id = task_key.0, plan_index = task_key.1, "Failed to compact iceberg runner");
                        IcebergPlanCompletion {
                            task_key,
                            error_message: Some(e.to_report_string()),
                        }
                    }
                };

                if completion_tx.send(completion).is_err() {
                    tracing::warn!(task_id = task_key.0, plan_index = task_key.1, "Failed to notify task completion - main loop may have shut down");
                }
            });
        }
    }

    fn handle_tick(
        &mut self,
        request_sender: &mpsc::UnboundedSender<SubscribeIcebergCompactionEventRequest>,
        pull_task_ack: &mut bool,
        log_throttler: &mut LogThrottler<IcebergCompactionLogState>,
    ) -> HandlerAction {
        let pending_pull_task_count = self
            .state
            .pull_tasks_if_needed(request_sender, pull_task_ack);
        if pending_pull_task_count.is_none() {
            return HandlerAction::Reconnect;
        }

        self.state.log_state(
            *pull_task_ack,
            pending_pull_task_count.unwrap_or_default(),
            log_throttler,
        );
        HandlerAction::Continue
    }

    async fn handle_response(
        &mut self,
        response: SubscribeIcebergCompactionEventResponse,
        request_sender: &mpsc::UnboundedSender<SubscribeIcebergCompactionEventRequest>,
        pull_task_ack: &mut bool,
    ) -> HandlerAction {
        let Some(action) = decode_iceberg_response(response, pull_task_ack) else {
            return HandlerAction::Continue;
        };

        match action {
            IcebergResponseAction::Continue => HandlerAction::Continue,
            IcebergResponseAction::CompactTask(task) => {
                self.handle_compact_task(task, request_sender).await
            }
        }
    }

    async fn handle_compact_task(
        &mut self,
        iceberg_compaction_task: risingwave_pb::iceberg_compaction::IcebergCompactionTask,
        request_sender: &mpsc::UnboundedSender<SubscribeIcebergCompactionEventRequest>,
    ) -> HandlerAction {
        let task_id = iceberg_compaction_task.task_id;
        let sink_id = iceberg_compaction_task.sink_id;
        let compactor_runner_config = match self.build_runner_config() {
            Ok(config) => config,
            Err(e) => {
                tracing::warn!(error = %e.as_report(), "Failed to build iceberg compactor runner config {}", task_id);
                return self.report_task_creation_failure(
                    request_sender,
                    task_id,
                    sink_id,
                    format!(
                        "Failed to build iceberg compactor runner config: {}",
                        e.as_report()
                    ),
                );
            }
        };

        let task_execution = match create_task_execution(
            iceberg_compaction_task,
            compactor_runner_config,
            self.compactor_context.compactor_metrics.clone(),
        )
        .await
        {
            Ok(task_execution) => task_execution,
            Err(e) => {
                tracing::warn!(error = %e.as_report(), task_id, "Failed to create plan runners");
                return self.report_task_creation_failure(
                    request_sender,
                    task_id,
                    sink_id,
                    format!(
                        "Failed to create iceberg compaction task execution: {}",
                        e.as_report()
                    ),
                );
            }
        };

        let sink_id = task_execution.sink_id;
        let plan_runners = task_execution.plan_runners;
        if plan_runners.is_empty() {
            tracing::info!(task_id, sink_id, "No plans to execute");
            return self.state.send_task_report(
                request_sender,
                build_iceberg_task_report(task_id, sink_id, None),
            );
        }

        let total_plans = plan_runners.len();
        let enqueued_count = self.enqueue_plan_runners(task_id, total_plans, plan_runners);
        if enqueued_count == 0 {
            return self.state.send_task_report(
                request_sender,
                build_iceberg_task_report(
                    task_id,
                    sink_id,
                    Some("Failed to enqueue all iceberg compaction plans".to_owned()),
                ),
            );
        }

        self.state
            .task_trackers
            .insert(task_id, IcebergTaskTracker::new(sink_id, enqueued_count));
        tracing::info!(
            task_id = task_id,
            sink_id = sink_id,
            total_plans = total_plans,
            enqueued_count = enqueued_count,
            "Enqueued {} of {} Iceberg plan runners",
            enqueued_count,
            total_plans
        );
        HandlerAction::Continue
    }

    fn build_runner_config(
        &self,
    ) -> Result<
        super::iceberg_compactor_runner::IcebergCompactorRunnerConfig,
        super::iceberg_compactor_runner::IcebergCompactorRunnerConfigBuilderError,
    > {
        IcebergCompactorRunnerConfigBuilder::default()
            .max_parallelism(
                (self.worker_num as f32
                    * self
                        .compactor_context
                        .storage_opts
                        .iceberg_compaction_task_parallelism_ratio) as u32,
            )
            .min_size_per_partition(
                self.compactor_context
                    .storage_opts
                    .iceberg_compaction_min_size_per_partition_mb as u64
                    * 1024
                    * 1024,
            )
            .max_file_count_per_partition(
                self.compactor_context
                    .storage_opts
                    .iceberg_compaction_max_file_count_per_partition,
            )
            .enable_validate_compaction(
                self.compactor_context
                    .storage_opts
                    .iceberg_compaction_enable_validate,
            )
            .max_record_batch_rows(
                self.compactor_context
                    .storage_opts
                    .iceberg_compaction_max_record_batch_rows,
            )
            .enable_heuristic_output_parallelism(
                self.compactor_context
                    .storage_opts
                    .iceberg_compaction_enable_heuristic_output_parallelism,
            )
            .max_concurrent_closes(
                self.compactor_context
                    .storage_opts
                    .iceberg_compaction_max_concurrent_closes,
            )
            .target_binpack_group_size_mb(
                self.compactor_context
                    .storage_opts
                    .iceberg_compaction_target_binpack_group_size_mb,
            )
            .min_group_size_mb(
                self.compactor_context
                    .storage_opts
                    .iceberg_compaction_min_group_size_mb,
            )
            .min_group_file_count(
                self.compactor_context
                    .storage_opts
                    .iceberg_compaction_min_group_file_count,
            )
            .build()
    }

    fn enqueue_plan_runners(
        &mut self,
        task_id: u64,
        total_plans: usize,
        plan_runners: Vec<IcebergCompactionPlanRunner>,
    ) -> usize {
        let mut enqueued_count = 0;
        for runner in plan_runners {
            let meta = runner.to_meta();
            let task_key = meta.key();
            let plan_index = meta.plan_index;
            let required_parallelism = meta.required_parallelism;

            match self.state.task_queue.push(meta) {
                PushResult::Added => {
                    if self.plan_runners.insert(task_key, runner).is_some() {
                        tracing::error!(
                            task_id = task_id,
                            plan_index = plan_index,
                            "Iceberg runner map already had a runner after successful queue admission"
                        );
                    }
                    enqueued_count += 1;
                    tracing::debug!(
                        task_id = task_id,
                        plan_index = plan_index,
                        required_parallelism = required_parallelism,
                        "Iceberg plan runner added to queue"
                    );
                }
                PushResult::RejectedCapacity => {
                    tracing::warn!(
                        task_id = task_id,
                        required_parallelism = required_parallelism,
                        pending_budget = self.state.pending_parallelism_budget,
                        enqueued_count = enqueued_count,
                        total_plans = total_plans,
                        "Iceberg plan runner rejected - queue capacity exceeded"
                    );
                    break;
                }
                PushResult::RejectedTooLarge => {
                    tracing::error!(
                        task_id = task_id,
                        required_parallelism = required_parallelism,
                        max_parallelism = self.state.max_task_parallelism,
                        "Iceberg plan runner rejected - parallelism exceeds max"
                    );
                }
                PushResult::RejectedInvalidParallelism => {
                    tracing::error!(
                        task_id = task_id,
                        required_parallelism = required_parallelism,
                        "Iceberg plan runner rejected - invalid parallelism"
                    );
                }
                PushResult::RejectedDuplicate => {
                    tracing::error!(
                        task_id = task_id,
                        plan_index = plan_index,
                        "Iceberg plan runner rejected - duplicate (task_id, plan_index)"
                    );
                }
            }
        }
        enqueued_count
    }

    fn report_task_creation_failure(
        &mut self,
        request_sender: &mpsc::UnboundedSender<SubscribeIcebergCompactionEventRequest>,
        task_id: u64,
        sink_id: u32,
        error_message: String,
    ) -> HandlerAction {
        self.state.send_task_report(
            request_sender,
            build_iceberg_task_report(task_id, sink_id, Some(error_message)),
        )
    }
}

fn pull_iceberg_tasks_if_needed(
    request_sender: &mpsc::UnboundedSender<SubscribeIcebergCompactionEventRequest>,
    running_parallelism: u32,
    max_task_parallelism: u32,
    max_pull_task_count: u32,
    pull_task_ack: &mut bool,
) -> Option<u32> {
    let pull_count = pending_pull_task_count(
        max_task_parallelism,
        max_pull_task_count,
        running_parallelism,
        *pull_task_ack,
    );
    if pull_count == 0 {
        return Some(0);
    }

    if let Err(e) = request_sender.send(SubscribeIcebergCompactionEventRequest {
        event: Some(subscribe_iceberg_compaction_event_request::Event::PullTask(
            subscribe_iceberg_compaction_event_request::PullTask {
                pull_task_count: pull_count,
            },
        )),
        create_at: unix_timestamp_millis(),
    }) {
        tracing::warn!(error = %e.as_report(), "Failed to pull task - will retry on stream restart");
        return None;
    }

    *pull_task_ack = false;
    Some(pull_count)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn expect_pull_task(
        rx: &mut mpsc::UnboundedReceiver<SubscribeIcebergCompactionEventRequest>,
        expected_pull_count: u32,
    ) {
        let request = rx.try_recv().expect("pull task request should be sent");
        let Some(subscribe_iceberg_compaction_event_request::Event::PullTask(pull_task)) =
            request.event
        else {
            panic!("expected pull task request");
        };
        assert_eq!(pull_task.pull_task_count, expected_pull_count);
    }

    fn expect_report_task(
        rx: &mut mpsc::UnboundedReceiver<SubscribeIcebergCompactionEventRequest>,
        expected_task_id: u64,
        expected_sink_id: u32,
        expect_success: bool,
    ) {
        let request = rx.try_recv().expect("report task request should be sent");
        let Some(subscribe_iceberg_compaction_event_request::Event::ReportTask(report_task)) =
            request.event
        else {
            panic!("expected report task request");
        };
        assert_eq!(report_task.task_id, expected_task_id);
        assert_eq!(report_task.sink_id, expected_sink_id);
        assert_eq!(
            report_task.status,
            if expect_success {
                subscribe_iceberg_compaction_event_request::report_task::Status::Success as i32
            } else {
                subscribe_iceberg_compaction_event_request::report_task::Status::Failed as i32
            }
        );
    }

    fn add_running_plan(state: &mut IcebergRuntimeState, task_id: u64, plan_index: usize, p: u32) {
        assert_eq!(
            state.task_queue.push(IcebergTaskMeta {
                task_id,
                plan_index,
                required_parallelism: p,
            }),
            PushResult::Added
        );
        assert_eq!(
            state.task_queue.pop().unwrap().meta.key(),
            (task_id, plan_index)
        );
    }

    #[test]
    fn test_prepare_iceberg_stream_session_flushes_pending_reports_before_consuming() {
        let (tx, mut rx) = mpsc::unbounded_channel();
        let mut pending_reports = VecDeque::from([build_iceberg_task_report(7, 9, None)]);
        let stream = futures::stream::empty::<
            Result<SubscribeIcebergCompactionEventResponse, tonic::Status>,
        >();

        let session_start = prepare_iceberg_stream_session(tx, stream, &mut pending_reports);

        let IcebergSessionStart::Ready(session) = session_start else {
            panic!("expected stream session to start");
        };
        assert!(session.pull_task_ack);
        assert!(pending_reports.is_empty());
        expect_report_task(&mut rx, 7, 9, true);
    }

    #[test]
    fn test_prepare_iceberg_stream_session_reconnects_when_pending_report_flush_fails() {
        let (tx, rx) = mpsc::unbounded_channel();
        drop(rx);
        let mut pending_reports = VecDeque::from([build_iceberg_task_report(7, 9, None)]);
        let stream = futures::stream::empty::<
            Result<SubscribeIcebergCompactionEventResponse, tonic::Status>,
        >();

        let session_start = prepare_iceberg_stream_session(tx, stream, &mut pending_reports);

        assert!(matches!(session_start, IcebergSessionStart::Reconnect));
        assert_eq!(pending_reports.len(), 1);
        assert_eq!(pending_reports[0].task_id, 7);
        assert_eq!(pending_reports[0].sink_id, 9);
    }

    #[test]
    fn test_iceberg_response_event_ignores_empty_event() {
        let mut pull_task_ack = false;

        let action = decode_iceberg_response(
            SubscribeIcebergCompactionEventResponse {
                event: None,
                create_at: 0,
            },
            &mut pull_task_ack,
        );

        assert!(action.is_none());
        assert!(!pull_task_ack);
    }

    #[test]
    fn test_iceberg_response_event_sets_pull_task_ack() {
        let mut pull_task_ack = false;

        let action = handle_iceberg_response_event(
            subscribe_iceberg_compaction_event_response::Event::PullTaskAck(
                subscribe_iceberg_compaction_event_response::PullTaskAck {},
            ),
            &mut pull_task_ack,
        );

        assert!(matches!(action, IcebergResponseAction::Continue));
        assert!(pull_task_ack);
    }

    #[test]
    fn test_iceberg_response_event_forwards_compact_task() {
        let mut pull_task_ack = false;

        let action = handle_iceberg_response_event(
            subscribe_iceberg_compaction_event_response::Event::CompactTask(
                risingwave_pb::iceberg_compaction::IcebergCompactionTask {
                    task_id: 7,
                    sink_id: 9,
                    ..Default::default()
                },
            ),
            &mut pull_task_ack,
        );

        let IcebergResponseAction::CompactTask(task) = action else {
            panic!("expected compact task action");
        };
        assert_eq!(task.task_id, 7);
        assert_eq!(task.sink_id, 9);
        assert!(!pull_task_ack);
    }

    #[test]
    fn test_pull_iceberg_tasks_sends_request_and_waits_for_ack() {
        let (tx, mut rx) = mpsc::unbounded_channel();
        let mut pull_task_ack = true;

        let result = pull_iceberg_tasks_if_needed(&tx, 2, 8, 4, &mut pull_task_ack);

        assert_eq!(result, Some(4));
        assert!(!pull_task_ack);
        expect_pull_task(&mut rx, 4);
    }

    #[test]
    fn test_pull_iceberg_tasks_skips_when_ack_is_pending() {
        let (tx, mut rx) = mpsc::unbounded_channel();
        let mut pull_task_ack = false;

        let result = pull_iceberg_tasks_if_needed(&tx, 0, 8, 4, &mut pull_task_ack);

        assert_eq!(result, Some(0));
        assert!(!pull_task_ack);
        assert!(rx.try_recv().is_err());
    }

    #[test]
    fn test_pull_iceberg_tasks_skips_when_parallelism_is_full() {
        let (tx, mut rx) = mpsc::unbounded_channel();
        let mut pull_task_ack = true;

        let result = pull_iceberg_tasks_if_needed(&tx, 8, 8, 4, &mut pull_task_ack);

        assert_eq!(result, Some(0));
        assert!(pull_task_ack);
        assert!(rx.try_recv().is_err());
    }

    #[test]
    fn test_pull_iceberg_tasks_reconnects_on_send_failure() {
        let (tx, rx) = mpsc::unbounded_channel();
        drop(rx);
        let mut pull_task_ack = true;

        let result = pull_iceberg_tasks_if_needed(&tx, 0, 8, 4, &mut pull_task_ack);

        assert_eq!(result, None);
        assert!(pull_task_ack);
    }

    #[test]
    fn test_iceberg_completion_releases_parallelism_and_reports_when_tracker_finishes() {
        let (tx, mut rx) = mpsc::unbounded_channel();
        let mut state = IcebergRuntimeState::new(8, 32);
        add_running_plan(&mut state, 7, 0, 3);
        state.task_trackers.insert(7, IcebergTaskTracker::new(9, 1));

        let action = state.handle_plan_completion(
            IcebergPlanCompletion {
                task_key: (7, 0),
                error_message: None,
            },
            &tx,
        );

        assert!(matches!(action, HandlerAction::Continue));
        assert_eq!(state.task_queue.running_parallelism_sum(), 0);
        assert!(!state.task_trackers.contains_key(&7));
        assert!(state.pending_task_reports.is_empty());
        expect_report_task(&mut rx, 7, 9, true);
    }

    #[test]
    fn test_iceberg_completion_keeps_tracker_until_all_admitted_plans_finish() {
        let (tx, mut rx) = mpsc::unbounded_channel();
        let mut state = IcebergRuntimeState::new(8, 32);
        add_running_plan(&mut state, 7, 0, 2);
        add_running_plan(&mut state, 7, 1, 2);
        state.task_trackers.insert(7, IcebergTaskTracker::new(9, 2));

        let action = state.handle_plan_completion(
            IcebergPlanCompletion {
                task_key: (7, 0),
                error_message: Some("plan failed".to_owned()),
            },
            &tx,
        );

        assert!(matches!(action, HandlerAction::Continue));
        assert_eq!(state.task_queue.running_parallelism_sum(), 2);
        assert!(state.task_trackers.contains_key(&7));
        assert!(rx.try_recv().is_err());

        let action = state.handle_plan_completion(
            IcebergPlanCompletion {
                task_key: (7, 1),
                error_message: None,
            },
            &tx,
        );

        assert!(matches!(action, HandlerAction::Continue));
        assert_eq!(state.task_queue.running_parallelism_sum(), 0);
        assert!(!state.task_trackers.contains_key(&7));
        expect_report_task(&mut rx, 7, 9, true);
    }

    #[test]
    fn test_iceberg_completion_buffers_report_and_reconnects_on_send_failure() {
        let (tx, rx) = mpsc::unbounded_channel();
        drop(rx);
        let mut state = IcebergRuntimeState::new(8, 32);
        add_running_plan(&mut state, 7, 0, 3);
        state.task_trackers.insert(7, IcebergTaskTracker::new(9, 1));

        let action = state.handle_plan_completion(
            IcebergPlanCompletion {
                task_key: (7, 0),
                error_message: Some("failed".to_owned()),
            },
            &tx,
        );

        assert!(matches!(action, HandlerAction::Reconnect));
        assert_eq!(state.task_queue.running_parallelism_sum(), 0);
        assert!(!state.task_trackers.contains_key(&7));
        assert_eq!(state.pending_task_reports.len(), 1);
        assert_eq!(state.pending_task_reports[0].task_id, 7);
        assert_eq!(state.pending_task_reports[0].sink_id, 9);
    }

    #[test]
    fn test_iceberg_completion_without_tracker_only_releases_parallelism() {
        let (tx, mut rx) = mpsc::unbounded_channel();
        let mut state = IcebergRuntimeState::new(8, 32);
        add_running_plan(&mut state, 7, 0, 3);

        let action = state.handle_plan_completion(
            IcebergPlanCompletion {
                task_key: (7, 0),
                error_message: None,
            },
            &tx,
        );

        assert!(matches!(action, HandlerAction::Continue));
        assert_eq!(state.task_queue.running_parallelism_sum(), 0);
        assert!(state.pending_task_reports.is_empty());
        assert!(rx.try_recv().is_err());
    }

    #[test]
    fn test_iceberg_schedule_missing_runner_releases_parallelism() {
        let mut state = IcebergRuntimeState::new(8, 32);
        let mut plan_runners = HashMap::new();
        assert_eq!(
            state.task_queue.push(IcebergTaskMeta {
                task_id: 7,
                plan_index: 0,
                required_parallelism: 3,
            }),
            PushResult::Added
        );

        let decision = state
            .pop_schedulable_task(&mut plan_runners)
            .expect("queued task should be popped");

        let IcebergScheduleDecision::MissingRunner { task_key } = decision else {
            panic!("expected missing runner decision");
        };
        assert_eq!(task_key, (7, 0));
        assert_eq!(state.task_queue.running_parallelism_sum(), 0);
        assert_eq!(state.task_queue.waiting_parallelism_sum(), 0);
        assert!(state.pop_schedulable_task(&mut plan_runners).is_none());
    }
}
