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

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::Duration;

use futures::{Stream, StreamExt, pin_mut};
use more_asserts::assert_ge;
use risingwave_hummock_sdk::compact_task::{CompactTask, ValidationTask};
use risingwave_hummock_sdk::table_stats::{TableStatsMap, to_prost_table_stats_map};
use risingwave_hummock_sdk::{HummockSstableObjectId, compact_task_to_string};
use risingwave_pb::hummock::compact_task::TaskStatus;
use risingwave_pb::hummock::subscribe_compaction_event_request::{
    Event as RequestEvent, HeartBeat, PullTask, ReportTask,
};
use risingwave_pb::hummock::subscribe_compaction_event_response::Event as ResponseEvent;
use risingwave_pb::hummock::{SubscribeCompactionEventRequest, SubscribeCompactionEventResponse};
use risingwave_rpc_client::HummockMetaClient;
use thiserror_ext::AsReport;
use tokio::sync::mpsc;
use tokio::sync::oneshot::{Receiver, Sender};
use tokio::task::JoinHandle;
use tokio::time::{Instant, Interval};

use super::CompactorContext;
use super::compaction_utils::{calculate_task_parallelism, check_compaction_result};
use super::compactor_runner::{self, compact_done};
use super::event_loop_utils::{
    COMPACTION_HEARTBEAT_LOG_INTERVAL, LogThrottler, ShutdownCancelResult, StreamEvent,
    TaskShutdownGuard, TaskShutdownRegistry, decode_stream_event, get_task_progress,
    pending_pull_task_count, unix_timestamp_millis,
};
use crate::compaction_catalog_manager::CompactionCatalogManagerRef;
use crate::hummock::{ObjectIdManager, validate_ssts};

type CompactionShutdownRegistry = TaskShutdownRegistry<u64>;
type TaskProgressMap = Arc<
    parking_lot::lock_api::Mutex<parking_lot::RawMutex, HashMap<u64, Arc<super::TaskProgress>>>,
>;

const STREAM_RETRY_INTERVAL: Duration = Duration::from_secs(30);
const PERIODIC_EVENT_UPDATE_INTERVAL: Duration = Duration::from_millis(1000);
const MAX_PULL_TASK_COUNT: u32 = 4;

#[derive(Debug, Clone, PartialEq, Eq)]
struct CompactionLogState {
    running_parallelism: u32,
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

enum HummockResponseAction {
    Continue,
    CompactTask(risingwave_pb::hummock::CompactTask),
    ValidationTask(risingwave_pb::hummock::ValidationTask),
    CancelTask(u64),
}

fn handle_hummock_response_event(
    event: ResponseEvent,
    pull_task_ack: &mut bool,
) -> HummockResponseAction {
    match event {
        ResponseEvent::CompactTask(compact_task) => {
            HummockResponseAction::CompactTask(compact_task)
        }
        #[expect(deprecated)]
        ResponseEvent::VacuumTask(_) => {
            unreachable!("unexpected vacuum task");
        }
        #[expect(deprecated)]
        ResponseEvent::FullScanTask(_) => {
            unreachable!("unexpected scan task");
        }
        #[expect(deprecated)]
        ResponseEvent::ValidationTask(validation_task) => {
            HummockResponseAction::ValidationTask(validation_task)
        }
        ResponseEvent::CancelCompactTask(cancel_compact_task) => {
            HummockResponseAction::CancelTask(cancel_compact_task.task_id)
        }
        ResponseEvent::PullTaskAck(_) => {
            *pull_task_ack = true;
            HummockResponseAction::Continue
        }
    }
}

fn decode_hummock_response(
    response: SubscribeCompactionEventResponse,
    pull_task_ack: &mut bool,
) -> Option<(HummockResponseAction, u64)> {
    let SubscribeCompactionEventResponse { event, create_at } = response;
    event.map(|event| {
        (
            handle_hummock_response_event(event, pull_task_ack),
            create_at,
        )
    })
}

pub fn start_compactor(
    compactor_context: CompactorContext,
    hummock_meta_client: Arc<dyn HummockMetaClient>,
    object_id_manager: Arc<ObjectIdManager>,
    compaction_catalog_manager_ref: CompactionCatalogManagerRef,
) -> (JoinHandle<()>, Sender<()>) {
    let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel();
    let max_task_parallelism: u32 = (compactor_context.compaction_executor.worker_num() as f32
        * compactor_context.storage_opts.compactor_max_task_multiplier)
        .ceil() as u32;

    assert_ge!(
        compactor_context.storage_opts.compactor_max_task_multiplier,
        0.0
    );

    let runner = HummockCompactorEventLoop {
        task_progress: compactor_context.task_progress_manager.clone(),
        compactor_context,
        hummock_meta_client,
        object_id_manager,
        compaction_catalog_manager_ref,
        shutdown_rx,
        shutdown_map: CompactionShutdownRegistry::default(),
        running_task_parallelism: Arc::new(AtomicU32::new(0)),
        max_task_parallelism,
        max_pull_task_count: std::cmp::min(max_task_parallelism, MAX_PULL_TASK_COUNT),
    };

    let join_handle = tokio::spawn(async move { runner.run().await });
    (join_handle, shutdown_tx)
}

struct HummockCompactorEventLoop {
    compactor_context: CompactorContext,
    hummock_meta_client: Arc<dyn HummockMetaClient>,
    object_id_manager: Arc<ObjectIdManager>,
    compaction_catalog_manager_ref: CompactionCatalogManagerRef,
    task_progress: TaskProgressMap,
    shutdown_rx: Receiver<()>,
    shutdown_map: CompactionShutdownRegistry,
    running_task_parallelism: Arc<AtomicU32>,
    max_task_parallelism: u32,
    max_pull_task_count: u32,
}

struct HummockStreamSession<S> {
    request_sender: mpsc::UnboundedSender<SubscribeCompactionEventRequest>,
    response_event_stream: S,
    pull_task_ack: bool,
}

impl<S> HummockStreamSession<S>
where
    S: Stream<Item = Result<SubscribeCompactionEventResponse, tonic::Status>>,
{
    fn new(
        request_sender: mpsc::UnboundedSender<SubscribeCompactionEventRequest>,
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
        handler: &mut HummockCompactorEventLoop,
        periodic_event_interval: &mut Interval,
        log_throttler: &mut LogThrottler<CompactionLogState>,
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
                    tracing::info!("Compactor is shutting down");
                    return SessionExit::Shutdown;
                }
            };

            match decode_stream_event(event, "hummock compaction event stream") {
                StreamEvent::Response(response) => {
                    if matches!(
                        handler.handle_response(response, &request_sender, &mut pull_task_ack),
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

impl HummockCompactorEventLoop {
    async fn run(mut self) {
        let mut min_interval = tokio::time::interval(STREAM_RETRY_INTERVAL);
        let mut periodic_event_interval = tokio::time::interval(PERIODIC_EVENT_UPDATE_INTERVAL);
        let mut log_throttler =
            LogThrottler::<CompactionLogState>::new(COMPACTION_HEARTBEAT_LOG_INTERVAL);

        loop {
            tokio::select! {
                _ = min_interval.tick() => {}
                _ = &mut self.shutdown_rx => {
                    tracing::info!("Compactor is shutting down");
                    return;
                }
            }

            let (request_sender, response_event_stream) =
                match self.hummock_meta_client.subscribe_compaction_event().await {
                    Ok((request_sender, response_event_stream)) => {
                        tracing::debug!("Succeeded subscribe_compaction_event.");
                        (request_sender, response_event_stream)
                    }
                    Err(e) => {
                        tracing::warn!(
                            error = %e.as_report(),
                            "Subscribing to compaction tasks failed with error. Will retry.",
                        );
                        continue;
                    }
                };

            let session = HummockStreamSession::new(request_sender, response_event_stream);
            match session
                .consume(&mut self, &mut periodic_event_interval, &mut log_throttler)
                .await
            {
                SessionExit::Reconnect => continue,
                SessionExit::Shutdown => return,
            }
        }
    }

    fn handle_tick(
        &mut self,
        request_sender: &mpsc::UnboundedSender<SubscribeCompactionEventRequest>,
        pull_task_ack: &mut bool,
        log_throttler: &mut LogThrottler<CompactionLogState>,
    ) -> HandlerAction {
        handle_hummock_tick(
            request_sender,
            self.task_progress.clone(),
            self.running_task_parallelism.load(Ordering::SeqCst),
            self.max_task_parallelism,
            self.max_pull_task_count,
            pull_task_ack,
            log_throttler,
        )
    }

    fn handle_response(
        &self,
        response: SubscribeCompactionEventResponse,
        request_sender: &mpsc::UnboundedSender<SubscribeCompactionEventRequest>,
        pull_task_ack: &mut bool,
    ) -> HandlerAction {
        let Some((action, create_at)) = decode_hummock_response(response, pull_task_ack) else {
            return HandlerAction::Continue;
        };

        self.compactor_context
            .compactor_metrics
            .compaction_event_consumed_latency
            .observe((unix_timestamp_millis() - create_at) as _);

        match action {
            HummockResponseAction::Continue => {}
            HummockResponseAction::CompactTask(compact_task) => {
                self.handle_compact_task(CompactTask::from(compact_task), request_sender);
            }
            HummockResponseAction::ValidationTask(validation_task) => {
                self.spawn_validation_task(ValidationTask::from(validation_task));
            }
            HummockResponseAction::CancelTask(task_id) => {
                self.cancel_task(task_id);
            }
        }
        HandlerAction::Continue
    }

    fn handle_compact_task(
        &self,
        compact_task: CompactTask,
        request_sender: &mpsc::UnboundedSender<SubscribeCompactionEventRequest>,
    ) {
        let parallelism = calculate_task_parallelism(&compact_task, &self.compactor_context);
        assert_ne!(parallelism, 0, "splits cannot be empty");

        if (self.max_task_parallelism - self.running_task_parallelism.load(Ordering::SeqCst))
            < parallelism as u32
        {
            self.report_no_cpu_resource_task(compact_task, request_sender);
            return;
        }

        self.running_task_parallelism
            .fetch_add(parallelism as u32, Ordering::SeqCst);
        let task_id = compact_task.task_id;
        let shutdown_registration = self.shutdown_map.register(task_id);
        if shutdown_registration.replaced_existing {
            tracing::warn!(
                "Replaced existing compaction shutdown handle. task_id: {}",
                task_id
            );
        }
        self.spawn_compact_task(
            compact_task,
            parallelism,
            shutdown_registration.shutdown_rx,
            shutdown_registration.guard,
            request_sender.clone(),
        );
    }

    fn report_no_cpu_resource_task(
        &self,
        compact_task: CompactTask,
        request_sender: &mpsc::UnboundedSender<SubscribeCompactionEventRequest>,
    ) {
        tracing::warn!(
            "Not enough core parallelism to serve the task {} task_parallelism {} running_task_parallelism {} max_task_parallelism {}",
            compact_task.task_id,
            calculate_task_parallelism(&compact_task, &self.compactor_context),
            self.running_task_parallelism.load(Ordering::Relaxed),
            self.max_task_parallelism,
        );
        let (compact_task, table_stats, object_timestamps) = compact_done(
            compact_task,
            self.compactor_context.clone(),
            vec![],
            TaskStatus::NoAvailCpuResourceCanceled,
        );
        send_report_task_event(
            &compact_task,
            table_stats,
            object_timestamps,
            request_sender,
        );
    }

    fn spawn_compact_task(
        &self,
        compact_task: CompactTask,
        parallelism: usize,
        shutdown_rx: Receiver<()>,
        shutdown_guard: TaskShutdownGuard<u64>,
        request_sender: mpsc::UnboundedSender<SubscribeCompactionEventRequest>,
    ) {
        let context = self.compactor_context.clone();
        let object_id_manager = self.object_id_manager.clone();
        let compaction_catalog_manager_ref = self.compaction_catalog_manager_ref.clone();
        let running_task_parallelism = self.running_task_parallelism.clone();
        let executor = self.compactor_context.compaction_executor.clone();

        executor.spawn(async move {
            let ((compact_task, table_stats, object_timestamps), _memory_tracker) = {
                let _shutdown_guard = shutdown_guard;
                let _parallelism_guard = scopeguard::guard(
                    (running_task_parallelism.clone(), parallelism as u32),
                    |(running_task_parallelism, parallelism)| {
                        running_task_parallelism.fetch_sub(parallelism, Ordering::SeqCst);
                    },
                );

                compactor_runner::compact(
                    context.clone(),
                    compact_task,
                    shutdown_rx,
                    object_id_manager,
                    compaction_catalog_manager_ref.clone(),
                )
                .await
            };

            send_report_task_event(
                &compact_task,
                table_stats,
                object_timestamps,
                &request_sender,
            );

            check_compaction_result_if_needed(
                &compact_task,
                context,
                compaction_catalog_manager_ref,
            )
            .await;
        });
    }

    fn spawn_validation_task(&self, validation_task: ValidationTask) {
        let context = self.compactor_context.clone();
        self.compactor_context
            .compaction_executor
            .spawn(
                async move { validate_ssts(validation_task, context.sstable_store.clone()).await },
            );
    }

    fn cancel_task(&self, task_id: u64) {
        match self.shutdown_map.cancel(&task_id) {
            ShutdownCancelResult::Sent => {}
            ShutdownCancelResult::AlreadyFinished => {
                tracing::warn!(
                    "Cancellation of compaction task failed. task_id: {}",
                    task_id
                );
            }
            ShutdownCancelResult::NotFound => {
                tracing::warn!(
                    "Attempting to cancel non-existent compaction task. task_id: {}",
                    task_id
                );
            }
        }
    }
}

fn pull_hummock_tasks_if_needed(
    request_sender: &mpsc::UnboundedSender<SubscribeCompactionEventRequest>,
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

    if let Err(e) = request_sender.send(SubscribeCompactionEventRequest {
        event: Some(RequestEvent::PullTask(PullTask {
            pull_task_count: pull_count,
        })),
        create_at: unix_timestamp_millis(),
    }) {
        tracing::warn!(error = %e.as_report(), "Failed to pull task");
        return None;
    }

    *pull_task_ack = false;
    Some(pull_count)
}

fn handle_hummock_tick(
    request_sender: &mpsc::UnboundedSender<SubscribeCompactionEventRequest>,
    task_progress: TaskProgressMap,
    running_parallelism: u32,
    max_task_parallelism: u32,
    max_pull_task_count: u32,
    pull_task_ack: &mut bool,
    log_throttler: &mut LogThrottler<CompactionLogState>,
) -> HandlerAction {
    if !send_hummock_heartbeat(request_sender, task_progress) {
        return HandlerAction::Reconnect;
    }

    let pending_pull_task_count = pull_hummock_tasks_if_needed(
        request_sender,
        running_parallelism,
        max_task_parallelism,
        max_pull_task_count,
        pull_task_ack,
    );
    let Some(pending_pull_task_count) = pending_pull_task_count else {
        return HandlerAction::Reconnect;
    };

    log_hummock_state(
        running_parallelism,
        *pull_task_ack,
        pending_pull_task_count,
        log_throttler,
    );
    HandlerAction::Continue
}

fn send_hummock_heartbeat(
    request_sender: &mpsc::UnboundedSender<SubscribeCompactionEventRequest>,
    task_progress: TaskProgressMap,
) -> bool {
    let progress_list = get_task_progress(task_progress);
    if let Err(e) = request_sender.send(SubscribeCompactionEventRequest {
        event: Some(RequestEvent::HeartBeat(HeartBeat {
            progress: progress_list,
        })),
        create_at: unix_timestamp_millis(),
    }) {
        tracing::warn!(error = %e.as_report(), "Failed to report task progress");
        return false;
    }
    true
}

fn log_hummock_state(
    running_parallelism: u32,
    pull_task_ack: bool,
    pending_pull_task_count: u32,
    log_throttler: &mut LogThrottler<CompactionLogState>,
) {
    let current_state = CompactionLogState {
        running_parallelism,
        pull_task_ack,
        pending_pull_task_count,
    };

    if log_throttler.should_log(&current_state) {
        tracing::info!(
            running_parallelism_count = %current_state.running_parallelism,
            pull_task_ack = %current_state.pull_task_ack,
            pending_pull_task_count = %current_state.pending_pull_task_count
        );
        log_throttler.update(current_state);
    }
}

fn send_report_task_event(
    compact_task: &CompactTask,
    table_stats: TableStatsMap,
    object_timestamps: HashMap<HummockSstableObjectId, u64>,
    request_sender: &mpsc::UnboundedSender<SubscribeCompactionEventRequest>,
) {
    if let Err(e) = request_sender.send(SubscribeCompactionEventRequest {
        event: Some(RequestEvent::ReportTask(ReportTask {
            task_id: compact_task.task_id,
            task_status: compact_task.task_status.into(),
            sorted_output_ssts: compact_task
                .sorted_output_ssts
                .iter()
                .map(|sst| sst.into())
                .collect(),
            table_stats_change: to_prost_table_stats_map(table_stats),
            object_timestamps,
        })),
        create_at: unix_timestamp_millis(),
    }) {
        let task_id = compact_task.task_id;
        tracing::warn!(error = %e.as_report(), "Failed to report task {task_id:?}");
    }
}

async fn check_compaction_result_if_needed(
    compact_task: &CompactTask,
    context: CompactorContext,
    compaction_catalog_manager_ref: CompactionCatalogManagerRef,
) {
    let enable_check_compaction_result = context.storage_opts.check_compaction_result;
    let need_check_task = !compact_task.sorted_output_ssts.is_empty()
        && compact_task.task_status == TaskStatus::Success;
    if !enable_check_compaction_result || !need_check_task {
        return;
    }

    let read_table_ids = compact_task
        .get_table_ids_from_input_ssts()
        .collect::<Vec<_>>();
    match compaction_catalog_manager_ref
        .acquire(read_table_ids.into_iter().collect())
        .await
    {
        Ok(compaction_catalog_agent_ref) => {
            match check_compaction_result(compact_task, context, compaction_catalog_agent_ref).await
            {
                Err(e) => {
                    tracing::warn!(error = %e.as_report(), "Failed to check compaction task {}", compact_task.task_id);
                }
                Ok(true) => (),
                Ok(false) => {
                    panic!(
                        "Failed to pass consistency check for result of compaction task:\n{:?}",
                        compact_task_to_string(compact_task)
                    );
                }
            }
        }
        Err(e) => {
            tracing::warn!(error = %e.as_report(), "failed to acquire compaction catalog agent");
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn expect_pull_task(
        rx: &mut mpsc::UnboundedReceiver<SubscribeCompactionEventRequest>,
        expected_pull_count: u32,
    ) {
        let request = rx.try_recv().expect("pull task request should be sent");
        let Some(RequestEvent::PullTask(pull_task)) = request.event else {
            panic!("expected pull task request");
        };
        assert_eq!(pull_task.pull_task_count, expected_pull_count);
    }

    fn expect_heartbeat(rx: &mut mpsc::UnboundedReceiver<SubscribeCompactionEventRequest>) {
        let request = rx.try_recv().expect("heartbeat request should be sent");
        let Some(RequestEvent::HeartBeat(heartbeat)) = request.event else {
            panic!("expected heartbeat request");
        };
        assert!(heartbeat.progress.is_empty());
    }

    fn empty_task_progress() -> TaskProgressMap {
        Arc::new(parking_lot::Mutex::new(HashMap::new()))
    }

    #[test]
    fn test_hummock_tick_sends_heartbeat_then_pull_and_waits_for_ack() {
        let (tx, mut rx) = mpsc::unbounded_channel();
        let mut pull_task_ack = true;
        let mut log_throttler =
            LogThrottler::<CompactionLogState>::new(COMPACTION_HEARTBEAT_LOG_INTERVAL);

        let action = handle_hummock_tick(
            &tx,
            empty_task_progress(),
            2,
            8,
            4,
            &mut pull_task_ack,
            &mut log_throttler,
        );

        assert!(matches!(action, HandlerAction::Continue));
        assert!(!pull_task_ack);
        expect_heartbeat(&mut rx);
        expect_pull_task(&mut rx, 4);
        assert!(rx.try_recv().is_err());
    }

    #[test]
    fn test_hummock_tick_reconnects_when_heartbeat_send_fails() {
        let (tx, rx) = mpsc::unbounded_channel();
        drop(rx);
        let mut pull_task_ack = true;
        let mut log_throttler =
            LogThrottler::<CompactionLogState>::new(COMPACTION_HEARTBEAT_LOG_INTERVAL);

        let action = handle_hummock_tick(
            &tx,
            empty_task_progress(),
            0,
            8,
            4,
            &mut pull_task_ack,
            &mut log_throttler,
        );

        assert!(matches!(action, HandlerAction::Reconnect));
        assert!(pull_task_ack);
    }

    #[test]
    fn test_hummock_response_ignores_empty_event() {
        let mut pull_task_ack = false;

        let action = decode_hummock_response(
            SubscribeCompactionEventResponse {
                event: None,
                create_at: 233,
            },
            &mut pull_task_ack,
        );

        assert!(action.is_none());
        assert!(!pull_task_ack);
    }

    #[test]
    fn test_hummock_response_event_sets_pull_task_ack() {
        let mut pull_task_ack = false;

        let action = handle_hummock_response_event(
            ResponseEvent::PullTaskAck(
                risingwave_pb::hummock::subscribe_compaction_event_response::PullTaskAck {},
            ),
            &mut pull_task_ack,
        );

        assert!(matches!(action, HummockResponseAction::Continue));
        assert!(pull_task_ack);
    }

    #[test]
    fn test_hummock_response_event_forwards_compact_task() {
        let mut pull_task_ack = false;

        let action = handle_hummock_response_event(
            ResponseEvent::CompactTask(risingwave_pb::hummock::CompactTask {
                task_id: 7,
                ..Default::default()
            }),
            &mut pull_task_ack,
        );

        let HummockResponseAction::CompactTask(task) = action else {
            panic!("expected compact task action");
        };
        assert_eq!(task.task_id, 7);
        assert!(!pull_task_ack);
    }

    #[test]
    #[expect(deprecated)]
    fn test_hummock_response_event_forwards_validation_task() {
        let mut pull_task_ack = false;

        let action = handle_hummock_response_event(
            ResponseEvent::ValidationTask(risingwave_pb::hummock::ValidationTask::default()),
            &mut pull_task_ack,
        );

        assert!(matches!(action, HummockResponseAction::ValidationTask(_)));
        assert!(!pull_task_ack);
    }

    #[test]
    fn test_hummock_response_event_forwards_cancel_task() {
        let mut pull_task_ack = false;

        let action = handle_hummock_response_event(
            ResponseEvent::CancelCompactTask(risingwave_pb::hummock::CancelCompactTask {
                task_id: 7,
                ..Default::default()
            }),
            &mut pull_task_ack,
        );

        let HummockResponseAction::CancelTask(task_id) = action else {
            panic!("expected cancel task action");
        };
        assert_eq!(task_id, 7);
        assert!(!pull_task_ack);
    }

    #[test]
    fn test_pull_hummock_tasks_sends_request_and_waits_for_ack() {
        let (tx, mut rx) = mpsc::unbounded_channel();
        let mut pull_task_ack = true;

        let result = pull_hummock_tasks_if_needed(&tx, 2, 8, 4, &mut pull_task_ack);

        assert_eq!(result, Some(4));
        assert!(!pull_task_ack);
        expect_pull_task(&mut rx, 4);
    }

    #[test]
    fn test_pull_hummock_tasks_skips_when_ack_is_pending() {
        let (tx, mut rx) = mpsc::unbounded_channel();
        let mut pull_task_ack = false;

        let result = pull_hummock_tasks_if_needed(&tx, 0, 8, 4, &mut pull_task_ack);

        assert_eq!(result, Some(0));
        assert!(!pull_task_ack);
        assert!(rx.try_recv().is_err());
    }

    #[test]
    fn test_pull_hummock_tasks_skips_when_parallelism_is_full() {
        let (tx, mut rx) = mpsc::unbounded_channel();
        let mut pull_task_ack = true;

        let result = pull_hummock_tasks_if_needed(&tx, 8, 8, 4, &mut pull_task_ack);

        assert_eq!(result, Some(0));
        assert!(pull_task_ack);
        assert!(rx.try_recv().is_err());
    }

    #[test]
    fn test_pull_hummock_tasks_reconnects_on_send_failure() {
        let (tx, rx) = mpsc::unbounded_channel();
        drop(rx);
        let mut pull_task_ack = true;

        let result = pull_hummock_tasks_if_needed(&tx, 0, 8, 4, &mut pull_task_ack);

        assert_eq!(result, None);
        assert!(pull_task_ack);
    }
}
