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

use std::collections::{HashMap, VecDeque};
use std::sync::Arc;
use std::time::Duration;

use risingwave_hummock_sdk::compact_task::{CompactTask, ValidationTask};
use risingwave_hummock_sdk::table_stats::to_prost_table_stats_map;
use risingwave_hummock_sdk::{HummockSstableObjectId, compact_task_to_string};
use risingwave_pb::compactor::{DispatchCompactionTaskRequest, dispatch_compaction_task_request};
use risingwave_pb::hummock::compact_task::TaskStatus;
use risingwave_pb::hummock::report_compaction_task_request::{
    Event as ReportCompactionTaskEvent, HeartBeat as SharedHeartBeat,
    ReportTask as ReportSharedTask,
};
use risingwave_pb::hummock::{PbCompactTask, ReportCompactionTaskRequest};
use risingwave_rpc_client::GrpcCompactorProxyClient;
use thiserror_ext::AsReport;
use tokio::sync::mpsc;
use tokio::sync::oneshot::{Receiver, Sender};
use tokio::task::JoinHandle;
use tonic::Request;

use super::compaction_utils::check_compaction_result;
use super::event_loop_utils::{
    ShutdownCancelResult, TaskShutdownGuard, TaskShutdownRegistry, get_task_progress,
};
use super::{CompactorContext, compactor_runner};
use crate::compaction_catalog_manager::CompactionCatalogManager;
use crate::hummock::{SharedComapctorObjectIdManager, validate_ssts};

type CompactionShutdownRegistry = TaskShutdownRegistry<u64>;

const PERIODIC_EVENT_UPDATE_INTERVAL: Duration = Duration::from_millis(1000);

enum SharedDispatchAction {
    Compact(risingwave_pb::hummock::CompactTask),
    Validation(risingwave_pb::hummock::ValidationTask),
    Cancel(u64),
}

fn handle_shared_dispatch_task(
    task: dispatch_compaction_task_request::Task,
) -> SharedDispatchAction {
    match task {
        dispatch_compaction_task_request::Task::CompactTask(compact_task) => {
            SharedDispatchAction::Compact(compact_task)
        }
        dispatch_compaction_task_request::Task::VacuumTask(_) => {
            unreachable!("unexpected vacuum task");
        }
        dispatch_compaction_task_request::Task::FullScanTask(_) => {
            unreachable!("unexpected scan task");
        }
        dispatch_compaction_task_request::Task::ValidationTask(validation_task) => {
            SharedDispatchAction::Validation(validation_task)
        }
        dispatch_compaction_task_request::Task::CancelCompactTask(cancel_compact_task) => {
            SharedDispatchAction::Cancel(cancel_compact_task.task_id)
        }
    }
}

#[must_use]
pub fn start_shared_compactor(
    grpc_proxy_client: GrpcCompactorProxyClient,
    receiver: mpsc::UnboundedReceiver<Request<DispatchCompactionTaskRequest>>,
    context: CompactorContext,
) -> (JoinHandle<()>, Sender<()>) {
    let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel();
    let runner = SharedCompactorEventLoop {
        task_progress: context.task_progress_manager.clone(),
        grpc_proxy_client,
        receiver,
        context,
        shutdown_rx,
        shutdown_map: CompactionShutdownRegistry::default(),
    };

    let join_handle = tokio::spawn(async move { runner.run().await });
    (join_handle, shutdown_tx)
}

struct SharedCompactorEventLoop {
    grpc_proxy_client: GrpcCompactorProxyClient,
    receiver: mpsc::UnboundedReceiver<Request<DispatchCompactionTaskRequest>>,
    context: CompactorContext,
    task_progress: Arc<
        parking_lot::lock_api::Mutex<parking_lot::RawMutex, HashMap<u64, Arc<super::TaskProgress>>>,
    >,
    shutdown_rx: Receiver<()>,
    shutdown_map: CompactionShutdownRegistry,
}

impl SharedCompactorEventLoop {
    async fn run(mut self) {
        let mut periodic_event_interval = tokio::time::interval(PERIODIC_EVENT_UPDATE_INTERVAL);

        loop {
            let request = tokio::select! {
                _ = periodic_event_interval.tick() => {
                    self.report_heartbeat().await;
                    continue;
                }
                _ = &mut self.shutdown_rx => {
                    tracing::info!("Compactor is shutting down");
                    return;
                }
                request = self.receiver.recv() => {
                    request
                }
            };

            let Some(request) = request else {
                continue;
            };
            self.handle_request(request);
        }
    }

    async fn report_heartbeat(&self) {
        let progress_list = get_task_progress(self.task_progress.clone());
        let report_compaction_task_request = ReportCompactionTaskRequest {
            event: Some(ReportCompactionTaskEvent::HeartBeat(SharedHeartBeat {
                progress: progress_list,
            })),
        };
        if let Err(e) = self
            .grpc_proxy_client
            .report_compaction_task(report_compaction_task_request)
            .await
        {
            tracing::warn!(error = %e.as_report(), "Failed to report heartbeat");
        }
    }

    fn handle_request(&self, request: Request<DispatchCompactionTaskRequest>) {
        let mut request = request.into_inner();
        let task = request
            .task
            .take()
            .expect("dispatch compaction task should exist");

        match handle_shared_dispatch_task(task) {
            SharedDispatchAction::Compact(compact_task) => {
                self.spawn_compact_task(compact_task, request);
            }
            SharedDispatchAction::Validation(validation_task) => {
                self.spawn_validation_task(ValidationTask::from(validation_task));
            }
            SharedDispatchAction::Cancel(task_id) => {
                self.cancel_task(task_id);
            }
        }
    }

    fn spawn_compact_task(
        &self,
        compact_task: risingwave_pb::hummock::CompactTask,
        request: DispatchCompactionTaskRequest,
    ) {
        let compact_task = CompactTask::from(&compact_task);
        let task_id = compact_task.task_id;
        let shutdown_registration = self.shutdown_map.register(task_id);
        if shutdown_registration.replaced_existing {
            tracing::warn!(
                "Replaced existing shared compaction shutdown handle. task_id: {}",
                task_id
            );
        }

        let context = self.context.clone();
        let grpc_proxy_client = self.grpc_proxy_client.clone();
        let executor = self.context.compaction_executor.clone();
        let shutdown_rx = shutdown_registration.shutdown_rx;
        let shutdown_guard = shutdown_registration.guard;

        executor.spawn(async move {
            let DispatchCompactionTaskRequest {
                tables,
                output_object_ids,
                task: _,
            } = request;
            let table_id_to_catalog = tables.into_iter().fold(HashMap::new(), |mut acc, table| {
                acc.insert(table.id, table);
                acc
            });
            let mut output_object_ids_deque = VecDeque::new();
            output_object_ids_deque.extend(
                output_object_ids
                    .into_iter()
                    .map(Into::<HummockSstableObjectId>::into),
            );
            let shared_compactor_object_id_manager = SharedComapctorObjectIdManager::new(
                output_object_ids_deque,
                grpc_proxy_client.clone(),
                context.storage_opts.sstable_id_remote_fetch_number,
            );

            let compaction_catalog_agent_ref =
                CompactionCatalogManager::build_compaction_catalog_agent(table_id_to_catalog);
            let ((compact_task, table_stats, object_timestamps), _memory_tracker) = {
                let _shutdown_guard: TaskShutdownGuard<u64> = shutdown_guard;

                compactor_runner::compact_with_agent(
                    context.clone(),
                    compact_task,
                    shutdown_rx,
                    shared_compactor_object_id_manager,
                    compaction_catalog_agent_ref.clone(),
                )
                .await
            };

            let report_compaction_task_request = ReportCompactionTaskRequest {
                event: Some(ReportCompactionTaskEvent::ReportTask(ReportSharedTask {
                    compact_task: Some(PbCompactTask::from(&compact_task)),
                    table_stats_change: to_prost_table_stats_map(table_stats),
                    object_timestamps,
                })),
            };

            match grpc_proxy_client
                .report_compaction_task(report_compaction_task_request)
                .await
            {
                Ok(_) => {
                    check_compaction_result_if_needed(
                        &compact_task,
                        context,
                        compaction_catalog_agent_ref,
                    )
                    .await;
                }
                Err(e) => {
                    tracing::warn!(error = %e.as_report(), "Failed to report task {task_id:?}")
                }
            }
        });
    }

    fn spawn_validation_task(&self, validation_task: ValidationTask) {
        let context = self.context.clone();
        self.context.compaction_executor.spawn(async move {
            validate_ssts(validation_task, context.sstable_store.clone()).await
        });
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

async fn check_compaction_result_if_needed(
    compact_task: &CompactTask,
    context: CompactorContext,
    compaction_catalog_agent_ref: crate::compaction_catalog_manager::CompactionCatalogAgentRef,
) {
    let enable_check_compaction_result = context.storage_opts.check_compaction_result;
    let need_check_task = !compact_task.sorted_output_ssts.is_empty()
        && compact_task.task_status == TaskStatus::Success;
    if !enable_check_compaction_result || !need_check_task {
        return;
    }

    match check_compaction_result(compact_task, context, compaction_catalog_agent_ref).await {
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_shared_dispatch_forwards_compact_task() {
        let action =
            handle_shared_dispatch_task(dispatch_compaction_task_request::Task::CompactTask(
                risingwave_pb::hummock::CompactTask {
                    task_id: 7,
                    ..Default::default()
                },
            ));

        let SharedDispatchAction::Compact(task) = action else {
            panic!("expected compact task action");
        };
        assert_eq!(task.task_id, 7);
    }

    #[test]
    fn test_shared_dispatch_forwards_validation_task() {
        let action =
            handle_shared_dispatch_task(dispatch_compaction_task_request::Task::ValidationTask(
                risingwave_pb::hummock::ValidationTask::default(),
            ));

        assert!(matches!(action, SharedDispatchAction::Validation(_)));
    }

    #[test]
    fn test_shared_dispatch_forwards_cancel_task() {
        let action =
            handle_shared_dispatch_task(dispatch_compaction_task_request::Task::CancelCompactTask(
                risingwave_pb::hummock::CancelCompactTask {
                    task_id: 7,
                    ..Default::default()
                },
            ));

        let SharedDispatchAction::Cancel(task_id) = action else {
            panic!("expected cancel task action");
        };
        assert_eq!(task_id, 7);
    }
}
