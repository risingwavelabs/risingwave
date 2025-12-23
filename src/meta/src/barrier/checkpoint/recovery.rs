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

use std::collections::{HashMap, HashSet};
use std::mem::{replace, take};
use std::task::{Context, Poll};

use futures::FutureExt;
use itertools::Itertools;
use prometheus::{HistogramTimer, IntCounter};
use risingwave_common::catalog::DatabaseId;
use risingwave_common::id::JobId;
use risingwave_meta_model::WorkerId;
use risingwave_pb::meta::event_log::{Event, EventRecovery};
use risingwave_pb::stream_service::BarrierCompleteResponse;
use risingwave_pb::stream_service::streaming_control_stream_response::ResetPartialGraphResponse;
use thiserror_ext::AsReport;
use tracing::{info, warn};

use crate::barrier::DatabaseRuntimeInfoSnapshot;
use crate::barrier::checkpoint::control::DatabaseCheckpointControlStatus;
use crate::barrier::checkpoint::creating_job::CreatingStreamingJobControl;
use crate::barrier::checkpoint::{BarrierWorkerState, CheckpointControl};
use crate::barrier::complete_task::BarrierCompleteOutput;
use crate::barrier::rpc::{
    ControlStreamManager, DatabaseInitialBarrierCollector, from_partial_graph_id,
};
use crate::barrier::worker::{
    RetryBackoffFuture, RetryBackoffStrategy, get_retry_backoff_strategy,
};
use crate::rpc::metrics::GLOBAL_META_METRICS;
use crate::{MetaError, MetaResult};

#[derive(Default)]
pub(super) struct ResetPartialGraphCollector {
    pub(super) reset_request_id: u32,
    pub(super) remaining_workers: HashSet<WorkerId>,
    pub(super) reset_resps: HashMap<WorkerId, ResetPartialGraphResponse>,
}

impl ResetPartialGraphCollector {
    pub(super) fn collect(&mut self, worker_id: WorkerId, resp: ResetPartialGraphResponse) {
        if resp.reset_request_id < self.reset_request_id {
            info!(
                partial_graph_id = %resp.partial_graph_id,
                %worker_id,
                received_request_id = resp.reset_request_id,
                ongoing_request_id = self.reset_request_id,
                "ignore stale reset response"
            );
        }
        assert_eq!(resp.reset_request_id, self.reset_request_id);
        assert!(self.remaining_workers.remove(&worker_id));
        self.reset_resps
            .try_insert(worker_id, resp)
            .expect("non-duplicate");
    }
}

/// We can treat each database as a state machine of 3 states: `Running`, `Resetting` and `Initializing`.
/// The state transition can be triggered when receiving 3 variants of response: `ReportDatabaseFailure`, `BarrierComplete`, `DatabaseReset`.
/// The logic of state transition can be summarized as followed:
///
/// `Running`
///     - on `ReportDatabaseFailure`
///         - wait for the inflight B`arrierCompletingTask` to finish if there is any, mark the database as blocked in command queue
///         - send `ResetDatabaseRequest` with `reset_request_id` as 0 to all CNs, and save `reset_request_id` and the set of nodes that need to collect response.
///         - enter `Resetting` state.
///     - on `BarrierComplete`: update the `DatabaseCheckpointControl`.
///     - on `DatabaseReset`: unreachable
/// `Resetting`
///     - on `ReportDatabaseFailure` or `BarrierComplete`: ignore
///     - on `DatabaseReset`:
///         - if the `reset_request_id` in the response is less than the saved `reset_request_id`, ignore
///         - otherwise, mark the CN as collected.
///         - when all CNs have collected the response:
///             - load the database runtime info from catalog manager and fragment manager
///             - inject the initial barrier to CNs, save the set of nodes that need to collect response
///             - if any failure happened, re-enter `Resetting` state.
///             - enter `Initializing` state
/// `Initializing`
///     - on `BarrierComplete`:
///         - mark the CN as collected
///         - when all CNs have collected the response: enter Running
///     - on `ReportDatabaseFailure`
///         - increment the previously saved `reset_request_id`, and send `ResetDatabaseRequest` to all CNs
///         - enter `Resetting`
///     - on `DatabaseReset`: unreachable
enum DatabaseRecoveringStage {
    Resetting {
        database_resp_collector: ResetPartialGraphCollector,
        creating_job_collectors: HashMap<JobId, ResetPartialGraphCollector>,
        backoff_future: Option<RetryBackoffFuture>,
    },
    Initializing {
        initial_barrier_collector: Box<DatabaseInitialBarrierCollector>,
    },
}

pub(crate) struct DatabaseRecoveringState {
    stage: DatabaseRecoveringStage,
    next_reset_request_id: u32,
    retry_backoff_strategy: RetryBackoffStrategy,
    metrics: DatabaseRecoveryMetrics,
}

pub(super) enum RecoveringStateAction {
    EnterInitializing(Vec<(WorkerId, ResetPartialGraphResponse)>),
    EnterRunning,
}

struct DatabaseRecoveryMetrics {
    recovery_failure_cnt: IntCounter,
    recovery_timer: Option<HistogramTimer>,
}

impl DatabaseRecoveryMetrics {
    fn new(database_id: DatabaseId) -> Self {
        let database_id_str = format!("database {}", database_id);
        Self {
            recovery_failure_cnt: GLOBAL_META_METRICS
                .recovery_failure_cnt
                .with_label_values(&[database_id_str.as_str()]),
            recovery_timer: Some(
                GLOBAL_META_METRICS
                    .recovery_latency
                    .with_label_values(&[database_id_str.as_str()])
                    .start_timer(),
            ),
        }
    }
}

pub(super) const INITIAL_RESET_REQUEST_ID: u32 = 0;

impl DatabaseRecoveringState {
    pub(super) fn reset_partial_graph(
        database_id: DatabaseId,
        creating_job_id: Option<JobId>,
        clear_tables: bool,
        control_stream_manager: &mut ControlStreamManager,
        reset_request_id: u32,
    ) -> ResetPartialGraphCollector {
        let remaining_workers = control_stream_manager.reset_partial_graph(
            database_id,
            creating_job_id,
            clear_tables,
            reset_request_id,
        );
        ResetPartialGraphCollector {
            reset_request_id,
            remaining_workers,
            reset_resps: Default::default(),
        }
    }

    /// reset the partial graphs of database again in recovery state
    /// should not call it when the database is running, because when database is running
    /// we cannot determine whether to `clear_tables` or not.
    fn reset_database_partial_graphs_again(
        database_id: DatabaseId,
        creating_jobs: impl Iterator<Item = JobId>,
        control_stream_manager: &mut ControlStreamManager,
        reset_request_id: u32,
    ) -> (
        ResetPartialGraphCollector,
        HashMap<JobId, ResetPartialGraphCollector>,
    ) {
        (
            Self::reset_partial_graph(
                database_id,
                None,
                true,
                control_stream_manager,
                reset_request_id,
            ),
            creating_jobs
                .map(|job_id| {
                    (
                        job_id,
                        Self::reset_partial_graph(
                            database_id,
                            Some(job_id),
                            true,
                            control_stream_manager,
                            reset_request_id,
                        ),
                    )
                })
                .collect(),
        )
    }

    pub(super) fn new_resetting(
        database_id: DatabaseId,
        creating_jobs: impl Iterator<Item = JobId>,
        control_stream_manager: &mut ControlStreamManager,
    ) -> Self {
        let mut retry_backoff_strategy = get_retry_backoff_strategy();
        let backoff_future = retry_backoff_strategy.next().unwrap();
        let metrics = DatabaseRecoveryMetrics::new(database_id);
        metrics.recovery_failure_cnt.inc();
        let (database_resp_collector, creating_job_collectors) =
            Self::reset_database_partial_graphs_again(
                database_id,
                creating_jobs,
                control_stream_manager,
                INITIAL_RESET_REQUEST_ID,
            );
        Self {
            stage: DatabaseRecoveringStage::Resetting {
                database_resp_collector,
                creating_job_collectors,
                backoff_future: Some(backoff_future),
            },
            next_reset_request_id: INITIAL_RESET_REQUEST_ID + 1,
            retry_backoff_strategy,
            metrics,
        }
    }

    fn next_retry(&mut self) -> (RetryBackoffFuture, u32) {
        let backoff_future = self
            .retry_backoff_strategy
            .next()
            .expect("should not be empty");
        let request_id = self.next_reset_request_id;
        self.next_reset_request_id += 1;
        (backoff_future, request_id)
    }

    pub(super) fn barrier_collected(
        &mut self,
        database_id: DatabaseId,
        resp: BarrierCompleteResponse,
    ) {
        match &mut self.stage {
            DatabaseRecoveringStage::Resetting { .. } => {
                // ignore the collected barrier on resetting or backoff
            }
            DatabaseRecoveringStage::Initializing {
                initial_barrier_collector,
            } => {
                let worker_id = resp.worker_id;
                initial_barrier_collector.collect_resp(resp);
                info!(
                    ?database_id,
                    %worker_id,
                    remaining_workers = ?initial_barrier_collector,
                    "initializing database barrier collected"
                );
            }
        }
    }

    pub(super) fn is_valid_after_worker_err(&mut self, worker_id: WorkerId) -> bool {
        match &mut self.stage {
            DatabaseRecoveringStage::Resetting {
                database_resp_collector,
                creating_job_collectors,
                ..
            } => {
                for collector in creating_job_collectors
                    .values_mut()
                    .chain([database_resp_collector])
                {
                    collector.remaining_workers.remove(&worker_id);
                }
                true
            }
            DatabaseRecoveringStage::Initializing {
                initial_barrier_collector,
                ..
            } => initial_barrier_collector.is_valid_after_worker_err(worker_id),
        }
    }

    pub(super) fn on_reset_partial_graph_resp(
        &mut self,
        worker_id: WorkerId,
        resp: ResetPartialGraphResponse,
    ) {
        let (database_id, creating_job_id) = from_partial_graph_id(resp.partial_graph_id);
        match &mut self.stage {
            DatabaseRecoveringStage::Resetting {
                database_resp_collector,
                creating_job_collectors,
                ..
            } => {
                let collector = if let Some(creating_job_id) = creating_job_id {
                    let Some(collector) = creating_job_collectors.get_mut(&creating_job_id) else {
                        if cfg!(debug_assertions) {
                            panic!(
                                "receive reset partial graph resp on non-existing creating job: {resp:?}"
                            )
                        }
                        warn!(
                            %database_id,
                            %creating_job_id,
                            %worker_id,
                            ?resp,
                            "ignore reset partial graph resp on non-existing creating job"
                        );
                        return;
                    };
                    collector
                } else {
                    database_resp_collector
                };
                collector.collect(worker_id, resp);
            }
            DatabaseRecoveringStage::Initializing { .. } => {
                unreachable!("all reset resp should have been received in Resetting")
            }
        }
    }

    pub(super) fn poll_next_event(&mut self, cx: &mut Context<'_>) -> Poll<RecoveringStateAction> {
        match &mut self.stage {
            DatabaseRecoveringStage::Resetting {
                database_resp_collector,
                creating_job_collectors,
                backoff_future: backoff_future_option,
                ..
            } => {
                let pass_backoff = if let Some(backoff_future) = backoff_future_option {
                    if backoff_future.poll_unpin(cx).is_ready() {
                        *backoff_future_option = None;
                        true
                    } else {
                        false
                    }
                } else {
                    true
                };
                if pass_backoff
                    && database_resp_collector.remaining_workers.is_empty()
                    && creating_job_collectors
                        .values()
                        .all(|collector| collector.remaining_workers.is_empty())
                {
                    return Poll::Ready(RecoveringStateAction::EnterInitializing(
                        creating_job_collectors
                            .values_mut()
                            .chain([database_resp_collector])
                            .flat_map(|collector| take(&mut collector.reset_resps))
                            .collect(),
                    ));
                }
            }
            DatabaseRecoveringStage::Initializing {
                initial_barrier_collector,
                ..
            } => {
                if initial_barrier_collector.is_collected() {
                    return Poll::Ready(RecoveringStateAction::EnterRunning);
                }
            }
        }
        Poll::Pending
    }

    pub(super) fn database_state(
        &self,
    ) -> Option<(
        &BarrierWorkerState,
        &HashMap<JobId, CreatingStreamingJobControl>,
    )> {
        match &self.stage {
            DatabaseRecoveringStage::Resetting { .. } => None,
            DatabaseRecoveringStage::Initializing {
                initial_barrier_collector,
                ..
            } => Some(initial_barrier_collector.database_state()),
        }
    }
}

pub(crate) struct DatabaseStatusAction<'a, A> {
    control: &'a mut CheckpointControl,
    database_id: DatabaseId,
    pub(crate) action: A,
}

impl<A> DatabaseStatusAction<'_, A> {
    pub(crate) fn database_id(&self) -> DatabaseId {
        self.database_id
    }
}

impl CheckpointControl {
    pub(super) fn new_database_status_action<A>(
        &mut self,
        database_id: DatabaseId,
        action: A,
    ) -> DatabaseStatusAction<'_, A> {
        DatabaseStatusAction {
            control: self,
            database_id,
            action,
        }
    }
}

pub(crate) struct EnterReset;

impl DatabaseStatusAction<'_, EnterReset> {
    pub(crate) fn enter(
        self,
        barrier_complete_output: Option<BarrierCompleteOutput>,
        control_stream_manager: &mut ControlStreamManager,
    ) {
        let event_log_manager_ref = self.control.env.event_log_manager_ref();
        if let Some(output) = barrier_complete_output {
            self.control.ack_completed(output);
        }
        let database_status = self
            .control
            .databases
            .get_mut(&self.database_id)
            .expect("should exist");
        match database_status {
            DatabaseCheckpointControlStatus::Running(database) => {
                let database_resp_collector = DatabaseRecoveringState::reset_partial_graph(
                    self.database_id,
                    None,
                    true,
                    control_stream_manager,
                    INITIAL_RESET_REQUEST_ID,
                );
                let creating_job_collectors: HashMap<_, _> = database
                    .creating_streaming_job_controls
                    .drain()
                    .map(|(job_id, job)| {
                        let collector = job.reset(control_stream_manager);
                        (job_id, collector)
                    })
                    .collect();
                let next_reset_request_id = creating_job_collectors
                    .values()
                    .map(|collector| collector.reset_request_id)
                    .max()
                    .unwrap_or(database_resp_collector.reset_request_id)
                    + 1;
                let metrics = DatabaseRecoveryMetrics::new(self.database_id);
                event_log_manager_ref.add_event_logs(vec![Event::Recovery(
                    EventRecovery::database_recovery_start(self.database_id.as_raw_id()),
                )]);
                *database_status =
                    DatabaseCheckpointControlStatus::Recovering(DatabaseRecoveringState {
                        stage: DatabaseRecoveringStage::Resetting {
                            database_resp_collector,
                            creating_job_collectors,
                            backoff_future: None,
                        },
                        next_reset_request_id,
                        retry_backoff_strategy: get_retry_backoff_strategy(),
                        metrics,
                    });
            }
            DatabaseCheckpointControlStatus::Recovering(state) => match &mut state.stage {
                DatabaseRecoveringStage::Resetting { .. } => {
                    unreachable!("should not enter resetting again")
                }
                DatabaseRecoveringStage::Initializing {
                    initial_barrier_collector,
                } => {
                    let creating_jobs = initial_barrier_collector.creating_job_ids().collect_vec();
                    event_log_manager_ref.add_event_logs(vec![Event::Recovery(
                        EventRecovery::database_recovery_failure(self.database_id.as_raw_id()),
                    )]);
                    let (backoff_future, reset_request_id) = state.next_retry();
                    let (database_resp_collector, creating_job_collectors) =
                        DatabaseRecoveringState::reset_database_partial_graphs_again(
                            self.database_id,
                            creating_jobs.into_iter(),
                            control_stream_manager,
                            reset_request_id,
                        );
                    state.metrics.recovery_failure_cnt.inc();
                    state.stage = DatabaseRecoveringStage::Resetting {
                        database_resp_collector,
                        creating_job_collectors,
                        backoff_future: Some(backoff_future),
                    };
                }
            },
        }
    }
}

impl CheckpointControl {
    pub(crate) fn on_report_failure(
        &mut self,
        database_id: DatabaseId,
        control_stream_manager: &mut ControlStreamManager,
    ) -> Option<DatabaseStatusAction<'_, EnterReset>> {
        let database_status = self.databases.get_mut(&database_id).expect("should exist");
        match database_status {
            DatabaseCheckpointControlStatus::Running(_) => {
                Some(self.new_database_status_action(database_id, EnterReset))
            }
            DatabaseCheckpointControlStatus::Recovering(state) => match &mut state.stage {
                DatabaseRecoveringStage::Resetting { .. } => {
                    // ignore reported failure during resetting or backoff.
                    None
                }
                DatabaseRecoveringStage::Initializing {
                    initial_barrier_collector,
                } => {
                    warn!(database_id = %database_id, "failed to initialize database");
                    let creating_jobs = initial_barrier_collector.creating_job_ids().collect_vec();
                    let (backoff_future, reset_request_id) = state.next_retry();
                    let (database_resp_collector, creating_job_collectors) =
                        DatabaseRecoveringState::reset_database_partial_graphs_again(
                            database_id,
                            creating_jobs.into_iter(),
                            control_stream_manager,
                            reset_request_id,
                        );
                    state.metrics.recovery_failure_cnt.inc();
                    state.stage = DatabaseRecoveringStage::Resetting {
                        database_resp_collector,
                        creating_job_collectors,
                        backoff_future: Some(backoff_future),
                    };
                    None
                }
            },
        }
    }
}

pub(crate) struct EnterInitializing(pub(crate) Vec<(WorkerId, ResetPartialGraphResponse)>);

impl DatabaseStatusAction<'_, EnterInitializing> {
    pub(crate) fn enter(
        self,
        runtime_info: DatabaseRuntimeInfoSnapshot,
        control_stream_manager: &mut ControlStreamManager,
    ) {
        let database_status = self
            .control
            .databases
            .get_mut(&self.database_id)
            .expect("should exist");
        let status = match database_status {
            DatabaseCheckpointControlStatus::Running(_) => {
                unreachable!("should not enter initializing when running")
            }
            DatabaseCheckpointControlStatus::Recovering(state) => match state.stage {
                DatabaseRecoveringStage::Initializing { .. } => {
                    unreachable!("can only enter initializing when resetting")
                }
                DatabaseRecoveringStage::Resetting { .. } => state,
            },
        };
        let DatabaseRuntimeInfoSnapshot {
            job_infos,
            mut state_table_committed_epochs,
            mut state_table_log_epochs,
            mut mv_depended_subscriptions,
            stream_actors,
            fragment_relations,
            mut source_splits,
            mut background_jobs,
            mut cdc_table_snapshot_splits,
        } = runtime_info;
        let mut injected_creating_jobs = HashSet::new();
        let result: MetaResult<_> = try {
            control_stream_manager.inject_database_initial_barrier(
                self.database_id,
                job_infos,
                &mut state_table_committed_epochs,
                &mut state_table_log_epochs,
                &fragment_relations,
                &stream_actors,
                &mut source_splits,
                &mut background_jobs,
                &mut mv_depended_subscriptions,
                false,
                &self.control.hummock_version_stats,
                &mut cdc_table_snapshot_splits,
                &mut injected_creating_jobs,
            )?
        };
        match result {
            Ok(initial_barrier_collector) => {
                info!(node_to_collect = ?initial_barrier_collector, database_id = ?self.database_id, "database enter initializing");
                status.stage = DatabaseRecoveringStage::Initializing {
                    initial_barrier_collector: initial_barrier_collector.into(),
                };
            }
            Err(e) => {
                warn!(
                    database_id = %self.database_id,
                    e = %e.as_report(),
                    "failed to inject initial barrier"
                );
                let (backoff_future, reset_request_id) = status.next_retry();
                let (database_resp_collector, creating_job_collectors) =
                    DatabaseRecoveringState::reset_database_partial_graphs_again(
                        self.database_id,
                        injected_creating_jobs.into_iter(),
                        control_stream_manager,
                        reset_request_id,
                    );
                status.metrics.recovery_failure_cnt.inc();
                status.stage = DatabaseRecoveringStage::Resetting {
                    database_resp_collector,
                    creating_job_collectors,
                    backoff_future: Some(backoff_future),
                };
            }
        }
    }

    pub(crate) fn fail_reload_runtime_info(self, e: MetaError) {
        let database_status = self
            .control
            .databases
            .get_mut(&self.database_id)
            .expect("should exist");
        let status = match database_status {
            DatabaseCheckpointControlStatus::Running(_) => {
                unreachable!("should not enter initializing when running")
            }
            DatabaseCheckpointControlStatus::Recovering(state) => match state.stage {
                DatabaseRecoveringStage::Initializing { .. } => {
                    unreachable!("can only enter initializing when resetting")
                }
                DatabaseRecoveringStage::Resetting { .. } => state,
            },
        };
        warn!(
            database_id = %self.database_id,
            e = %e.as_report(),
            "failed to reload runtime info"
        );
        let (backoff_future, _reset_request_id) = status.next_retry();
        status.metrics.recovery_failure_cnt.inc();
        status.stage = DatabaseRecoveringStage::Resetting {
            database_resp_collector: Default::default(),
            creating_job_collectors: Default::default(),
            backoff_future: Some(backoff_future),
        };
    }

    pub(crate) fn remove(self) {
        self.control
            .databases
            .remove(&self.database_id)
            .expect("should exist");
        self.control
            .env
            .shared_actor_infos()
            .remove_database(self.database_id);
    }
}

pub(crate) struct EnterRunning;

impl DatabaseStatusAction<'_, EnterRunning> {
    pub(crate) fn enter(self) {
        info!(database_id = ?self.database_id, "database enter running");
        let event_log_manager_ref = self.control.env.event_log_manager_ref();
        event_log_manager_ref.add_event_logs(vec![Event::Recovery(
            EventRecovery::database_recovery_success(self.database_id.as_raw_id()),
        )]);
        let database_status = self
            .control
            .databases
            .get_mut(&self.database_id)
            .expect("should exist");
        match database_status {
            DatabaseCheckpointControlStatus::Running(_) => {
                unreachable!("should not enter running again")
            }
            DatabaseCheckpointControlStatus::Recovering(state) => {
                let temp_place_holder = DatabaseRecoveringStage::Resetting {
                    database_resp_collector: Default::default(),
                    creating_job_collectors: Default::default(),
                    backoff_future: None,
                };
                match state.metrics.recovery_timer.take() {
                    Some(recovery_timer) => {
                        recovery_timer.observe_duration();
                    }
                    _ => {
                        if cfg!(debug_assertions) {
                            panic!(
                                "take database {} recovery latency for twice",
                                self.database_id
                            )
                        } else {
                            warn!(database_id = %self.database_id,"failed to take recovery latency")
                        }
                    }
                }
                match replace(&mut state.stage, temp_place_holder) {
                    DatabaseRecoveringStage::Resetting { .. } => {
                        unreachable!("can only enter running during initializing")
                    }
                    DatabaseRecoveringStage::Initializing {
                        initial_barrier_collector,
                    } => {
                        *database_status = DatabaseCheckpointControlStatus::Running(
                            initial_barrier_collector.finish(),
                        );
                    }
                }
            }
        }
    }
}
