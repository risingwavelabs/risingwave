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

use std::collections::HashSet;
use std::marker::PhantomData;
use std::mem::replace;
use std::task::{Context, Poll};

use futures::FutureExt;
use risingwave_common::catalog::DatabaseId;
use risingwave_meta_model::WorkerId;
use risingwave_pb::stream_service::streaming_control_stream_response::{
    ReportDatabaseFailureResponse, ResetDatabaseResponse,
};
use risingwave_pb::stream_service::BarrierCompleteResponse;
use thiserror_ext::AsReport;
use tracing::{info, warn};

use crate::barrier::checkpoint::control::DatabaseCheckpointControlStatus;
use crate::barrier::checkpoint::{CheckpointControl, DatabaseCheckpointControl};
use crate::barrier::complete_task::BarrierCompleteOutput;
use crate::barrier::rpc::ControlStreamManager;
use crate::barrier::worker::{
    get_retry_backoff_strategy, RetryBackoffFuture, RetryBackoffStrategy,
};
use crate::barrier::DatabaseRuntimeInfoSnapshot;

enum DatabaseRecoveringStage {
    Resetting(HashSet<WorkerId>, u32, Option<RetryBackoffFuture>),
    Initializing(HashSet<WorkerId>, DatabaseCheckpointControl, u64),
}

pub(crate) struct DatabaseRecoveringState {
    stage: DatabaseRecoveringStage,
    next_reset_request_id: u32,
    retry_backoff_strategy: RetryBackoffStrategy,
}

pub(super) enum RecoveringStateAction {
    EnterInitializing,
    EnterRunning,
}

impl DatabaseRecoveringState {
    fn next_retry(&mut self) -> (RetryBackoffFuture, u32) {
        let backoff_future = self
            .retry_backoff_strategy
            .next()
            .expect("should not be empty");
        let request_id = self.next_reset_request_id;
        self.next_reset_request_id += 1;
        (backoff_future, request_id)
    }

    pub(super) fn barrier_collected(&mut self, resp: BarrierCompleteResponse) {
        match &mut self.stage {
            DatabaseRecoveringStage::Resetting(..) => {
                // ignore the collected barrier on resetting or backoff
            }
            DatabaseRecoveringStage::Initializing(remaining_worker, _, prev_epoch) => {
                assert!(remaining_worker.remove(&(resp.worker_id as WorkerId)));
                assert_eq!(resp.epoch, *prev_epoch);
            }
        }
    }

    pub(super) fn remaining_workers(&self) -> &HashSet<WorkerId> {
        match &self.stage {
            DatabaseRecoveringStage::Resetting(remaining_workers, _, _)
            | DatabaseRecoveringStage::Initializing(remaining_workers, ..) => remaining_workers,
        }
    }

    pub(super) fn on_reset_database_resp(
        &mut self,
        worker_id: WorkerId,
        resp: ResetDatabaseResponse,
    ) {
        match &mut self.stage {
            DatabaseRecoveringStage::Resetting(remaining_worker, request_id, _) => {
                if resp.reset_request_id < *request_id {
                    info!(
                        database_id = resp.database_id,
                        worker_id,
                        received_request_id = resp.reset_request_id,
                        ongoing_request_id = request_id,
                        "ignore stale reset response"
                    );
                } else {
                    assert_eq!(resp.reset_request_id, *request_id);
                    assert!(remaining_worker.remove(&worker_id));
                }
            }
            DatabaseRecoveringStage::Initializing(..) => {
                unreachable!("all reset resp should have been received in Resetting")
            }
        }
    }

    pub(super) fn poll_next_event(&mut self, cx: &mut Context<'_>) -> Poll<RecoveringStateAction> {
        match &mut self.stage {
            DatabaseRecoveringStage::Resetting(remaining_workers, _, backoff_future_option) => {
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
                if pass_backoff && remaining_workers.is_empty() {
                    return Poll::Ready(RecoveringStateAction::EnterInitializing);
                }
            }
            DatabaseRecoveringStage::Initializing(remaining_workers, ..) => {
                if remaining_workers.is_empty() {
                    return Poll::Ready(RecoveringStateAction::EnterRunning);
                }
            }
        }
        Poll::Pending
    }
}

pub(crate) struct DatabaseStatusAction<'a, A> {
    control: &'a mut CheckpointControl,
    database_id: DatabaseId,
    _phantom_action: PhantomData<A>,
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
    ) -> DatabaseStatusAction<'_, A> {
        DatabaseStatusAction {
            control: self,
            database_id,
            _phantom_action: PhantomData,
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
        if let Some(output) = barrier_complete_output {
            self.control.ack_completed(output);
        }
        let database_status = self
            .control
            .databases
            .get_mut(&self.database_id)
            .expect("should exist");
        match database_status {
            DatabaseCheckpointControlStatus::Running(_) => {
                let reset_request_id = 0;
                let remaining_workers =
                    control_stream_manager.reset_database(self.database_id, reset_request_id);
                *database_status =
                    DatabaseCheckpointControlStatus::Recovering(DatabaseRecoveringState {
                        stage: DatabaseRecoveringStage::Resetting(
                            remaining_workers,
                            reset_request_id,
                            None,
                        ),
                        next_reset_request_id: reset_request_id + 1,
                        retry_backoff_strategy: get_retry_backoff_strategy(),
                    });
            }
            DatabaseCheckpointControlStatus::Recovering(state) => match state.stage {
                DatabaseRecoveringStage::Resetting(..) => {
                    unreachable!("should not enter resetting again")
                }
                DatabaseRecoveringStage::Initializing(..) => {
                    let (backoff_future, reset_request_id) = state.next_retry();
                    let remaining_workers =
                        control_stream_manager.reset_database(self.database_id, reset_request_id);
                    state.stage = DatabaseRecoveringStage::Resetting(
                        remaining_workers,
                        reset_request_id,
                        Some(backoff_future),
                    );
                }
            },
        }
    }
}

impl CheckpointControl {
    pub(crate) fn on_report_failure(
        &mut self,
        resp: ReportDatabaseFailureResponse,
        control_stream_manager: &mut ControlStreamManager,
    ) -> Option<DatabaseStatusAction<'_, EnterReset>> {
        let database_id = DatabaseId::new(resp.database_id);
        let database_status = self.databases.get_mut(&database_id).expect("should exist");
        match database_status {
            DatabaseCheckpointControlStatus::Running(_) => {
                Some(self.new_database_status_action(database_id))
            }
            DatabaseCheckpointControlStatus::Recovering(state) => match state.stage {
                DatabaseRecoveringStage::Resetting(..) => {
                    // ignore reported failure during resetting or backoff.
                    None
                }
                DatabaseRecoveringStage::Initializing(..) => {
                    warn!(database_id = database_id.database_id, "");
                    let (backoff_future, request_id) = state.next_retry();
                    let remaining_workers =
                        control_stream_manager.reset_database(database_id, request_id);
                    state.stage = DatabaseRecoveringStage::Resetting(
                        remaining_workers,
                        request_id,
                        Some(backoff_future),
                    );
                    None
                }
            },
        }
    }
}

pub(crate) struct EnterInitializing;

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
                DatabaseRecoveringStage::Initializing(..) => {
                    unreachable!("can only enter initializing when resetting")
                }
                DatabaseRecoveringStage::Resetting(..) => state,
            },
        };
        let DatabaseRuntimeInfoSnapshot {
            database_fragment_info,
            mut state_table_committed_epochs,
            subscription_info,
            mut stream_actors,
            mut source_splits,
            mut background_jobs,
        } = runtime_info;
        match control_stream_manager.inject_database_initial_barrier(
            self.database_id,
            database_fragment_info,
            &mut state_table_committed_epochs,
            &mut stream_actors,
            &mut source_splits,
            &mut background_jobs,
            subscription_info,
            None,
            &self.control.hummock_version_stats,
        ) {
            Ok((node_to_collect, database, prev_epoch)) => {
                status.stage =
                    DatabaseRecoveringStage::Initializing(node_to_collect, database, prev_epoch);
            }
            Err(e) => {
                warn!(database_id = self.database_id.database_id,e = ?e.as_report(), "failed to inject initial barrier");
                let (backoff_future, request_id) = status.next_retry();
                let remaining_workers =
                    control_stream_manager.reset_database(self.database_id, request_id);
                status.stage = DatabaseRecoveringStage::Resetting(
                    remaining_workers,
                    request_id,
                    Some(backoff_future),
                );
            }
        }
    }

    pub(crate) fn remove(self) {
        self.control
            .databases
            .remove(&self.database_id)
            .expect("should exist");
    }
}

pub(crate) struct EnterRunning;

impl DatabaseStatusAction<'_, EnterRunning> {
    pub(crate) fn enter(self) {
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
                let temp_place_holder = DatabaseRecoveringStage::Resetting(HashSet::new(), 0, None);
                match replace(&mut state.stage, temp_place_holder) {
                    DatabaseRecoveringStage::Resetting(..) => {
                        unreachable!("can only enter running during initializing")
                    }
                    DatabaseRecoveringStage::Initializing(remaining_workers, control, _) => {
                        assert!(remaining_workers.is_empty());
                        *database_status = DatabaseCheckpointControlStatus::Running(control);
                    }
                }
            }
        }
    }
}
