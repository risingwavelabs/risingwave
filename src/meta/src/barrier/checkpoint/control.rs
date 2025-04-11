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

use std::collections::hash_map::Entry;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::future::{Future, poll_fn};
use std::mem::take;
use std::task::Poll;

use anyhow::anyhow;
use fail::fail_point;
use prometheus::HistogramTimer;
use risingwave_common::catalog::{DatabaseId, TableId};
use risingwave_common::metrics::{LabelGuardedHistogram, LabelGuardedIntGauge};
use risingwave_meta_model::WorkerId;
use risingwave_pb::ddl_service::DdlProgress;
use risingwave_pb::hummock::HummockVersionStats;
use risingwave_pb::stream_service::BarrierCompleteResponse;
use risingwave_pb::stream_service::streaming_control_stream_response::ResetDatabaseResponse;
use thiserror_ext::AsReport;
use tracing::{debug, warn};

use crate::barrier::checkpoint::creating_job::{CompleteJobType, CreatingStreamingJobControl};
use crate::barrier::checkpoint::recovery::{
    DatabaseRecoveringState, DatabaseStatusAction, EnterInitializing, EnterRunning,
    RecoveringStateAction,
};
use crate::barrier::checkpoint::state::BarrierWorkerState;
use crate::barrier::command::CommandContext;
use crate::barrier::complete_task::{BarrierCompleteOutput, CompleteBarrierTask};
use crate::barrier::info::InflightStreamingJobInfo;
use crate::barrier::notifier::Notifier;
use crate::barrier::progress::{CreateMviewProgressTracker, TrackingCommand, TrackingJob};
use crate::barrier::rpc::{ControlStreamManager, from_partial_graph_id};
use crate::barrier::schedule::{NewBarrier, PeriodicBarriers};
use crate::barrier::utils::{
    NodeToCollect, collect_creating_job_commit_epoch_info, is_valid_after_worker_err,
};
use crate::barrier::{
    BarrierKind, Command, CreateStreamingJobType, InflightSubscriptionInfo, TracedEpoch,
};
use crate::manager::MetaSrvEnv;
use crate::rpc::metrics::GLOBAL_META_METRICS;
use crate::stream::fill_snapshot_backfill_epoch;
use crate::{MetaError, MetaResult};

pub(crate) struct CheckpointControl {
    pub(crate) env: MetaSrvEnv,
    pub(super) databases: HashMap<DatabaseId, DatabaseCheckpointControlStatus>,
    pub(super) hummock_version_stats: HummockVersionStats,
}

impl CheckpointControl {
    pub fn new(env: MetaSrvEnv) -> Self {
        Self {
            env,
            databases: Default::default(),
            hummock_version_stats: Default::default(),
        }
    }

    pub(crate) fn recover(
        databases: impl IntoIterator<Item = (DatabaseId, DatabaseCheckpointControl)>,
        failed_databases: HashSet<DatabaseId>,
        control_stream_manager: &mut ControlStreamManager,
        hummock_version_stats: HummockVersionStats,
        env: MetaSrvEnv,
    ) -> Self {
        Self {
            env,
            databases: databases
                .into_iter()
                .map(|(database_id, control)| {
                    (
                        database_id,
                        DatabaseCheckpointControlStatus::Running(control),
                    )
                })
                .chain(failed_databases.into_iter().map(|database_id| {
                    (
                        database_id,
                        DatabaseCheckpointControlStatus::Recovering(
                            DatabaseRecoveringState::resetting(database_id, control_stream_manager),
                        ),
                    )
                }))
                .collect(),
            hummock_version_stats,
        }
    }

    pub(crate) fn ack_completed(&mut self, output: BarrierCompleteOutput) {
        self.hummock_version_stats = output.hummock_version_stats;
        for (database_id, (command_prev_epoch, creating_job_epochs)) in output.epochs_to_ack {
            self.databases
                .get_mut(&database_id)
                .expect("should exist")
                .expect_running("should have wait for completing command before enter recovery")
                .ack_completed(command_prev_epoch, creating_job_epochs);
        }
    }

    pub(crate) fn next_complete_barrier_task(
        &mut self,
        mut context: Option<(&mut PeriodicBarriers, &mut ControlStreamManager)>,
    ) -> Option<CompleteBarrierTask> {
        let mut task = None;
        for database in self.databases.values_mut() {
            let Some(database) = database.running_state_mut() else {
                continue;
            };
            let context = context.as_mut().map(|(s, c)| (&mut **s, &mut **c));
            database.next_complete_barrier_task(&mut task, context, &self.hummock_version_stats);
        }
        task
    }

    pub(crate) fn barrier_collected(
        &mut self,
        resp: BarrierCompleteResponse,
        periodic_barriers: &mut PeriodicBarriers,
    ) -> MetaResult<()> {
        let database_id = DatabaseId::new(resp.database_id);
        let database_status = self.databases.get_mut(&database_id).expect("should exist");
        match database_status {
            DatabaseCheckpointControlStatus::Running(database) => {
                database.barrier_collected(resp, periodic_barriers)
            }
            DatabaseCheckpointControlStatus::Recovering(state) => {
                state.barrier_collected(database_id, resp);
                Ok(())
            }
        }
    }

    pub(crate) fn can_inject_barrier(&self, in_flight_barrier_nums: usize) -> bool {
        self.databases.values().all(|database| {
            database
                .running_state()
                .map(|database| database.can_inject_barrier(in_flight_barrier_nums))
                .unwrap_or(true)
        })
    }

    pub(crate) fn max_prev_epoch(&self) -> Option<TracedEpoch> {
        self.databases
            .values()
            .flat_map(|database| {
                database
                    .running_state()
                    .map(|database| database.state.in_flight_prev_epoch())
            })
            .max_by_key(|epoch| epoch.value())
            .cloned()
    }

    pub(crate) fn recovering_databases(&self) -> impl Iterator<Item = DatabaseId> + '_ {
        self.databases.iter().filter_map(|(database_id, database)| {
            database.running_state().is_none().then_some(*database_id)
        })
    }

    pub(crate) fn running_databases(&self) -> impl Iterator<Item = DatabaseId> + '_ {
        self.databases.iter().filter_map(|(database_id, database)| {
            database.running_state().is_some().then_some(*database_id)
        })
    }

    /// return Some(failed `database_id` -> `err`)
    pub(crate) fn handle_new_barrier(
        &mut self,
        new_barrier: NewBarrier,
        control_stream_manager: &mut ControlStreamManager,
    ) -> Option<HashMap<DatabaseId, MetaError>> {
        let NewBarrier {
            command,
            span,
            checkpoint,
        } = new_barrier;

        if let Some((database_id, mut command, notifiers)) = command {
            if let &mut Command::CreateStreamingJob {
                ref mut cross_db_snapshot_backfill_info,
                ref info,
                ..
            } = &mut command
            {
                for (table_id, snapshot_epoch) in
                    &mut cross_db_snapshot_backfill_info.upstream_mv_table_id_to_backfill_epoch
                {
                    for database in self.databases.values() {
                        if let Some(database) = database.running_state()
                            && database.state.inflight_graph_info.contains_job(*table_id)
                        {
                            if let Some(committed_epoch) = database.committed_epoch {
                                *snapshot_epoch = Some(committed_epoch);
                            }
                            break;
                        }
                    }
                    if snapshot_epoch.is_none() {
                        let table_id = *table_id;
                        warn!(
                            ?cross_db_snapshot_backfill_info,
                            ?table_id,
                            ?info,
                            "database of cross db upstream table not found"
                        );
                        let err: MetaError =
                            anyhow!("database of cross db upstream table {} not found", table_id)
                                .into();
                        for notifier in notifiers {
                            notifier.notify_start_failed(err.clone());
                        }

                        return None;
                    }
                }
            }

            let max_prev_epoch = self.max_prev_epoch();
            let (database, max_prev_epoch) = match self.databases.entry(database_id) {
                Entry::Occupied(entry) => (
                    entry
                        .into_mut()
                        .expect_running("should not have command when not running"),
                    max_prev_epoch.expect("should exist when having some database"),
                ),
                Entry::Vacant(entry) => match &command {
                    Command::CreateStreamingJob {
                        job_type: CreateStreamingJobType::Normal,
                        ..
                    } => {
                        let new_database = DatabaseCheckpointControl::new(database_id);
                        let max_prev_epoch = if let Some(max_prev_epoch) = max_prev_epoch {
                            if max_prev_epoch.value()
                                < new_database.state.in_flight_prev_epoch().value()
                            {
                                new_database.state.in_flight_prev_epoch().clone()
                            } else {
                                max_prev_epoch
                            }
                        } else {
                            new_database.state.in_flight_prev_epoch().clone()
                        };
                        control_stream_manager.add_partial_graph(database_id, None);
                        (
                            entry
                                .insert(DatabaseCheckpointControlStatus::Running(new_database))
                                .expect_running("just initialized as running"),
                            max_prev_epoch,
                        )
                    }
                    Command::Flush | Command::Pause | Command::Resume => {
                        for mut notifier in notifiers {
                            notifier.notify_started();
                            notifier.notify_collected();
                        }
                        warn!(?command, "skip command for empty database");
                        return None;
                    }
                    _ => {
                        panic!(
                            "new database graph info can only be created for normal creating streaming job, but get command: {} {:?}",
                            database_id, command
                        )
                    }
                },
            };

            let curr_epoch = max_prev_epoch.next();

            let mut failed_databases: Option<HashMap<_, _>> = None;

            if let Err(e) = database.handle_new_barrier(
                Some((command, notifiers)),
                checkpoint,
                span.clone(),
                control_stream_manager,
                &self.hummock_version_stats,
                curr_epoch.clone(),
            ) {
                failed_databases
                    .get_or_insert_default()
                    .try_insert(database_id, e)
                    .expect("non-duplicate");
            }
            for database in self.databases.values_mut() {
                let Some(database) = database.running_state_mut() else {
                    continue;
                };
                if database.database_id == database_id {
                    continue;
                }
                if let Err(e) = database.handle_new_barrier(
                    None,
                    checkpoint,
                    span.clone(),
                    control_stream_manager,
                    &self.hummock_version_stats,
                    curr_epoch.clone(),
                ) {
                    failed_databases
                        .get_or_insert_default()
                        .try_insert(database.database_id, e)
                        .expect("non-duplicate");
                }
            }
            failed_databases
        } else {
            #[expect(clippy::question_mark)]
            let Some(max_prev_epoch) = self.max_prev_epoch() else {
                return None;
            };
            let curr_epoch = max_prev_epoch.next();

            let mut failed_databases: Option<HashMap<_, _>> = None;
            for database in self.databases.values_mut() {
                let Some(database) = database.running_state_mut() else {
                    continue;
                };
                if let Err(e) = database.handle_new_barrier(
                    None,
                    checkpoint,
                    span.clone(),
                    control_stream_manager,
                    &self.hummock_version_stats,
                    curr_epoch.clone(),
                ) {
                    failed_databases
                        .get_or_insert_default()
                        .try_insert(database.database_id, e)
                        .expect("non-duplicate");
                }
            }

            failed_databases
        }
    }

    pub(crate) fn update_barrier_nums_metrics(&self) {
        self.databases
            .values()
            .flat_map(|database| database.running_state())
            .for_each(|database| database.update_barrier_nums_metrics());
    }

    pub(crate) fn gen_ddl_progress(&self) -> HashMap<u32, DdlProgress> {
        let mut progress = HashMap::new();
        for status in self.databases.values() {
            let Some(database_checkpoint_control) = status.running_state() else {
                continue;
            };
            // Progress of normal backfill
            progress.extend(
                database_checkpoint_control
                    .create_mview_tracker
                    .gen_ddl_progress(),
            );
            // Progress of snapshot backfill
            for creating_job in database_checkpoint_control
                .creating_streaming_job_controls
                .values()
            {
                progress.extend([(
                    creating_job.job_id.table_id,
                    creating_job.gen_ddl_progress(),
                )]);
            }
        }
        progress
    }

    pub(crate) fn databases_failed_at_worker_err(
        &mut self,
        worker_id: WorkerId,
    ) -> Vec<DatabaseId> {
        let mut failed_databases = Vec::new();
        for (database_id, database_status) in &mut self.databases {
            let database_checkpoint_control = match database_status {
                DatabaseCheckpointControlStatus::Running(control) => control,
                DatabaseCheckpointControlStatus::Recovering(state) => {
                    if !state.is_valid_after_worker_err(worker_id) {
                        failed_databases.push(*database_id);
                    }
                    continue;
                }
            };

            if !database_checkpoint_control.is_valid_after_worker_err(worker_id as _)
                || database_checkpoint_control
                    .state
                    .inflight_graph_info
                    .contains_worker(worker_id as _)
                || database_checkpoint_control
                    .creating_streaming_job_controls
                    .values_mut()
                    .any(|job| !job.is_valid_after_worker_err(worker_id))
            {
                failed_databases.push(*database_id);
            }
        }
        failed_databases
    }

    pub(crate) fn clear_on_err(&mut self, err: &MetaError) {
        for (_, node) in self.databases.values_mut().flat_map(|status| {
            status
                .running_state_mut()
                .map(|database| take(&mut database.command_ctx_queue))
                .into_iter()
                .flatten()
        }) {
            for notifier in node.notifiers {
                notifier.notify_collection_failed(err.clone());
            }
            node.enqueue_time.observe_duration();
        }
        self.databases.values_mut().for_each(|database| {
            if let Some(database) = database.running_state_mut() {
                database.create_mview_tracker.abort_all()
            }
        });
    }

    pub(crate) fn inflight_infos(
        &self,
    ) -> impl Iterator<
        Item = (
            DatabaseId,
            &InflightSubscriptionInfo,
            impl Iterator<Item = TableId> + '_,
        ),
    > + '_ {
        self.databases.iter().flat_map(|(database_id, database)| {
            database
                .database_state()
                .map(|(database_state, creating_jobs)| {
                    (
                        *database_id,
                        &database_state.inflight_subscription_info,
                        creating_jobs
                            .values()
                            .filter_map(|job| job.is_consuming().then_some(job.job_id)),
                    )
                })
        })
    }
}

pub(crate) enum CheckpointControlEvent<'a> {
    EnteringInitializing(DatabaseStatusAction<'a, EnterInitializing>),
    EnteringRunning(DatabaseStatusAction<'a, EnterRunning>),
}

impl CheckpointControl {
    pub(crate) fn on_reset_database_resp(
        &mut self,
        worker_id: WorkerId,
        resp: ResetDatabaseResponse,
    ) {
        let database_id = DatabaseId::new(resp.database_id);
        match self.databases.get_mut(&database_id).expect("should exist") {
            DatabaseCheckpointControlStatus::Running(_) => {
                unreachable!("should not receive reset database resp when running")
            }
            DatabaseCheckpointControlStatus::Recovering(state) => {
                state.on_reset_database_resp(worker_id, resp)
            }
        }
    }

    pub(crate) fn next_event(
        &mut self,
    ) -> impl Future<Output = CheckpointControlEvent<'_>> + Send + '_ {
        let mut this = Some(self);
        poll_fn(move |cx| {
            let Some(this_mut) = this.as_mut() else {
                unreachable!("should not be polled after poll ready")
            };
            for (&database_id, database_status) in &mut this_mut.databases {
                match database_status {
                    DatabaseCheckpointControlStatus::Running(_) => {}
                    DatabaseCheckpointControlStatus::Recovering(state) => {
                        let poll_result = state.poll_next_event(cx);
                        if let Poll::Ready(action) = poll_result {
                            let this = this.take().expect("checked Some");
                            return Poll::Ready(match action {
                                RecoveringStateAction::EnterInitializing(reset_workers) => {
                                    CheckpointControlEvent::EnteringInitializing(
                                        this.new_database_status_action(
                                            database_id,
                                            EnterInitializing(reset_workers),
                                        ),
                                    )
                                }
                                RecoveringStateAction::EnterRunning => {
                                    CheckpointControlEvent::EnteringRunning(
                                        this.new_database_status_action(database_id, EnterRunning),
                                    )
                                }
                            });
                        }
                    }
                }
            }
            Poll::Pending
        })
    }
}

pub(crate) enum DatabaseCheckpointControlStatus {
    Running(DatabaseCheckpointControl),
    Recovering(DatabaseRecoveringState),
}

impl DatabaseCheckpointControlStatus {
    fn running_state(&self) -> Option<&DatabaseCheckpointControl> {
        match self {
            DatabaseCheckpointControlStatus::Running(state) => Some(state),
            DatabaseCheckpointControlStatus::Recovering(_) => None,
        }
    }

    fn running_state_mut(&mut self) -> Option<&mut DatabaseCheckpointControl> {
        match self {
            DatabaseCheckpointControlStatus::Running(state) => Some(state),
            DatabaseCheckpointControlStatus::Recovering(_) => None,
        }
    }

    fn database_state(
        &self,
    ) -> Option<(
        &BarrierWorkerState,
        &HashMap<TableId, CreatingStreamingJobControl>,
    )> {
        match self {
            DatabaseCheckpointControlStatus::Running(control) => {
                Some((&control.state, &control.creating_streaming_job_controls))
            }
            DatabaseCheckpointControlStatus::Recovering(state) => state.database_state(),
        }
    }

    fn expect_running(&mut self, reason: &'static str) -> &mut DatabaseCheckpointControl {
        match self {
            DatabaseCheckpointControlStatus::Running(state) => state,
            DatabaseCheckpointControlStatus::Recovering(_) => {
                panic!("should be at running: {}", reason)
            }
        }
    }
}

struct DatabaseCheckpointControlMetrics {
    barrier_latency: LabelGuardedHistogram<1>,
    in_flight_barrier_nums: LabelGuardedIntGauge<1>,
    all_barrier_nums: LabelGuardedIntGauge<1>,
}

impl DatabaseCheckpointControlMetrics {
    fn new(database_id: DatabaseId) -> Self {
        let database_id_str = database_id.database_id.to_string();
        let barrier_latency = GLOBAL_META_METRICS
            .barrier_latency
            .with_guarded_label_values(&[&database_id_str]);
        let in_flight_barrier_nums = GLOBAL_META_METRICS
            .in_flight_barrier_nums
            .with_guarded_label_values(&[&database_id_str]);
        let all_barrier_nums = GLOBAL_META_METRICS
            .all_barrier_nums
            .with_guarded_label_values(&[&database_id_str]);
        Self {
            barrier_latency,
            in_flight_barrier_nums,
            all_barrier_nums,
        }
    }
}

/// Controls the concurrent execution of commands.
pub(crate) struct DatabaseCheckpointControl {
    database_id: DatabaseId,
    state: BarrierWorkerState,

    /// Save the state and message of barrier in order.
    /// Key is the `prev_epoch`.
    command_ctx_queue: BTreeMap<u64, EpochNode>,
    /// The barrier that are completing.
    /// Some(`prev_epoch`)
    completing_barrier: Option<u64>,

    committed_epoch: Option<u64>,
    creating_streaming_job_controls: HashMap<TableId, CreatingStreamingJobControl>,

    create_mview_tracker: CreateMviewProgressTracker,

    metrics: DatabaseCheckpointControlMetrics,
}

impl DatabaseCheckpointControl {
    fn new(database_id: DatabaseId) -> Self {
        Self {
            database_id,
            state: BarrierWorkerState::new(),
            command_ctx_queue: Default::default(),
            completing_barrier: None,
            committed_epoch: None,
            creating_streaming_job_controls: Default::default(),
            create_mview_tracker: Default::default(),
            metrics: DatabaseCheckpointControlMetrics::new(database_id),
        }
    }

    pub(crate) fn recovery(
        database_id: DatabaseId,
        create_mview_tracker: CreateMviewProgressTracker,
        state: BarrierWorkerState,
        committed_epoch: u64,
    ) -> Self {
        Self {
            database_id,
            state,
            command_ctx_queue: Default::default(),
            completing_barrier: None,
            committed_epoch: Some(committed_epoch),
            creating_streaming_job_controls: Default::default(),
            create_mview_tracker,
            metrics: DatabaseCheckpointControlMetrics::new(database_id),
        }
    }

    fn total_command_num(&self) -> usize {
        self.command_ctx_queue.len()
            + match &self.completing_barrier {
                Some(_) => 1,
                None => 0,
            }
    }

    /// Update the metrics of barrier nums.
    fn update_barrier_nums_metrics(&self) {
        self.metrics.in_flight_barrier_nums.set(
            self.command_ctx_queue
                .values()
                .filter(|x| x.state.is_inflight())
                .count() as i64,
        );
        self.metrics
            .all_barrier_nums
            .set(self.total_command_num() as i64);
    }

    fn jobs_to_merge(
        &self,
    ) -> Option<HashMap<TableId, (HashSet<TableId>, InflightStreamingJobInfo)>> {
        let mut table_ids_to_merge = HashMap::new();

        for (table_id, creating_streaming_job) in &self.creating_streaming_job_controls {
            if let Some(graph_info) = creating_streaming_job.should_merge_to_upstream() {
                table_ids_to_merge.insert(
                    *table_id,
                    (
                        creating_streaming_job
                            .snapshot_backfill_upstream_tables
                            .clone(),
                        graph_info.clone(),
                    ),
                );
            }
        }
        if table_ids_to_merge.is_empty() {
            None
        } else {
            Some(table_ids_to_merge)
        }
    }

    /// Enqueue a barrier command
    fn enqueue_command(
        &mut self,
        command_ctx: CommandContext,
        notifiers: Vec<Notifier>,
        node_to_collect: NodeToCollect,
        creating_jobs_to_wait: HashSet<TableId>,
    ) {
        let timer = self.metrics.barrier_latency.start_timer();

        if let Some((_, node)) = self.command_ctx_queue.last_key_value() {
            assert_eq!(
                command_ctx.barrier_info.prev_epoch.value(),
                node.command_ctx.barrier_info.curr_epoch.value()
            );
        }

        tracing::trace!(
            prev_epoch = command_ctx.barrier_info.prev_epoch(),
            ?creating_jobs_to_wait,
            ?node_to_collect,
            "enqueue command"
        );
        self.command_ctx_queue.insert(
            command_ctx.barrier_info.prev_epoch(),
            EpochNode {
                enqueue_time: timer,
                state: BarrierEpochState {
                    node_to_collect,
                    resps: vec![],
                    creating_jobs_to_wait,
                    finished_jobs: HashMap::new(),
                },
                command_ctx,
                notifiers,
            },
        );
    }

    /// Change the state of this `prev_epoch` to `Completed`. Return continuous nodes
    /// with `Completed` starting from first node [`Completed`..`InFlight`) and remove them.
    fn barrier_collected(
        &mut self,
        resp: BarrierCompleteResponse,
        periodic_barriers: &mut PeriodicBarriers,
    ) -> MetaResult<()> {
        let worker_id = resp.worker_id;
        let prev_epoch = resp.epoch;
        tracing::trace!(
            worker_id,
            prev_epoch,
            partial_graph_id = resp.partial_graph_id,
            "barrier collected"
        );
        let creating_job_id = from_partial_graph_id(resp.partial_graph_id);
        match creating_job_id {
            None => {
                if let Some(node) = self.command_ctx_queue.get_mut(&prev_epoch) {
                    assert!(
                        node.state
                            .node_to_collect
                            .remove(&(worker_id as _))
                            .is_some()
                    );
                    node.state.resps.push(resp);
                } else {
                    panic!(
                        "collect barrier on non-existing barrier: {}, {}",
                        prev_epoch, worker_id
                    );
                }
            }
            Some(creating_job_id) => {
                let should_merge_to_upstream = self
                    .creating_streaming_job_controls
                    .get_mut(&creating_job_id)
                    .expect("should exist")
                    .collect(prev_epoch, worker_id as _, resp)?;
                if should_merge_to_upstream {
                    periodic_barriers.force_checkpoint_in_next_barrier();
                }
            }
        }
        Ok(())
    }

    /// Pause inject barrier until True.
    fn can_inject_barrier(&self, in_flight_barrier_nums: usize) -> bool {
        self.command_ctx_queue
            .values()
            .filter(|x| x.state.is_inflight())
            .count()
            < in_flight_barrier_nums
    }

    /// Return whether the database can still work after worker failure
    pub(crate) fn is_valid_after_worker_err(&mut self, worker_id: WorkerId) -> bool {
        for epoch_node in self.command_ctx_queue.values_mut() {
            if !is_valid_after_worker_err(&mut epoch_node.state.node_to_collect, worker_id) {
                return false;
            }
        }
        // TODO: include barrier in creating jobs
        true
    }
}

impl DatabaseCheckpointControl {
    /// return creating job table fragment id -> (backfill progress epoch , {`upstream_mv_table_id`})
    fn collect_backfill_pinned_upstream_log_epoch(
        &self,
    ) -> HashMap<TableId, (u64, HashSet<TableId>)> {
        self.creating_streaming_job_controls
            .iter()
            .filter_map(|(table_id, creating_job)| {
                creating_job
                    .pinned_upstream_log_epoch()
                    .map(|progress_epoch| {
                        (
                            *table_id,
                            (
                                progress_epoch,
                                creating_job.snapshot_backfill_upstream_tables.clone(),
                            ),
                        )
                    })
            })
            .collect()
    }

    fn next_complete_barrier_task(
        &mut self,
        task: &mut Option<CompleteBarrierTask>,
        mut context: Option<(&mut PeriodicBarriers, &mut ControlStreamManager)>,
        hummock_version_stats: &HummockVersionStats,
    ) {
        // `Vec::new` is a const fn, and do not have memory allocation, and therefore is lightweight enough
        let mut creating_jobs_task = vec![];
        {
            // `Vec::new` is a const fn, and do not have memory allocation, and therefore is lightweight enough
            let mut finished_jobs = Vec::new();
            let min_upstream_inflight_barrier = self
                .command_ctx_queue
                .first_key_value()
                .map(|(epoch, _)| *epoch);
            for (table_id, job) in &mut self.creating_streaming_job_controls {
                if let Some((epoch, resps, status)) =
                    job.start_completing(min_upstream_inflight_barrier)
                {
                    let is_first_time = match status {
                        CompleteJobType::First => true,
                        CompleteJobType::Normal => false,
                        CompleteJobType::Finished => {
                            finished_jobs.push((*table_id, epoch, resps));
                            continue;
                        }
                    };
                    creating_jobs_task.push((*table_id, epoch, resps, is_first_time));
                }
            }
            if !finished_jobs.is_empty()
                && let Some((_, control_stream_manager)) = &mut context
            {
                control_stream_manager.remove_partial_graph(
                    self.database_id,
                    finished_jobs
                        .iter()
                        .map(|(table_id, _, _)| *table_id)
                        .collect(),
                );
            }
            for (table_id, epoch, resps) in finished_jobs {
                let epoch_state = &mut self
                    .command_ctx_queue
                    .get_mut(&epoch)
                    .expect("should exist")
                    .state;
                assert!(epoch_state.creating_jobs_to_wait.remove(&table_id));
                debug!(epoch, ?table_id, "finish creating job");
                // It's safe to remove the creating job, because on CompleteJobType::Finished,
                // all previous barriers have been collected and completed.
                let creating_streaming_job = self
                    .creating_streaming_job_controls
                    .remove(&table_id)
                    .expect("should exist");
                assert!(creating_streaming_job.is_finished());
                assert!(epoch_state.finished_jobs.insert(table_id, resps).is_none());
            }
        }
        assert!(self.completing_barrier.is_none());
        while let Some((_, EpochNode { state, .. })) = self.command_ctx_queue.first_key_value()
            && !state.is_inflight()
        {
            {
                let (_, mut node) = self.command_ctx_queue.pop_first().expect("non-empty");
                assert!(node.state.creating_jobs_to_wait.is_empty());
                assert!(node.state.node_to_collect.is_empty());
                let mut finished_jobs = self.create_mview_tracker.apply_collected_command(
                    node.command_ctx.command.as_ref(),
                    &node.command_ctx.barrier_info,
                    &node.state.resps,
                    hummock_version_stats,
                );
                if !node.command_ctx.barrier_info.kind.is_checkpoint() {
                    assert!(finished_jobs.is_empty());
                    node.notifiers.into_iter().for_each(|notifier| {
                        notifier.notify_collected();
                    });
                    if let Some((periodic_barriers, _)) = &mut context
                        && self.create_mview_tracker.has_pending_finished_jobs()
                        && self
                            .command_ctx_queue
                            .values()
                            .all(|node| !node.command_ctx.barrier_info.kind.is_checkpoint())
                    {
                        periodic_barriers.force_checkpoint_in_next_barrier();
                    }
                    continue;
                }
                node.state
                    .finished_jobs
                    .drain()
                    .for_each(|(job_id, resps)| {
                        node.state.resps.extend(resps);
                        finished_jobs.push(TrackingJob::New(TrackingCommand {
                            job_id,
                            replace_stream_job: None,
                        }));
                    });
                let task = task.get_or_insert_default();
                node.command_ctx.collect_commit_epoch_info(
                    &mut task.commit_info,
                    take(&mut node.state.resps),
                    self.collect_backfill_pinned_upstream_log_epoch(),
                );
                self.completing_barrier = Some(node.command_ctx.barrier_info.prev_epoch());
                task.finished_jobs.extend(finished_jobs);
                task.notifiers.extend(node.notifiers);
                task.epoch_infos
                    .try_insert(
                        self.database_id,
                        (Some((node.command_ctx, node.enqueue_time)), vec![]),
                    )
                    .expect("non duplicate");
                break;
            }
        }
        if !creating_jobs_task.is_empty() {
            let task = task.get_or_insert_default();
            for (table_id, epoch, resps, is_first_time) in creating_jobs_task {
                collect_creating_job_commit_epoch_info(
                    &mut task.commit_info,
                    epoch,
                    resps,
                    self.creating_streaming_job_controls[&table_id].state_table_ids(),
                    is_first_time,
                );
                let (_, creating_job_epochs) =
                    task.epoch_infos.entry(self.database_id).or_default();
                creating_job_epochs.push((table_id, epoch));
            }
        }
    }

    fn ack_completed(
        &mut self,
        command_prev_epoch: Option<u64>,
        creating_job_epochs: Vec<(TableId, u64)>,
    ) {
        {
            if let Some(prev_epoch) = self.completing_barrier.take() {
                assert_eq!(command_prev_epoch, Some(prev_epoch));
                self.committed_epoch = Some(prev_epoch);
            } else {
                assert_eq!(command_prev_epoch, None);
            };
            for (table_id, epoch) in creating_job_epochs {
                self.creating_streaming_job_controls
                    .get_mut(&table_id)
                    .expect("should exist")
                    .ack_completed(epoch)
            }
        }
    }
}

/// The state and message of this barrier, a node for concurrent checkpoint.
struct EpochNode {
    /// Timer for recording barrier latency, taken after `complete_barriers`.
    enqueue_time: HistogramTimer,

    /// Whether this barrier is in-flight or completed.
    state: BarrierEpochState,

    /// Context of this command to generate barrier and do some post jobs.
    command_ctx: CommandContext,
    /// Notifiers of this barrier.
    notifiers: Vec<Notifier>,
}

#[derive(Debug)]
/// The state of barrier.
struct BarrierEpochState {
    node_to_collect: NodeToCollect,

    resps: Vec<BarrierCompleteResponse>,

    creating_jobs_to_wait: HashSet<TableId>,

    finished_jobs: HashMap<TableId, Vec<BarrierCompleteResponse>>,
}

impl BarrierEpochState {
    fn is_inflight(&self) -> bool {
        !self.node_to_collect.is_empty() || !self.creating_jobs_to_wait.is_empty()
    }
}

impl DatabaseCheckpointControl {
    /// Handle the new barrier from the scheduled queue and inject it.
    fn handle_new_barrier(
        &mut self,
        command: Option<(Command, Vec<Notifier>)>,
        checkpoint: bool,
        span: tracing::Span,
        control_stream_manager: &mut ControlStreamManager,
        hummock_version_stats: &HummockVersionStats,
        curr_epoch: TracedEpoch,
    ) -> MetaResult<()> {
        let (mut command, mut notifiers) = if let Some((command, notifiers)) = command {
            (Some(command), notifiers)
        } else {
            (None, vec![])
        };

        for table_to_cancel in command
            .as_ref()
            .map(Command::tables_to_drop)
            .into_iter()
            .flatten()
        {
            if self
                .creating_streaming_job_controls
                .contains_key(&table_to_cancel)
            {
                warn!(
                    table_id = table_to_cancel.table_id,
                    "ignore cancel command on creating streaming job"
                );
                for notifier in notifiers {
                    notifier
                        .notify_start_failed(anyhow!("cannot cancel creating streaming job, the job will continue creating until created or recovery").into());
                }
                return Ok(());
            }
        }

        if let Some(Command::RescheduleFragment { .. }) = &command {
            if !self.creating_streaming_job_controls.is_empty() {
                warn!("ignore reschedule when creating streaming job with snapshot backfill");
                for notifier in notifiers {
                    notifier.notify_start_failed(
                        anyhow!(
                            "cannot reschedule when creating streaming job with snapshot backfill",
                        )
                        .into(),
                    );
                }
                return Ok(());
            }
        }

        let Some(barrier_info) =
            self.state
                .next_barrier_info(command.as_ref(), checkpoint, curr_epoch)
        else {
            // skip the command when there is nothing to do with the barrier
            for mut notifier in notifiers {
                notifier.notify_started();
                notifier.notify_collected();
            }
            return Ok(());
        };

        let mut edges = self
            .state
            .inflight_graph_info
            .build_edge(command.as_ref(), &*control_stream_manager);

        // Insert newly added creating job
        if let Some(Command::CreateStreamingJob {
            job_type,
            info,
            cross_db_snapshot_backfill_info,
        }) = &mut command
        {
            match job_type {
                CreateStreamingJobType::Normal | CreateStreamingJobType::SinkIntoTable(_) => {
                    for fragment in info.stream_job_fragments.inner.fragments.values_mut() {
                        fill_snapshot_backfill_epoch(
                            &mut fragment.nodes,
                            None,
                            cross_db_snapshot_backfill_info,
                        )?;
                    }
                }
                CreateStreamingJobType::SnapshotBackfill(snapshot_backfill_info) => {
                    if self.state.is_paused() {
                        warn!("cannot create streaming job with snapshot backfill when paused");
                        for notifier in notifiers {
                            notifier.notify_start_failed(
                                anyhow!("cannot create streaming job with snapshot backfill when paused",)
                                    .into(),
                            );
                        }
                        return Ok(());
                    }
                    // set snapshot epoch of upstream table for snapshot backfill
                    for snapshot_backfill_epoch in snapshot_backfill_info
                        .upstream_mv_table_id_to_backfill_epoch
                        .values_mut()
                    {
                        assert_eq!(
                            snapshot_backfill_epoch.replace(barrier_info.prev_epoch()),
                            None,
                            "must not set previously"
                        );
                    }
                    for fragment in info.stream_job_fragments.inner.fragments.values_mut() {
                        if let Err(e) = fill_snapshot_backfill_epoch(
                            &mut fragment.nodes,
                            Some(snapshot_backfill_info),
                            cross_db_snapshot_backfill_info,
                        ) {
                            warn!(e = %e.as_report(), "failed to fill snapshot backfill epoch");
                            for notifier in notifiers {
                                notifier.notify_start_failed(e.clone());
                            }
                            return Ok(());
                        };
                    }
                    let job_id = info.stream_job_fragments.stream_job_id();
                    let snapshot_backfill_upstream_tables = snapshot_backfill_info
                        .upstream_mv_table_id_to_backfill_epoch
                        .keys()
                        .cloned()
                        .collect();

                    self.creating_streaming_job_controls.insert(
                        job_id,
                        CreatingStreamingJobControl::new(
                            info,
                            snapshot_backfill_upstream_tables,
                            barrier_info.prev_epoch(),
                            hummock_version_stats,
                            control_stream_manager,
                            edges.as_mut().expect("should exist"),
                        )?,
                    );
                }
            }
        }

        // Collect the jobs to finish
        if let (BarrierKind::Checkpoint(_), None) = (&barrier_info.kind, &command)
            && let Some(jobs_to_merge) = self.jobs_to_merge()
        {
            command = Some(Command::MergeSnapshotBackfillStreamingJobs(jobs_to_merge));
        }

        let command = command;

        let (
            pre_applied_graph_info,
            pre_applied_subscription_info,
            table_ids_to_commit,
            jobs_to_wait,
            prev_paused_reason,
        ) = self.state.apply_command(command.as_ref());

        // Tracing related stuff
        barrier_info.prev_epoch.span().in_scope(|| {
            tracing::info!(target: "rw_tracing", epoch = barrier_info.curr_epoch.value().0, "new barrier enqueued");
        });
        span.record("epoch", barrier_info.curr_epoch.value().0);

        for creating_job in &mut self.creating_streaming_job_controls.values_mut() {
            creating_job.on_new_command(control_stream_manager, command.as_ref(), &barrier_info)?;
        }

        let node_to_collect = match control_stream_manager.inject_command_ctx_barrier(
            self.database_id,
            command.as_ref(),
            &barrier_info,
            prev_paused_reason,
            &pre_applied_graph_info,
            &self.state.inflight_graph_info,
            &mut edges,
        ) {
            Ok(node_to_collect) => node_to_collect,
            Err(err) => {
                for notifier in notifiers {
                    notifier.notify_start_failed(err.clone());
                }
                fail_point!("inject_barrier_err_success");
                return Err(err);
            }
        };

        // Notify about the injection.
        notifiers.iter_mut().for_each(|n| n.notify_started());

        let command_ctx = CommandContext::new(
            barrier_info,
            pre_applied_subscription_info,
            table_ids_to_commit.clone(),
            command,
            span,
        );

        // Record the in-flight barrier.
        self.enqueue_command(command_ctx, notifiers, node_to_collect, jobs_to_wait);

        Ok(())
    }
}
