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

use std::collections::{BTreeMap, HashMap, HashSet, VecDeque};
use std::sync::Arc;
use std::time::Instant;

use anyhow::anyhow;
use iceberg::scan::FileScanTask;
use iceberg::spec::{
    DataContentType, DataFile, DataFileBuilder, DataFileFormat, MAIN_BRANCH, Operation,
    SerializedDataFile, Struct, StructType,
};
use iceberg::table::Table;
use iceberg::transaction::{ApplyTransactionAction, Transaction};
use iceberg_compaction_core::compaction::{
    CommitConsistencyParams, CommitManagerRetryConfig, CompactionBuilder, CompactionPlan,
    CompactionPlanner,
};
use iceberg_compaction_core::config::{
    CompactionPlanningConfig, FilesWithDeletesConfigBuilder, FullCompactionConfigBuilder,
    GroupFilters, SmallFilesConfigBuilder,
};
use itertools::Itertools;
use parking_lot::RwLock;
use risingwave_common::id::WorkerId;
use risingwave_connector::connector_common::IcebergSinkCompactionUpdate;
use risingwave_connector::sink::catalog::{SinkCatalog, SinkId};
use risingwave_connector::sink::iceberg::{
    CompactionType, IcebergConfig, commit_branch, should_enable_iceberg_cow,
};
use risingwave_connector::sink::{SinkError, SinkParam};
use risingwave_pb::iceberg_compaction::file_scan_task::{FileContent, FileFormat};
use risingwave_pb::iceberg_compaction::iceberg_compaction_task::TaskType;
use risingwave_pb::iceberg_compaction::subscribe_iceberg_compaction_event_request::ReportPlan;
use risingwave_pb::iceberg_compaction::{
    FileGroup, FileScanTask as PbFileScanTask, Plan, PlanKey,
    SerializedDataFile as PbSerializedDataFile, SubscribeIcebergCompactionEventRequest,
    subscribe_iceberg_compaction_event_response,
};
use thiserror_ext::AsReport;
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender};
use tokio::sync::oneshot::Sender;
use tokio::task::JoinHandle;
use tonic::Streaming;

use super::MetaSrvEnv;
use crate::MetaResult;
use crate::hummock::{
    IcebergCompactionEventDispatcher, IcebergCompactionEventHandler, IcebergCompactionEventLoop,
    IcebergCompactor, IcebergCompactorManagerRef,
};
use crate::manager::MetadataManager;
use crate::rpc::metrics::MetaMetrics;

pub type IcebergCompactionManagerRef = std::sync::Arc<IcebergCompactionManager>;

type CompactorChangeTx =
    UnboundedSender<(WorkerId, Streaming<SubscribeIcebergCompactionEventRequest>)>;

type CompactorChangeRx =
    UnboundedReceiver<(WorkerId, Streaming<SubscribeIcebergCompactionEventRequest>)>;

const ICEBERG_PLAN_MAX_RETRY_TIMES: u32 = 3;

/// Snapshot for restoring track state on failure
#[derive(Debug, Clone)]
struct CompactionTrackSnapshot {
    next_compaction_time: Option<Instant>,
}

/// Compaction track states using type-safe state machine pattern
#[derive(Debug, Clone)]
enum CompactionTrackState {
    /// Ready to accept commits and check for trigger conditions
    Idle { next_compaction_time: Instant },
    /// Compaction task is being processed
    Processing,
}

#[derive(Debug, Clone)]
struct CompactionTrack {
    task_type: TaskType,
    trigger_interval_sec: u64,
    /// Minimum snapshot count threshold to trigger compaction (early trigger).
    /// Compaction triggers when `pending_snapshot_count` >= this threshold, even before interval expires.
    trigger_snapshot_count: usize,
    state: CompactionTrackState,
}

impl CompactionTrack {
    /// Determines if compaction should be triggered.
    ///
    /// Trigger conditions (OR logic):
    /// 1. `snapshot_ready` - Snapshot count >= threshold (early trigger)
    /// 2. `time_ready && has_snapshots` - Interval expired and there's at least 1 snapshot
    ///
    /// This ensures:
    /// - `trigger_snapshot_count` is an early trigger threshold
    /// - `compaction_interval_sec` is the maximum wait time (as long as there are new snapshots)
    /// - Force compaction works by setting `next_compaction_time` to now
    /// - No empty compaction runs (requires at least 1 snapshot for time-based trigger)
    fn should_trigger(&self, now: Instant, snapshot_count: usize) -> bool {
        // Only Idle state can trigger
        let next_compaction_time = match &self.state {
            CompactionTrackState::Idle {
                next_compaction_time,
            } => *next_compaction_time,
            CompactionTrackState::Processing => return false,
        };

        // Check conditions
        let time_ready = now >= next_compaction_time;
        let snapshot_ready = snapshot_count >= self.trigger_snapshot_count;
        let has_snapshots = snapshot_count > 0;

        // OR logic: snapshot threshold triggers early,
        // time trigger requires at least 1 snapshot to avoid empty compaction
        snapshot_ready || (time_ready && has_snapshots)
    }

    /// Create snapshot and transition to Processing state
    fn start_processing(&mut self) -> CompactionTrackSnapshot {
        match &self.state {
            CompactionTrackState::Idle {
                next_compaction_time,
            } => {
                let snapshot = CompactionTrackSnapshot {
                    next_compaction_time: Some(*next_compaction_time),
                };
                self.state = CompactionTrackState::Processing;
                snapshot
            }
            CompactionTrackState::Processing => {
                unreachable!("Cannot start processing when already processing")
            }
        }
    }

    /// Complete processing successfully
    fn complete_processing(&mut self) {
        match &self.state {
            CompactionTrackState::Processing => {
                self.state = CompactionTrackState::Idle {
                    next_compaction_time: Instant::now()
                        + std::time::Duration::from_secs(self.trigger_interval_sec),
                };
            }
            CompactionTrackState::Idle { .. } => {
                unreachable!("Cannot complete processing when not processing")
            }
        }
    }

    /// Restore from snapshot on failure
    fn restore_from_snapshot(&mut self, snapshot: CompactionTrackSnapshot) {
        self.state = CompactionTrackState::Idle {
            next_compaction_time: snapshot.next_compaction_time.unwrap_or_else(Instant::now),
        };
    }

    fn update_interval(&mut self, new_interval_sec: u64, now: Instant) {
        if self.trigger_interval_sec == new_interval_sec {
            return;
        }

        self.trigger_interval_sec = new_interval_sec;

        // Reset next_compaction_time based on current state
        match &mut self.state {
            CompactionTrackState::Idle {
                next_compaction_time,
            } => {
                *next_compaction_time = now + std::time::Duration::from_secs(new_interval_sec);
            }
            CompactionTrackState::Processing => {
                // Keep Processing state, will reset time when completing
            }
        }
    }
}

// Removed CompactionScheduleState - each sink now only has one CompactionTrack
pub struct IcebergCompactionHandle {
    sink_id: SinkId,
    task_type: TaskType,
    inner: Arc<RwLock<IcebergCompactionManagerInner>>,
    metadata_manager: MetadataManager,
    handle_success: bool,

    /// Snapshot of the compaction track for recovery.
    track_snapshot: CompactionTrackSnapshot,
}

impl IcebergCompactionHandle {
    fn new(
        sink_id: SinkId,
        task_type: TaskType,
        inner: Arc<RwLock<IcebergCompactionManagerInner>>,
        metadata_manager: MetadataManager,
        track_snapshot: CompactionTrackSnapshot,
    ) -> Self {
        Self {
            sink_id,
            task_type,
            inner,
            metadata_manager,
            handle_success: false,
            track_snapshot,
        }
    }

    pub(crate) async fn build_plan_tasks(
        &self,
        task_id: u64,
        attempt: u32,
    ) -> MetaResult<Vec<subscribe_iceberg_compaction_event_response::PlanTask>> {
        let Some(prost_sink_catalog) = self
            .metadata_manager
            .catalog_controller
            .get_sink_by_id(self.sink_id)
            .await?
        else {
            tracing::warn!("Sink not found: {}", self.sink_id);
            return Err(anyhow!("Sink not found: {}", self.sink_id).into());
        };
        let sink_catalog = SinkCatalog::from(prost_sink_catalog);
        let param = SinkParam::try_from_sink_catalog(sink_catalog)?;
        let iceberg_config = IcebergConfig::from_btreemap(param.properties.clone())?;
        let catalog = iceberg_config
            .create_catalog()
            .await
            .map_err(|e| SinkError::Iceberg(e.into()))?;
        let table_ident = iceberg_config.full_table_name()?;

        let planning_config = build_planning_config(&iceberg_config, self.task_type)?;
        let planner = CompactionPlanner::new(planning_config);
        let to_branch = commit_branch(iceberg_config.r#type.as_str(), iceberg_config.write_mode);
        let table = catalog
            .load_table(&table_ident)
            .await
            .map_err(|e| SinkError::Iceberg(e.into()))?;
        let compaction_plans = planner
            .plan_compaction_with_branch(&table, &to_branch)
            .await
            .map_err(|e| SinkError::Iceberg(e.into()))?;

        if compaction_plans.is_empty() {
            tracing::info!(
                sink_id = %self.sink_id,
                task_id = task_id,
                attempt = attempt,
                task_type = ?self.task_type,
                table = %table_ident,
                to_branch = %to_branch,
                "【iceberg compaction】planner returned empty plan list"
            );
            return Ok(vec![]);
        }

        tracing::info!(
            sink_id = %self.sink_id,
            task_id = task_id,
            attempt = attempt,
            task_type = ?self.task_type,
            table = %table_ident,
            to_branch = %to_branch,
            plan_count = compaction_plans.len(),
            "【iceberg compaction】planner generated plans"
        );

        let mut plan_tasks = Vec::with_capacity(compaction_plans.len());
        for (plan_index, plan) in compaction_plans.into_iter().enumerate() {
            let plan_pb = encode_compaction_plan(
                plan,
                param.properties.clone(),
                table_ident.to_string(),
                self.task_type as i32,
            )?;
            tracing::debug!(
                sink_id = %self.sink_id,
                task_id = task_id,
                plan_index = plan_index,
                attempt = attempt,
                required_parallelism = plan_pb
                    .file_group
                    .as_ref()
                    .map(|fg| fg.executor_parallelism)
                    .unwrap_or(1),
                "【iceberg compaction】encoded plan task"
            );
            plan_tasks.push(subscribe_iceberg_compaction_event_response::PlanTask {
                key: Some(PlanKey {
                    task_id,
                    plan_index: plan_index as u32,
                }),
                required_parallelism: plan_pb
                    .file_group
                    .as_ref()
                    .map(|fg| fg.executor_parallelism)
                    .unwrap_or(1),
                plan: Some(plan_pb),
                attempt,
            });
        }

        Ok(plan_tasks)
    }

    pub async fn send_compact_task(
        mut self,
        compactor: Arc<IcebergCompactor>,
        task_id: u64,
    ) -> MetaResult<()> {
        use risingwave_pb::iceberg_compaction::subscribe_iceberg_compaction_event_response::Event as IcebergResponseEvent;
        let plan_tasks = self.build_plan_tasks(task_id, 0).await?;
        if !plan_tasks.is_empty() {
            let cow_task = if let Some(plan) = plan_tasks
                .first()
                .and_then(|plan_task| plan_task.plan.as_ref())
            {
                let iceberg_config = IcebergConfig::from_btreemap(BTreeMap::from_iter(
                    plan.props.clone().into_iter(),
                ))?;
                should_enable_iceberg_cow(iceberg_config.r#type.as_str(), iceberg_config.write_mode)
            } else {
                false
            };
            let mut guard = self.inner.write();
            if cow_task {
                guard.task_plan_counts.insert(task_id, plan_tasks.len());
            }
            let assigned_at = Instant::now();
            for plan_task in &plan_tasks {
                let Some(key) = plan_task.key.as_ref() else {
                    continue;
                };
                let assignment = PlanAssignment {
                    key: (key.task_id, key.plan_index),
                    attempt: plan_task.attempt,
                    plan_task: plan_task.clone(),
                    assigned_at: Some(assigned_at),
                };
                guard.in_flight_plans.insert(assignment.key, assignment);
                tracing::info!(
                    sink_id = %self.sink_id,
                    task_id = key.task_id,
                    plan_index = key.plan_index,
                    attempt = plan_task.attempt,
                    in_flight_count = guard.in_flight_plans.len(),
                    "【iceberg compaction】registered plan task in in_flight_plans"
                );
            }
        }

        let mut result = Ok(());
        for plan_task in plan_tasks {
            if let Some(key) = plan_task.key.as_ref() {
                tracing::info!(
                    sink_id = %self.sink_id,
                    task_id = key.task_id,
                    plan_index = key.plan_index,
                    attempt = plan_task.attempt,
                    required_parallelism = plan_task.required_parallelism,
                    "【iceberg compaction】sending plan task to compactor"
                );
            }
            result = compactor.send_event(IcebergResponseEvent::PlanTask(plan_task));
            if result.is_err() {
                break;
            }
        }

        if result.is_ok() {
            self.handle_success = true;
        }

        result
    }

    pub fn sink_id(&self) -> SinkId {
        self.sink_id
    }
}

impl Drop for IcebergCompactionHandle {
    fn drop(&mut self) {
        let mut guard = self.inner.write();
        if let Some(track) = guard.sink_schedules.get_mut(&self.sink_id) {
            // Only restore/complete if this handle's task_type matches the track's task_type
            if track.task_type == self.task_type {
                if self.handle_success {
                    track.complete_processing();
                } else {
                    // If the handle is not successful, we need to restore the track from snapshot.
                    // This is to avoid the case where the handle is dropped before the compaction task is sent.
                    track.restore_from_snapshot(self.track_snapshot.clone());
                }
            }
        }
    }
}

struct IcebergCompactionManagerInner {
    pub sink_schedules: HashMap<SinkId, CompactionTrack>,
    pub pending_plans: VecDeque<PlanAssignment>,
    pub in_flight_plans: HashMap<(u64, u32), PlanAssignment>,
    pub task_plan_counts: HashMap<u64, usize>,
    pub cow_task_aggregations: HashMap<u64, CowTaskAggregation>,
    pub cow_task_failed: HashSet<u64>,
}

#[derive(Debug, Clone)]
struct CowTaskAggregation {
    expected_plan_count: usize,
    results: HashMap<u32, Vec<DataFile>>,
}

impl CowTaskAggregation {
    fn new(expected_plan_count: usize) -> Self {
        Self {
            expected_plan_count,
            results: HashMap::new(),
        }
    }
}

pub struct IcebergCompactionManager {
    pub env: MetaSrvEnv,
    inner: Arc<RwLock<IcebergCompactionManagerInner>>,

    metadata_manager: MetadataManager,
    pub iceberg_compactor_manager: IcebergCompactorManagerRef,

    compactor_streams_change_tx: CompactorChangeTx,

    pub metrics: Arc<MetaMetrics>,
}

impl IcebergCompactionManager {
    pub fn build(
        env: MetaSrvEnv,
        metadata_manager: MetadataManager,
        iceberg_compactor_manager: IcebergCompactorManagerRef,
        metrics: Arc<MetaMetrics>,
    ) -> (Arc<Self>, CompactorChangeRx) {
        let (compactor_streams_change_tx, compactor_streams_change_rx) =
            tokio::sync::mpsc::unbounded_channel();
        (
            Arc::new(Self {
                env,
                inner: Arc::new(RwLock::new(IcebergCompactionManagerInner {
                    sink_schedules: HashMap::default(),
                    pending_plans: VecDeque::default(),
                    in_flight_plans: HashMap::default(),
                    task_plan_counts: HashMap::default(),
                    cow_task_aggregations: HashMap::default(),
                    cow_task_failed: HashSet::default(),
                })),
                metadata_manager,
                iceberg_compactor_manager,
                compactor_streams_change_tx,
                metrics,
            }),
            compactor_streams_change_rx,
        )
    }

    pub fn compaction_stat_loop(
        manager: Arc<Self>,
        mut rx: UnboundedReceiver<IcebergSinkCompactionUpdate>,
    ) -> (JoinHandle<()>, Sender<()>) {
        let (shutdown_tx, mut shutdown_rx) = tokio::sync::oneshot::channel();
        let join_handle = tokio::spawn(async move {
            loop {
                tokio::select! {
                    Some(stat) = rx.recv() => {
                        manager.update_iceberg_commit_info(stat).await;
                    },
                    _ = &mut shutdown_rx => {
                        tracing::info!("Iceberg compaction manager is stopped");
                        return;
                    }
                }
            }
        });

        (join_handle, shutdown_tx)
    }

    pub async fn update_iceberg_commit_info(&self, msg: IcebergSinkCompactionUpdate) {
        let IcebergSinkCompactionUpdate {
            sink_id,
            compaction_interval,
            force_compaction,
        } = msg;

        // Check if track exists
        let track_exists = {
            let guard = self.inner.read();
            guard.sink_schedules.contains_key(&sink_id)
        };

        // Create track if it doesn't exist
        if !track_exists {
            // Load config first (async operation outside of lock)
            let iceberg_config = self.load_iceberg_config(sink_id).await;

            let new_track = match iceberg_config {
                Ok(config) => {
                    // Call synchronous create function with the config
                    match self.create_compaction_track(sink_id, &config) {
                        Ok(track) => track,
                        Err(e) => {
                            tracing::error!(
                                error = ?e.as_report(),
                                "Failed to create compaction track from config for sink {}, using default Full track",
                                sink_id
                            );
                            // Fallback to default Full track
                            CompactionTrack {
                                task_type: TaskType::Full,
                                trigger_interval_sec: compaction_interval,
                                trigger_snapshot_count: 10,
                                state: CompactionTrackState::Idle {
                                    next_compaction_time: Instant::now()
                                        + std::time::Duration::from_secs(compaction_interval),
                                },
                            }
                        }
                    }
                }
                Err(e) => {
                    tracing::error!(
                        error = ?e.as_report(),
                        "Failed to load iceberg config for sink {}, using default Full track",
                        sink_id
                    );
                    // Fallback to default Full track
                    CompactionTrack {
                        task_type: TaskType::Full,
                        trigger_interval_sec: compaction_interval,
                        trigger_snapshot_count: 10,
                        state: CompactionTrackState::Idle {
                            next_compaction_time: Instant::now()
                                + std::time::Duration::from_secs(compaction_interval),
                        },
                    }
                }
            };

            let mut guard = self.inner.write();
            guard.sink_schedules.insert(sink_id, new_track);
        }

        // Update track
        let mut guard = self.inner.write();
        if let Some(track) = guard.sink_schedules.get_mut(&sink_id) {
            // Force compaction: trigger immediately by setting next_compaction_time to now
            if force_compaction {
                if let CompactionTrackState::Idle {
                    next_compaction_time,
                } = &mut track.state
                {
                    *next_compaction_time = Instant::now();
                }
                // Skip Processing tracks - they will complete naturally
            } else {
                // Update interval if changed
                track.update_interval(compaction_interval, Instant::now());
            }
        } else {
            tracing::error!(
                "Failed to find compaction track for sink {} during update; configuration changes not applied.",
                sink_id
            );
        }
    }

    /// Create a compaction track for a sink based on its Iceberg configuration
    fn create_compaction_track(
        &self,
        _sink_id: SinkId,
        iceberg_config: &IcebergConfig,
    ) -> MetaResult<CompactionTrack> {
        let trigger_interval_sec = iceberg_config.compaction_interval_sec();
        let trigger_snapshot_count = iceberg_config.trigger_snapshot_count();

        // For `copy-on-write` mode, always use Full compaction regardless of config
        let task_type =
            if should_enable_iceberg_cow(iceberg_config.r#type.as_str(), iceberg_config.write_mode)
            {
                TaskType::Full
            } else {
                // For `merge-on-read` mode, use configured compaction_type
                match iceberg_config.compaction_type() {
                    CompactionType::Full => TaskType::Full,
                    CompactionType::SmallFiles => TaskType::SmallFiles,
                    CompactionType::FilesWithDelete => TaskType::FilesWithDelete,
                }
            };

        Ok(CompactionTrack {
            task_type,
            trigger_interval_sec,
            trigger_snapshot_count,
            state: CompactionTrackState::Idle {
                next_compaction_time: Instant::now()
                    + std::time::Duration::from_secs(trigger_interval_sec),
            },
        })
    }

    /// Get the top N compaction tasks to trigger
    /// Returns handles for tasks that are ready to be compacted
    /// Sorted by commit count and next compaction time
    pub async fn get_top_n_iceberg_commit_sink_ids(
        &self,
        n: usize,
    ) -> Vec<IcebergCompactionHandle> {
        let now = Instant::now();

        // Collect all sink_ids to check
        let sink_ids: Vec<SinkId> = {
            let guard = self.inner.read();
            guard.sink_schedules.keys().cloned().collect()
        };

        // Fetch snapshot counts for all sinks in parallel
        let snapshot_count_futures = sink_ids
            .iter()
            .map(|sink_id| async move {
                let count = self.get_pending_snapshot_count(*sink_id).await?;
                Some((*sink_id, count))
            })
            .collect::<Vec<_>>();

        let snapshot_counts: HashMap<SinkId, usize> =
            futures::future::join_all(snapshot_count_futures)
                .await
                .into_iter()
                .flatten()
                .collect();

        let mut guard = self.inner.write();

        // Collect all triggerable tasks with their priority info
        let mut candidates = Vec::new();
        for (sink_id, track) in &guard.sink_schedules {
            // Skip sinks that failed to get snapshot count
            let Some(&snapshot_count) = snapshot_counts.get(sink_id) else {
                continue;
            };
            if track.should_trigger(now, snapshot_count) {
                // Extract next_time from Idle state (triggerable means Idle)
                if let CompactionTrackState::Idle {
                    next_compaction_time,
                } = track.state
                {
                    candidates.push((*sink_id, track.task_type, next_compaction_time));
                }
            }
        }

        // Sort by next_compaction_time (ascending) - earlier times have higher priority
        candidates.sort_by(|a, b| a.2.cmp(&b.2));

        // Take top N and create handles
        candidates
            .into_iter()
            .take(n)
            .filter_map(|(sink_id, task_type, _)| {
                let track = guard.sink_schedules.get_mut(&sink_id)?;

                let track_snapshot = track.start_processing();

                Some(IcebergCompactionHandle::new(
                    sink_id,
                    task_type,
                    self.inner.clone(),
                    self.metadata_manager.clone(),
                    track_snapshot,
                ))
            })
            .collect()
    }

    pub fn clear_iceberg_commits_by_sink_id(&self, sink_id: SinkId) {
        let mut guard = self.inner.write();
        guard.sink_schedules.remove(&sink_id);
    }

    pub async fn get_sink_param(&self, sink_id: SinkId) -> MetaResult<SinkParam> {
        let prost_sink_catalog = self
            .metadata_manager
            .catalog_controller
            .get_sink_by_id(sink_id)
            .await?
            .ok_or_else(|| anyhow!("Sink not found: {}", sink_id))?;
        let sink_catalog = SinkCatalog::from(prost_sink_catalog);
        let param = SinkParam::try_from_sink_catalog(sink_catalog)?;
        Ok(param)
    }

    pub async fn load_iceberg_config(&self, sink_id: SinkId) -> MetaResult<IcebergConfig> {
        let sink_param = self.get_sink_param(sink_id).await?;
        let iceberg_config = IcebergConfig::from_btreemap(sink_param.properties)?;
        Ok(iceberg_config)
    }

    pub fn add_compactor_stream(
        &self,
        context_id: WorkerId,
        req_stream: Streaming<SubscribeIcebergCompactionEventRequest>,
    ) {
        self.compactor_streams_change_tx
            .send((context_id, req_stream))
            .unwrap();
    }

    pub async fn report_plan(&self, report_plan: ReportPlan) -> MetaResult<()> {
        let Some(key) = report_plan.key.as_ref() else {
            tracing::warn!("Received iceberg plan report without key");
            return Ok(());
        };
        let key_tuple = (key.task_id, key.plan_index);
        let attempt = report_plan.attempt;

        if report_plan.status
            == risingwave_pb::iceberg_compaction::subscribe_iceberg_compaction_event_request::report_plan::Status::Failed as i32
        {
            tracing::warn!(
                task_id = key.task_id,
                plan_index = key.plan_index,
                attempt = attempt,
                error_message = %report_plan.error_message,
                "【iceberg compaction】received failed plan report"
            );
        } else {
            tracing::info!(
                task_id = key.task_id,
                plan_index = key.plan_index,
                attempt = attempt,
                "【iceberg compaction】received plan report"
            );
        }

        let plan_task = {
            let mut guard = self.inner.write();
            match guard.in_flight_plans.get(&key_tuple) {
                Some(plan) if plan.attempt == attempt => {
                    let plan_task = plan.plan_task.clone();
                    guard.in_flight_plans.remove(&key_tuple);
                    Some(plan_task)
                }
                Some(plan) => {
                    tracing::info!(
                        task_id = key.task_id,
                        plan_index = key.plan_index,
                        expected_attempt = plan.attempt,
                        got_attempt = attempt,
                        "Ignored stale iceberg plan report"
                    );
                    None
                }
                None => {
                    tracing::info!(
                        task_id = key.task_id,
                        plan_index = key.plan_index,
                        "Ignored unknown iceberg plan report"
                    );
                    None
                }
            }
        };

        if let Some(plan_task) = plan_task {
            tracing::info!(
                task_id = key.task_id,
                plan_index = key.plan_index,
                status = report_plan.status,
                "Accepted iceberg plan report"
            );
            if report_plan.status
                == risingwave_pb::iceberg_compaction::subscribe_iceberg_compaction_event_request::report_plan::Status::Success as i32
            {
                if let Some(result) = report_plan.result {
                    let Some(plan) = plan_task.plan.as_ref() else {
                        return Ok(());
                    };
                    let iceberg_config = IcebergConfig::from_btreemap(BTreeMap::from_iter(
                        plan.props.clone().into_iter(),
                    ))?;
                    let cow = should_enable_iceberg_cow(
                        iceberg_config.r#type.as_str(),
                        iceberg_config.write_mode,
                    );

                    if cow {
                        let table = self.load_table_for_plan(plan).await?;
                        let added_data_files =
                            decode_serialized_data_files(&result.added_data_files, &table)?;
                        let added_position_delete_files = decode_serialized_data_files(
                            &result.added_position_delete_files,
                            &table,
                        )?;
                        let added_equality_delete_files = decode_serialized_data_files(
                            &result.added_equality_delete_files,
                            &table,
                        )?;
                        let mut added_files = Vec::with_capacity(
                            added_data_files.len()
                                + added_position_delete_files.len()
                                + added_equality_delete_files.len(),
                        );
                        added_files.extend(added_data_files);
                        added_files.extend(added_position_delete_files);
                        added_files.extend(added_equality_delete_files);

                        let maybe_commit = {
                            let mut guard = self.inner.write();
                            if guard.cow_task_failed.contains(&key.task_id) {
                                tracing::warn!(
                                    task_id = key.task_id,
                                    plan_index = key.plan_index,
                                    "【iceberg compaction】skip cow aggregation due to previous failure"
                                );
                                None
                            } else {
                                let expected = guard
                                    .task_plan_counts
                                    .get(&key.task_id)
                                    .copied()
                                    .unwrap_or(1);
                                let entry = guard
                                    .cow_task_aggregations
                                    .entry(key.task_id)
                                    .or_insert_with(|| CowTaskAggregation::new(expected));
                                if let std::collections::hash_map::Entry::Vacant(slot) =
                                    entry.results.entry(key.plan_index)
                                {
                                    slot.insert(added_files);
                                    if entry.results.len() == entry.expected_plan_count {
                                        let mut all_added_files = Vec::new();
                                        for files in entry.results.values() {
                                            all_added_files.extend(files.clone());
                                        }
                                        guard.cow_task_aggregations.remove(&key.task_id);
                                        guard.task_plan_counts.remove(&key.task_id);
                                        Some(all_added_files)
                                    } else {
                                        None
                                    }
                                } else {
                                    tracing::info!(
                                        task_id = key.task_id,
                                        plan_index = key.plan_index,
                                        "【iceberg compaction】duplicate cow plan result ignored"
                                    );
                                    None
                                }
                            }
                        };

                        if let Some(all_added_files) = maybe_commit {
                            self.commit_cow_task(plan_task, all_added_files).await?;
                        }
                    } else {
                        if let Some(stats) = result.stats.as_ref() {
                            tracing::info!(
                                task_id = key.task_id,
                                plan_index = key.plan_index,
                                attempt = attempt,
                                input_data_files = stats.input_data_files,
                                input_delete_files = stats.input_delete_files,
                                output_files = stats.output_files,
                                output_bytes = stats.output_bytes,
                                added_data_files = result.added_data_files.len(),
                                "【iceberg compaction】accepted success report, start committing"
                            );
                        } else {
                            tracing::info!(
                                task_id = key.task_id,
                                plan_index = key.plan_index,
                                attempt = attempt,
                                added_data_files = result.added_data_files.len(),
                                "【iceberg compaction】accepted success report (no stats), start committing"
                            );
                        }
                        self.commit_plan(plan_task, result).await?;
                    }
                }
            } else {
                tracing::warn!(
                    task_id = key.task_id,
                    plan_index = key.plan_index,
                    error_message = %report_plan.error_message,
                    "【iceberg compaction】plan failed, dropping"
                );
                self.mark_task_failed(key.task_id);
            }
        }
        Ok(())
    }

    pub(crate) fn requeue_expired_plans(&self, now: Instant) {
        const ICEBERG_PLAN_TIMEOUT_SECS: u64 = 300;
        let mut guard = self.inner.write();
        let timeout = std::time::Duration::from_secs(ICEBERG_PLAN_TIMEOUT_SECS);
        let mut expired_keys = Vec::new();
        for (key, plan) in &guard.in_flight_plans {
            if let Some(assigned_at) = plan.assigned_at
                && now.duration_since(assigned_at) > timeout
            {
                expired_keys.push(*key);
            }
        }

        for key in expired_keys {
            if let Some(mut plan) = guard.in_flight_plans.remove(&key) {
                let next_attempt = plan.attempt.saturating_add(1);
                if next_attempt >= ICEBERG_PLAN_MAX_RETRY_TIMES {
                    tracing::warn!(
                        task_id = key.0,
                        plan_index = key.1,
                        attempt = plan.attempt,
                        max_retry_times = ICEBERG_PLAN_MAX_RETRY_TIMES,
                        "【iceberg compaction】plan timed out, retry limit reached, dropping"
                    );
                    guard.cow_task_failed.insert(key.0);
                    guard.cow_task_aggregations.remove(&key.0);
                    guard.task_plan_counts.remove(&key.0);
                    continue;
                }
                tracing::warn!(
                    task_id = key.0,
                    plan_index = key.1,
                    old_attempt = plan.attempt,
                    new_attempt = next_attempt,
                    max_retry_times = ICEBERG_PLAN_MAX_RETRY_TIMES,
                    "【iceberg compaction】plan timed out, requeueing"
                );
                plan.attempt = next_attempt;
                plan.plan_task.attempt = plan.attempt;
                plan.assigned_at = None;
                guard.pending_plans.push_back(plan);
            }
        }
    }

    pub(crate) fn enqueue_plan(
        &self,
        plan_task: subscribe_iceberg_compaction_event_response::PlanTask,
    ) {
        let Some(key) = plan_task.key.as_ref() else {
            return;
        };
        tracing::debug!(
            task_id = key.task_id,
            plan_index = key.plan_index,
            attempt = plan_task.attempt,
            required_parallelism = plan_task.required_parallelism,
            "【iceberg compaction】enqueue plan"
        );
        let plan = PlanAssignment {
            key: (key.task_id, key.plan_index),
            attempt: plan_task.attempt,
            plan_task,
            assigned_at: None,
        };
        let mut guard = self.inner.write();
        guard.pending_plans.push_back(plan);
    }

    pub(crate) fn take_next_plans(
        &self,
        count: usize,
    ) -> Vec<subscribe_iceberg_compaction_event_response::PlanTask> {
        let mut guard = self.inner.write();
        let mut ret = Vec::with_capacity(count);
        for _ in 0..count {
            if let Some(mut plan) = guard.pending_plans.pop_front() {
                plan.assigned_at = Some(Instant::now());
                guard.in_flight_plans.insert(plan.key, plan.clone());
                tracing::info!(
                    task_id = plan.key.0,
                    plan_index = plan.key.1,
                    attempt = plan.attempt,
                    required_parallelism = plan.plan_task.required_parallelism,
                    "【iceberg compaction】assigned plan to compactor (moved pending -> in_flight)"
                );
                ret.push(plan.plan_task);
            } else {
                break;
            }
        }
        ret
    }

    pub(crate) fn is_pending_empty(&self) -> bool {
        let guard = self.inner.read();
        guard.pending_plans.is_empty()
    }

    async fn commit_plan(
        &self,
        plan_task: subscribe_iceberg_compaction_event_response::PlanTask,
        result: risingwave_pb::iceberg_compaction::PlanResult,
    ) -> MetaResult<()> {
        let Some(plan) = plan_task.plan else {
            return Ok(());
        };
        let iceberg_config =
            IcebergConfig::from_btreemap(BTreeMap::from_iter(plan.props.clone().into_iter()))?;
        let catalog = iceberg_config
            .create_catalog()
            .await
            .map_err(|e| SinkError::Iceberg(e.into()))?;
        let table_ident = iceberg_config.full_table_name()?;
        let table = catalog
            .load_table(&table_ident)
            .await
            .map_err(|e| SinkError::Iceberg(e.into()))?;

        let added_data_files = decode_serialized_data_files(&result.added_data_files, &table)?;
        let added_position_delete_files =
            decode_serialized_data_files(&result.added_position_delete_files, &table)?;
        let added_equality_delete_files =
            decode_serialized_data_files(&result.added_equality_delete_files, &table)?;
        let mut added_files = Vec::with_capacity(
            added_data_files.len()
                + added_position_delete_files.len()
                + added_equality_delete_files.len(),
        );
        added_files.extend(added_data_files);
        added_files.extend(added_position_delete_files);
        added_files.extend(added_equality_delete_files);

        let rewritten_data_files = match plan.file_group {
            Some(file_group) => build_rewritten_data_files(file_group, &table)?,
            None => vec![],
        };

        let consistency_params = CommitConsistencyParams {
            starting_snapshot_id: plan.snapshot_id,
            use_starting_sequence_number: true,
            basic_schema_id: table.metadata().current_schema().schema_id(),
        };

        let compaction = CompactionBuilder::new(catalog.clone(), table_ident.clone())
            .with_catalog_name(iceberg_config.catalog_name())
            .with_executor_type(iceberg_compaction_core::executor::ExecutorType::DataFusion)
            .with_retry_config(CommitManagerRetryConfig::default())
            .with_to_branch(plan.to_branch.clone())
            .build();
        let commit_manager = compaction.build_commit_manager(consistency_params);

        let cow =
            should_enable_iceberg_cow(iceberg_config.r#type.as_str(), iceberg_config.write_mode);
        tracing::info!(
            task_id = plan_task
                .key
                .as_ref()
                .map(|k| k.task_id)
                .unwrap_or_default(),
            plan_index = plan_task
                .key
                .as_ref()
                .map(|k| k.plan_index)
                .unwrap_or_default(),
            attempt = plan_task.attempt,
            table = %table_ident,
            cow = cow,
            to_branch = %plan.to_branch,
            snapshot_id = plan.snapshot_id,
            added_files = added_files.len(),
            added_position_delete_files = result.added_position_delete_files.len(),
            added_equality_delete_files = result.added_equality_delete_files.len(),
            rewritten_data_files = rewritten_data_files.len(),
            "【iceberg compaction】start committing plan result"
        );

        if should_enable_iceberg_cow(iceberg_config.r#type.as_str(), iceberg_config.write_mode) {
            let input_files = collect_main_branch_data_files(&table).await?;
            commit_manager
                .overwrite_files(added_files, input_files, MAIN_BRANCH)
                .await
                .map_err(|e| SinkError::Iceberg(e.into()))?;
        } else {
            commit_manager
                .rewrite_files(added_files, rewritten_data_files, &plan.to_branch)
                .await
                .map_err(|e| SinkError::Iceberg(e.into()))?;
        }

        tracing::info!(
            task_id = plan_task
                .key
                .as_ref()
                .map(|k| k.task_id)
                .unwrap_or_default(),
            plan_index = plan_task
                .key
                .as_ref()
                .map(|k| k.plan_index)
                .unwrap_or_default(),
            attempt = plan_task.attempt,
            table = %table_ident,
            "【iceberg compaction】commit finished"
        );

        Ok(())
    }

    async fn load_table_for_plan(&self, plan: &Plan) -> MetaResult<Table> {
        let iceberg_config =
            IcebergConfig::from_btreemap(BTreeMap::from_iter(plan.props.clone().into_iter()))?;
        let catalog = iceberg_config
            .create_catalog()
            .await
            .map_err(|e| SinkError::Iceberg(e.into()))?;
        let table_ident = iceberg_config.full_table_name()?;
        Ok(catalog
            .load_table(&table_ident)
            .await
            .map_err(|e| SinkError::Iceberg(e.into()))?)
    }

    fn mark_task_failed(&self, task_id: u64) {
        let mut guard = self.inner.write();
        guard.cow_task_failed.insert(task_id);
        guard.cow_task_aggregations.remove(&task_id);
        guard.task_plan_counts.remove(&task_id);
    }

    async fn commit_cow_task(
        &self,
        plan_task: subscribe_iceberg_compaction_event_response::PlanTask,
        added_files: Vec<DataFile>,
    ) -> MetaResult<()> {
        let Some(plan) = plan_task.plan else {
            return Ok(());
        };
        let iceberg_config =
            IcebergConfig::from_btreemap(BTreeMap::from_iter(plan.props.clone().into_iter()))?;
        let catalog = iceberg_config
            .create_catalog()
            .await
            .map_err(|e| SinkError::Iceberg(e.into()))?;
        let table_ident = iceberg_config.full_table_name()?;
        let table = catalog
            .load_table(&table_ident)
            .await
            .map_err(|e| SinkError::Iceberg(e.into()))?;

        let consistency_params = CommitConsistencyParams {
            starting_snapshot_id: plan.snapshot_id,
            use_starting_sequence_number: true,
            basic_schema_id: table.metadata().current_schema().schema_id(),
        };

        let compaction = CompactionBuilder::new(catalog.clone(), table_ident.clone())
            .with_catalog_name(iceberg_config.catalog_name())
            .with_executor_type(iceberg_compaction_core::executor::ExecutorType::DataFusion)
            .with_retry_config(CommitManagerRetryConfig::default())
            .with_to_branch(plan.to_branch.clone())
            .build();
        let commit_manager = compaction.build_commit_manager(consistency_params);

        tracing::info!(
            task_id = plan_task
                .key
                .as_ref()
                .map(|k| k.task_id)
                .unwrap_or_default(),
            attempt = plan_task.attempt,
            table = %table_ident,
            cow = true,
            to_branch = %plan.to_branch,
            snapshot_id = plan.snapshot_id,
            added_files = added_files.len(),
            "【iceberg compaction】start committing cow task result (aggregated)"
        );

        let input_files = collect_main_branch_data_files(&table).await?;
        commit_manager
            .overwrite_files(added_files, input_files, MAIN_BRANCH)
            .await
            .map_err(|e| SinkError::Iceberg(e.into()))?;

        tracing::info!(
            task_id = plan_task
                .key
                .as_ref()
                .map(|k| k.task_id)
                .unwrap_or_default(),
            "【iceberg compaction】cow aggregated commit finished"
        );

        Ok(())
    }

    pub fn iceberg_compaction_event_loop(
        iceberg_compaction_manager: Arc<Self>,
        compactor_streams_change_rx: UnboundedReceiver<(
            WorkerId,
            Streaming<SubscribeIcebergCompactionEventRequest>,
        )>,
    ) -> Vec<(JoinHandle<()>, Sender<()>)> {
        let mut join_handle_vec = Vec::default();

        let iceberg_compaction_event_handler =
            IcebergCompactionEventHandler::new(iceberg_compaction_manager.clone());

        let iceberg_compaction_event_dispatcher =
            IcebergCompactionEventDispatcher::new(iceberg_compaction_event_handler);

        let event_loop = IcebergCompactionEventLoop::new(
            iceberg_compaction_event_dispatcher,
            iceberg_compaction_manager.metrics.clone(),
            compactor_streams_change_rx,
        );

        let (event_loop_join_handle, event_loop_shutdown_tx) = event_loop.run();
        join_handle_vec.push((event_loop_join_handle, event_loop_shutdown_tx));

        join_handle_vec
    }

    /// GC loop for expired snapshots management
    /// This is a separate loop that periodically checks all tracked Iceberg tables
    /// and performs garbage collection operations like expiring old snapshots
    pub fn gc_loop(manager: Arc<Self>, interval_sec: u64) -> (JoinHandle<()>, Sender<()>) {
        assert!(
            interval_sec > 0,
            "Iceberg GC interval must be greater than 0"
        );
        let (shutdown_tx, mut shutdown_rx) = tokio::sync::oneshot::channel();
        let join_handle = tokio::spawn(async move {
            tracing::info!(
                interval_sec = interval_sec,
                "Starting Iceberg GC loop with configurable interval"
            );
            let mut interval = tokio::time::interval(std::time::Duration::from_secs(interval_sec));

            loop {
                tokio::select! {
                    _ = interval.tick() => {
                        if let Err(e) = manager.perform_gc_operations().await {
                            tracing::error!(error = ?e.as_report(), "GC operations failed");
                        }
                    },
                    _ = &mut shutdown_rx => {
                        tracing::info!("Iceberg GC loop is stopped");
                        return;
                    }
                }
            }
        });

        (join_handle, shutdown_tx)
    }

    /// Trigger manual compaction for a specific sink and wait for completion
    /// This method records the initial snapshot, sends a compaction task, then waits for a new snapshot with replace operation
    pub async fn trigger_manual_compaction(&self, sink_id: SinkId) -> MetaResult<u64> {
        use risingwave_pb::iceberg_compaction::subscribe_iceberg_compaction_event_response::Event as IcebergResponseEvent;

        // Load the initial table state to get the current snapshot
        let iceberg_config = self.load_iceberg_config(sink_id).await?;
        let initial_table = iceberg_config.load_table().await?;
        let initial_snapshot_id = initial_table
            .metadata()
            .current_snapshot()
            .map(|s| s.snapshot_id())
            .unwrap_or(0); // Use 0 if no snapshots exist
        let initial_timestamp = chrono::Utc::now().timestamp_millis();

        // Get a compactor to send the task to
        let compactor = self
            .iceberg_compactor_manager
            .next_compactor()
            .ok_or_else(|| anyhow!("No iceberg compactor available"))?;

        // Generate a unique task ID
        let task_id = self
            .env
            .hummock_seq
            .next_interval("compaction_task", 1)
            .await?;

        let sink_param = self.get_sink_param(sink_id).await?;
        let iceberg_config = IcebergConfig::from_btreemap(sink_param.properties.clone())?;
        let catalog = iceberg_config
            .create_catalog()
            .await
            .map_err(|e| SinkError::Iceberg(e.into()))?;
        let table_ident = iceberg_config.full_table_name()?;
        let planning_config = build_planning_config(&iceberg_config, TaskType::Full)?;
        let planner = CompactionPlanner::new(planning_config);
        let to_branch = commit_branch(iceberg_config.r#type.as_str(), iceberg_config.write_mode);
        let table = catalog
            .load_table(&table_ident)
            .await
            .map_err(|e| SinkError::Iceberg(e.into()))?;
        let compaction_plans = planner
            .plan_compaction_with_branch(&table, &to_branch)
            .await
            .map_err(|e| SinkError::Iceberg(e.into()))?;

        if compaction_plans.is_empty() {
            tracing::info!(
                sink_id = %sink_id,
                task_id = task_id,
                task_type = ?TaskType::Full,
                table = %table_ident,
                to_branch = %to_branch,
                "【iceberg compaction】manual planner returned empty plan list"
            );
            return Ok(task_id);
        }

        let mut plan_tasks = Vec::with_capacity(compaction_plans.len());
        for (plan_index, plan) in compaction_plans.into_iter().enumerate() {
            let plan_pb = encode_compaction_plan(
                plan,
                sink_param.properties.clone(),
                table_ident.to_string(),
                TaskType::Full as i32,
            )?;
            plan_tasks.push(subscribe_iceberg_compaction_event_response::PlanTask {
                key: Some(PlanKey {
                    task_id,
                    plan_index: plan_index as u32,
                }),
                required_parallelism: plan_pb
                    .file_group
                    .as_ref()
                    .map(|fg| fg.executor_parallelism)
                    .unwrap_or(1),
                plan: Some(plan_pb),
                attempt: 0,
            });
        }

        if !plan_tasks.is_empty() {
            let cow_task = if let Some(plan) = plan_tasks
                .first()
                .and_then(|plan_task| plan_task.plan.as_ref())
            {
                let iceberg_config = IcebergConfig::from_btreemap(BTreeMap::from_iter(
                    plan.props.clone().into_iter(),
                ))?;
                should_enable_iceberg_cow(iceberg_config.r#type.as_str(), iceberg_config.write_mode)
            } else {
                false
            };
            let mut guard = self.inner.write();
            if cow_task {
                guard.task_plan_counts.insert(task_id, plan_tasks.len());
            }
            let assigned_at = Instant::now();
            for plan_task in &plan_tasks {
                let Some(key) = plan_task.key.as_ref() else {
                    continue;
                };
                let assignment = PlanAssignment {
                    key: (key.task_id, key.plan_index),
                    attempt: plan_task.attempt,
                    plan_task: plan_task.clone(),
                    assigned_at: Some(assigned_at),
                };
                guard.in_flight_plans.insert(assignment.key, assignment);
                tracing::info!(
                    sink_id = %sink_id,
                    task_id = key.task_id,
                    plan_index = key.plan_index,
                    attempt = plan_task.attempt,
                    in_flight_count = guard.in_flight_plans.len(),
                    "【iceberg compaction】registered manual plan task in in_flight_plans"
                );
            }
        }

        for plan_task in plan_tasks {
            if let Some(key) = plan_task.key.as_ref() {
                tracing::info!(
                    sink_id = %sink_id,
                    task_id = key.task_id,
                    plan_index = key.plan_index,
                    attempt = plan_task.attempt,
                    required_parallelism = plan_task.required_parallelism,
                    "【iceberg compaction】sending manual plan task to compactor"
                );
            }
            compactor.send_event(IcebergResponseEvent::PlanTask(plan_task))?;
        }

        tracing::info!(
            "Manual compaction triggered for sink {} with task ID {}, waiting for completion...",
            sink_id,
            task_id
        );

        self.wait_for_compaction_completion(
            &sink_id,
            iceberg_config,
            initial_snapshot_id,
            initial_timestamp,
            task_id,
        )
        .await?;

        Ok(task_id)
    }

    async fn wait_for_compaction_completion(
        &self,
        sink_id: &SinkId,
        iceberg_config: IcebergConfig,
        initial_snapshot_id: i64,
        initial_timestamp: i64,
        task_id: u64,
    ) -> MetaResult<()> {
        const INITIAL_POLL_INTERVAL_SECS: u64 = 2;
        const MAX_POLL_INTERVAL_SECS: u64 = 60;
        const MAX_WAIT_TIME_SECS: u64 = 1800;
        const BACKOFF_MULTIPLIER: f64 = 1.5;

        let mut elapsed_time = 0;
        let mut current_interval_secs = INITIAL_POLL_INTERVAL_SECS;

        let cow =
            should_enable_iceberg_cow(iceberg_config.r#type.as_str(), iceberg_config.write_mode);

        while elapsed_time < MAX_WAIT_TIME_SECS {
            let poll_interval = std::time::Duration::from_secs(current_interval_secs);
            tokio::time::sleep(poll_interval).await;
            elapsed_time += current_interval_secs;

            tracing::info!(
                "Checking iceberg compaction completion for sink {} task_id={}, elapsed={}s, interval={}s",
                sink_id,
                task_id,
                elapsed_time,
                current_interval_secs
            );

            let current_table = iceberg_config.load_table().await?;

            let metadata = current_table.metadata();
            let new_snapshots: Vec<_> = metadata
                .snapshots()
                .filter(|snapshot| {
                    let snapshot_timestamp = snapshot.timestamp_ms();
                    let snapshot_id = snapshot.snapshot_id();
                    snapshot_timestamp > initial_timestamp && snapshot_id != initial_snapshot_id
                })
                .collect();

            for snapshot in new_snapshots {
                let summary = snapshot.summary();
                if cow {
                    if matches!(summary.operation, Operation::Overwrite) {
                        tracing::info!(
                            "Iceberg compaction completed for sink {} task_id={} with Overwrite operation",
                            sink_id,
                            task_id
                        );
                        return Ok(());
                    }
                } else if matches!(summary.operation, Operation::Replace) {
                    tracing::info!(
                        "Iceberg compaction completed for sink {} task_id={} with Replace operation",
                        sink_id,
                        task_id
                    );
                    return Ok(());
                }
            }

            current_interval_secs = std::cmp::min(
                MAX_POLL_INTERVAL_SECS,
                ((current_interval_secs as f64) * BACKOFF_MULTIPLIER) as u64,
            );
        }

        Err(anyhow!(
            "Iceberg compaction did not complete within {} seconds for sink {} (task_id={})",
            MAX_WAIT_TIME_SECS,
            sink_id,
            task_id
        )
        .into())
    }

    async fn perform_gc_operations(&self) -> MetaResult<()> {
        let sink_ids = {
            let guard = self.inner.read();
            guard.sink_schedules.keys().cloned().collect::<Vec<_>>()
        };

        tracing::info!("Starting GC operations for {} tables", sink_ids.len());

        for sink_id in sink_ids {
            if let Err(e) = self.check_and_expire_snapshots(sink_id).await {
                tracing::error!(error = ?e.as_report(), "Failed to perform GC for sink {}", sink_id);
            }
        }

        tracing::info!("GC operations completed");
        Ok(())
    }

    /// Get the number of snapshots pending compaction for a sink's Iceberg table.
    /// Returns None if the table cannot be loaded.
    ///
    /// Counts snapshots since last compaction:
    /// - For COW mode: Counts snapshots on `ingestion` branch with timestamp > current snapshot on main
    /// - For MORE mode: Counts snapshots since last `Replace` on main branch
    async fn get_pending_snapshot_count(&self, sink_id: SinkId) -> Option<usize> {
        let iceberg_config = self.load_iceberg_config(sink_id).await.ok()?;
        let is_cow_mode =
            should_enable_iceberg_cow(iceberg_config.r#type.as_str(), iceberg_config.write_mode);
        let catalog = iceberg_config.create_catalog().await.ok()?;
        let table_name = iceberg_config.full_table_name().ok()?;
        let table = catalog.load_table(&table_name).await.ok()?;
        let metadata = table.metadata();

        if is_cow_mode {
            // COW mode: count snapshots on ingestion branch since last compaction.
            // Compaction writes Overwrite to main branch, so the current snapshot on main
            // is the last compaction point.

            // Get last compaction timestamp from main branch's current snapshot
            let last_compaction_timestamp = metadata
                .current_snapshot()
                .map(|s| s.timestamp_ms())
                .unwrap_or(0); // 0 means no compaction has happened yet

            // Count snapshots on ingestion branch with timestamp > last compaction
            let branch = commit_branch(iceberg_config.r#type.as_str(), iceberg_config.write_mode);
            let current_snapshot = metadata.snapshot_for_ref(&branch)?;

            let mut count = 0;
            let mut snapshot_id = Some(current_snapshot.snapshot_id());

            while let Some(id) = snapshot_id {
                let snapshot = metadata.snapshot_by_id(id)?;
                if snapshot.timestamp_ms() > last_compaction_timestamp {
                    count += 1;
                    snapshot_id = snapshot.parent_snapshot_id();
                } else {
                    // Reached snapshots before or at last compaction, stop counting
                    break;
                }
            }

            Some(count)
        } else {
            // MORE mode: simple rposition on all snapshots (main branch only)
            let mut snapshots = metadata.snapshots().collect_vec();
            if snapshots.is_empty() {
                return Some(0);
            }

            snapshots.sort_by_key(|s| s.timestamp_ms());

            let last_replace_index = snapshots
                .iter()
                .rposition(|s| matches!(s.summary().operation, Operation::Replace));

            let count = match last_replace_index {
                Some(index) => snapshots.len() - index - 1,
                None => snapshots.len(),
            };

            Some(count)
        }
    }

    pub async fn check_and_expire_snapshots(&self, sink_id: SinkId) -> MetaResult<()> {
        const MAX_SNAPSHOT_AGE_MS_DEFAULT: i64 = 24 * 60 * 60 * 1000; // 24 hours
        let now = chrono::Utc::now().timestamp_millis();

        let iceberg_config = self.load_iceberg_config(sink_id).await?;
        if !iceberg_config.enable_snapshot_expiration {
            return Ok(());
        }

        let catalog = iceberg_config.create_catalog().await?;
        let mut table = catalog
            .load_table(&iceberg_config.full_table_name()?)
            .await
            .map_err(|e| SinkError::Iceberg(e.into()))?;

        let metadata = table.metadata();
        let mut snapshots = metadata.snapshots().collect_vec();
        snapshots.sort_by_key(|s| s.timestamp_ms());

        let default_snapshot_expiration_timestamp_ms = now - MAX_SNAPSHOT_AGE_MS_DEFAULT;

        let snapshot_expiration_timestamp_ms =
            match iceberg_config.snapshot_expiration_timestamp_ms(now) {
                Some(timestamp) => timestamp,
                None => default_snapshot_expiration_timestamp_ms,
            };

        if snapshots.is_empty()
            || snapshots.first().unwrap().timestamp_ms() > snapshot_expiration_timestamp_ms
        {
            // avoid commit empty table updates
            return Ok(());
        }

        tracing::info!(
            catalog_name = iceberg_config.catalog_name(),
            table_name = iceberg_config.full_table_name()?.to_string(),
            %sink_id,
            snapshots_len = snapshots.len(),
            snapshot_expiration_timestamp_ms = snapshot_expiration_timestamp_ms,
            snapshot_expiration_retain_last = ?iceberg_config.snapshot_expiration_retain_last,
            clear_expired_files = ?iceberg_config.snapshot_expiration_clear_expired_files,
            clear_expired_meta_data = ?iceberg_config.snapshot_expiration_clear_expired_meta_data,
            "try trigger snapshots expiration",
        );

        let txn = Transaction::new(&table);

        let mut expired_snapshots = txn
            .expire_snapshot()
            .expire_older_than(snapshot_expiration_timestamp_ms)
            .clear_expire_files(iceberg_config.snapshot_expiration_clear_expired_files)
            .clear_expired_meta_data(iceberg_config.snapshot_expiration_clear_expired_meta_data);

        if let Some(retain_last) = iceberg_config.snapshot_expiration_retain_last {
            expired_snapshots = expired_snapshots.retain_last(retain_last);
        }

        let before_metadata = table.metadata_ref();
        let tx = expired_snapshots
            .apply(txn)
            .map_err(|e| SinkError::Iceberg(e.into()))?;
        table = tx
            .commit(catalog.as_ref())
            .await
            .map_err(|e| SinkError::Iceberg(e.into()))?;

        if iceberg_config.snapshot_expiration_clear_expired_files {
            table
                .cleanup_expired_files(&before_metadata)
                .await
                .map_err(|e| SinkError::Iceberg(e.into()))?;
        }

        tracing::info!(
            catalog_name = iceberg_config.catalog_name(),
            table_name = iceberg_config.full_table_name()?.to_string(),
            %sink_id,
            "Expired snapshots for iceberg table",
        );

        Ok(())
    }
}

fn build_planning_config(
    iceberg_config: &IcebergConfig,
    task_type: TaskType,
) -> MetaResult<CompactionPlanningConfig> {
    // TODO: wire real config from meta/cluster settings.
    let max_parallelism = 4usize;
    let min_size_per_partition = 1024u64 * 1024 * 1024;
    let max_file_count_per_partition = 32usize;
    let target_file_size_bytes = iceberg_config.target_file_size_mb() * 1024 * 1024;

    let group_filters = None::<GroupFilters>;

    let planning_config = match task_type {
        TaskType::SmallFiles => {
            let mut builder = SmallFilesConfigBuilder::default();
            builder
                .max_parallelism(max_parallelism)
                .min_size_per_partition(min_size_per_partition)
                .max_file_count_per_partition(max_file_count_per_partition)
                .target_file_size_bytes(target_file_size_bytes)
                .enable_heuristic_output_parallelism(true)
                .small_file_threshold_bytes(
                    iceberg_config.small_files_threshold_mb() * 1024 * 1024,
                );
            if let Some(filters) = group_filters {
                builder.group_filters(filters);
            }
            let config = builder.build().map_err(|e| SinkError::Iceberg(e.into()))?;
            CompactionPlanningConfig::SmallFiles(config)
        }
        TaskType::Full => {
            let config = FullCompactionConfigBuilder::default()
                .max_parallelism(max_parallelism)
                .min_size_per_partition(min_size_per_partition)
                .max_file_count_per_partition(max_file_count_per_partition)
                .target_file_size_bytes(target_file_size_bytes)
                .enable_heuristic_output_parallelism(true)
                .build()
                .map_err(|e| SinkError::Iceberg(e.into()))?;
            CompactionPlanningConfig::Full(config)
        }
        TaskType::FilesWithDelete => {
            let config = FilesWithDeletesConfigBuilder::default()
                .max_parallelism(max_parallelism)
                .min_size_per_partition(min_size_per_partition)
                .max_file_count_per_partition(max_file_count_per_partition)
                .target_file_size_bytes(target_file_size_bytes)
                .enable_heuristic_output_parallelism(true)
                .min_delete_file_count_threshold(iceberg_config.delete_files_count_threshold())
                .build()
                .map_err(|e| SinkError::Iceberg(e.into()))?;
            CompactionPlanningConfig::FilesWithDeletes(config)
        }
        _ => {
            return Err(anyhow!("Unsupported iceberg task type: {:?}", task_type).into());
        }
    };

    Ok(planning_config)
}

fn encode_compaction_plan(
    plan: CompactionPlan,
    props: std::collections::BTreeMap<String, String>,
    table_ident: String,
    task_type: i32,
) -> MetaResult<Plan> {
    let file_group = plan.file_group;
    let data_files = file_group
        .data_files
        .iter()
        .map(encode_file_scan_task)
        .collect::<MetaResult<Vec<_>>>()?;
    let position_delete_files = file_group
        .position_delete_files
        .iter()
        .map(encode_file_scan_task)
        .collect::<MetaResult<Vec<_>>>()?;
    let equality_delete_files = file_group
        .equality_delete_files
        .iter()
        .map(encode_file_scan_task)
        .collect::<MetaResult<Vec<_>>>()?;

    let props = props
        .into_iter()
        .collect::<std::collections::HashMap<_, _>>();
    Ok(Plan {
        props,
        table_ident,
        task_type,
        to_branch: plan.to_branch.into_owned(),
        snapshot_id: plan.snapshot_id,
        file_group: Some(FileGroup {
            data_files,
            position_delete_files,
            equality_delete_files,
            executor_parallelism: file_group.executor_parallelism as u32,
            output_parallelism: file_group.output_parallelism as u32,
        }),
    })
}

fn encode_file_scan_task(task: &FileScanTask) -> MetaResult<PbFileScanTask> {
    let schema_json =
        serde_json::to_vec(task.schema()).map_err(|e| SinkError::Iceberg(e.into()))?;
    let (predicate_json, has_predicate) = match task.predicate() {
        Some(pred) => (
            serde_json::to_vec(pred).map_err(|e| SinkError::Iceberg(e.into()))?,
            true,
        ),
        None => (vec![], false),
    };

    let (equality_ids, has_equality_ids) = match &task.equality_ids {
        Some(ids) => (ids.clone(), true),
        None => (vec![], false),
    };

    Ok(PbFileScanTask {
        start: task.start,
        length: task.length,
        record_count: task.record_count,
        data_file_path: task.data_file_path.clone(),
        data_file_content: map_content_type(task.data_file_content) as i32,
        data_file_format: map_file_format(task.data_file_format) as i32,
        schema_json,
        project_field_ids: task.project_field_ids.clone(),
        predicate_json,
        has_predicate,
        sequence_number: task.sequence_number,
        equality_ids,
        has_equality_ids,
        file_size_in_bytes: task.file_size_in_bytes,
    })
}

fn map_content_type(content: DataContentType) -> FileContent {
    match content {
        DataContentType::Data => FileContent::Data,
        DataContentType::PositionDeletes => FileContent::PositionDeletes,
        DataContentType::EqualityDeletes => FileContent::EqualityDeletes,
    }
}

fn map_file_format(format: DataFileFormat) -> FileFormat {
    match format {
        DataFileFormat::Parquet => FileFormat::Parquet,
        DataFileFormat::Avro => FileFormat::Avro,
        DataFileFormat::Orc => FileFormat::Orc,
        DataFileFormat::Puffin => FileFormat::Puffin,
    }
}

fn build_rewritten_data_files(file_group: FileGroup, table: &Table) -> MetaResult<Vec<DataFile>> {
    let spec_id = table.metadata().default_partition_spec_id();
    let mut files = Vec::new();
    for task in &file_group.data_files {
        files.push(build_data_file_from_scan_task(task, spec_id)?);
    }
    Ok(files)
}

fn build_data_file_from_scan_task(
    task: &PbFileScanTask,
    partition_spec_id: i32,
) -> MetaResult<DataFile> {
    let content = match FileContent::try_from(task.data_file_content) {
        Ok(FileContent::Data) => DataContentType::Data,
        Ok(FileContent::PositionDeletes) => DataContentType::PositionDeletes,
        Ok(FileContent::EqualityDeletes) => DataContentType::EqualityDeletes,
        _ => DataContentType::Data,
    };
    let format = match FileFormat::try_from(task.data_file_format) {
        Ok(FileFormat::Parquet) => DataFileFormat::Parquet,
        Ok(FileFormat::Avro) => DataFileFormat::Avro,
        Ok(FileFormat::Orc) => DataFileFormat::Orc,
        Ok(FileFormat::Puffin) => DataFileFormat::Puffin,
        _ => DataFileFormat::Parquet,
    };

    let mut builder = DataFileBuilder::default();
    builder
        .content(content)
        .file_path(task.data_file_path.clone())
        .file_format(format)
        .file_size_in_bytes(task.file_size_in_bytes)
        .record_count(task.record_count.unwrap_or(0))
        .partition_spec_id(partition_spec_id)
        .partition(Struct::empty());

    if task.has_equality_ids {
        builder.equality_ids(Some(task.equality_ids.clone()));
    }

    builder
        .build()
        .map_err(|e| SinkError::Iceberg(e.into()).into())
}

fn decode_serialized_data_files(
    files: &[PbSerializedDataFile],
    table: &Table,
) -> MetaResult<Vec<DataFile>> {
    let schema = table.metadata().current_schema();
    let mut partition_types: HashMap<i32, StructType> = HashMap::new();

    let mut decoded = Vec::with_capacity(files.len());
    for file in files {
        let partition_type = match partition_types.get(&file.partition_spec_id) {
            Some(partition_type) => partition_type,
            None => {
                let Some(partition_spec) = table
                    .metadata()
                    .partition_spec_by_id(file.partition_spec_id)
                else {
                    return Err(SinkError::Iceberg(anyhow!(
                        "Can't find partition spec by id {}",
                        file.partition_spec_id
                    ))
                    .into());
                };
                let partition_type = partition_spec
                    .partition_type(schema.as_ref())
                    .map_err(|e| SinkError::Iceberg(e.into()))?;
                partition_types.insert(file.partition_spec_id, partition_type);
                partition_types
                    .get(&file.partition_spec_id)
                    .expect("just inserted partition type")
            }
        };
        let serialized: SerializedDataFile =
            serde_json::from_slice(&file.json).map_err(|e| SinkError::Iceberg(e.into()))?;
        let data_file = serialized
            .try_into(file.partition_spec_id, partition_type, schema.as_ref())
            .map_err(|e| SinkError::Iceberg(e.into()))?;
        decoded.push(data_file);
    }
    Ok(decoded)
}

async fn collect_main_branch_data_files(table: &Table) -> MetaResult<Vec<DataFile>> {
    let mut input_files = vec![];
    if let Some(snapshot) = table.metadata().snapshot_for_ref(MAIN_BRANCH) {
        let manifest_list = snapshot
            .load_manifest_list(table.file_io(), table.metadata())
            .await
            .map_err(|e| SinkError::Iceberg(e.into()))?;

        for manifest_file in manifest_list
            .entries()
            .iter()
            .filter(|entry| entry.has_added_files() || entry.has_existing_files())
        {
            let manifest = manifest_file
                .load_manifest(table.file_io())
                .await
                .map_err(|e| SinkError::Iceberg(e.into()))?;
            let (entry, _) = manifest.into_parts();
            for i in entry {
                if matches!(i.content_type(), iceberg::spec::DataContentType::Data) {
                    input_files.push(i.data_file().clone());
                }
            }
        }
    }
    Ok(input_files)
}

#[derive(Debug, Clone)]
struct PlanAssignment {
    key: (u64, u32),
    plan_task: subscribe_iceberg_compaction_event_response::PlanTask,
    attempt: u32,
    assigned_at: Option<Instant>,
}
