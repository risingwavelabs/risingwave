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

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::anyhow;
use iceberg::spec::Operation;
use iceberg::transaction::{ApplyTransactionAction, Transaction};
use itertools::Itertools;
use parking_lot::RwLock;
use risingwave_common::id::WorkerId;
use risingwave_connector::connector_common::IcebergSinkCompactionUpdate;
use risingwave_connector::sink::catalog::{SinkCatalog, SinkId};
use risingwave_connector::sink::iceberg::{
    CompactionType, IcebergConfig, should_enable_iceberg_cow,
};
use risingwave_connector::sink::{SinkError, SinkParam};
use risingwave_pb::iceberg_compaction::iceberg_compaction_task::TaskType;
use risingwave_pb::iceberg_compaction::subscribe_iceberg_compaction_event_request::ReportTask as IcebergReportTask;
use risingwave_pb::iceberg_compaction::subscribe_iceberg_compaction_event_request::report_task::Status as IcebergReportTaskStatus;
use risingwave_pb::iceberg_compaction::{
    IcebergCompactionTask, SubscribeIcebergCompactionEventRequest,
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

/// Compaction track states using type-safe state machine pattern
#[derive(Debug, Clone)]
enum CompactionTrackState {
    /// Ready to accept commits and check for trigger conditions
    Idle { next_compaction_time: Instant },
    /// Task has been selected locally but not yet accepted by a compactor.
    PendingDispatch {
        next_compaction_time_on_failure: Instant,
        pending_commit_count_at_dispatch: usize,
    },
    /// Compaction task is in-flight. `report_deadline` acts as a lease; if it
    /// expires before a report arrives, the task becomes retryable.
    InFlight {
        task_id: u64,
        pending_commit_count_at_dispatch: usize,
        report_deadline: Instant,
    },
}

#[derive(Debug, Clone)]
struct CompactionTrack {
    task_type: TaskType,
    trigger_interval_sec: u64,
    /// Minimum pending commit threshold to trigger compaction early.
    /// Compaction triggers when `pending_commit_count` >= this threshold, even before interval expires.
    trigger_snapshot_count: usize,
    report_timeout: Duration,
    last_config_refresh_at: Instant,
    pending_commit_count: usize,
    state: CompactionTrackState,
}

impl CompactionTrack {
    fn new(
        task_type: TaskType,
        trigger_interval_sec: u64,
        trigger_snapshot_count: usize,
        report_timeout: Duration,
        now: Instant,
    ) -> Self {
        Self {
            task_type,
            trigger_interval_sec,
            trigger_snapshot_count,
            report_timeout,
            last_config_refresh_at: now,
            pending_commit_count: 0,
            state: CompactionTrackState::Idle {
                next_compaction_time: now + Duration::from_secs(trigger_interval_sec),
            },
        }
    }

    /// Determines if compaction should be triggered.
    ///
    /// Trigger conditions (OR logic):
    /// 1. `commit_ready` - Pending commit count >= threshold (early trigger)
    /// 2. `time_ready && has_commits` - Interval expired and there's at least 1 pending commit
    ///
    /// This ensures:
    /// - `trigger_snapshot_count` is an early trigger threshold
    /// - `compaction_interval_sec` is the maximum wait time (as long as there are new snapshots)
    /// - Force compaction works by setting `next_compaction_time` to now
    /// - No empty compaction runs (requires at least 1 snapshot for time-based trigger)
    fn should_trigger(&self, now: Instant) -> bool {
        // Only Idle state can trigger.
        let next_compaction_time = match &self.state {
            CompactionTrackState::Idle {
                next_compaction_time,
            } => *next_compaction_time,
            CompactionTrackState::PendingDispatch { .. }
            | CompactionTrackState::InFlight { .. } => return false,
        };

        // Check conditions
        let time_ready = now >= next_compaction_time;
        let commit_ready = self.pending_commit_count >= self.trigger_snapshot_count;
        let has_commits = self.pending_commit_count > 0;

        // OR logic: the commit threshold triggers early,
        // while the time trigger still requires at least 1 pending commit.
        commit_ready || (time_ready && has_commits)
    }

    fn record_commit(&mut self) {
        self.pending_commit_count = self.pending_commit_count.saturating_add(1);
    }

    fn record_force_compaction(&mut self, now: Instant) {
        if let CompactionTrackState::Idle {
            next_compaction_time,
        } = &mut self.state
        {
            *next_compaction_time = now;
            self.pending_commit_count = self.pending_commit_count.max(1);
        }
    }

    fn needs_config_refresh(&self, now: Instant, refresh_interval: Duration) -> bool {
        now.saturating_duration_since(self.last_config_refresh_at) >= refresh_interval
    }

    fn should_refresh_config(&self, now: Instant, refresh_interval: Duration) -> bool {
        matches!(self.state, CompactionTrackState::Idle { .. })
            && self.needs_config_refresh(now, refresh_interval)
    }

    fn mark_config_refreshed(&mut self, now: Instant) {
        self.last_config_refresh_at = now;
    }

    fn start_processing(&mut self) {
        match &self.state {
            CompactionTrackState::Idle {
                next_compaction_time,
            } => {
                self.state = CompactionTrackState::PendingDispatch {
                    next_compaction_time_on_failure: *next_compaction_time,
                    pending_commit_count_at_dispatch: self.pending_commit_count,
                };
            }
            CompactionTrackState::PendingDispatch { .. }
            | CompactionTrackState::InFlight { .. } => {
                unreachable!("Cannot start processing when already processing")
            }
        }
    }

    fn mark_dispatched(&mut self, task_id: u64, now: Instant) {
        let pending_commit_count_at_dispatch = match &self.state {
            CompactionTrackState::PendingDispatch {
                next_compaction_time_on_failure: _,
                pending_commit_count_at_dispatch,
            } => *pending_commit_count_at_dispatch,
            CompactionTrackState::Idle { .. } => unreachable!("Cannot mark dispatched when idle"),
            CompactionTrackState::InFlight { .. } => {
                unreachable!("Cannot mark dispatched when already in flight")
            }
        };
        self.state = CompactionTrackState::InFlight {
            task_id,
            pending_commit_count_at_dispatch,
            report_deadline: now + self.report_timeout,
        };
    }

    fn is_pending_dispatch(&self) -> bool {
        matches!(self.state, CompactionTrackState::PendingDispatch { .. })
    }

    fn is_processing_task(&self, task_id: u64) -> bool {
        matches!(
            &self.state,
            CompactionTrackState::InFlight {
                task_id: current_task_id,
                pending_commit_count_at_dispatch: _,
                report_deadline: _,
            } if *current_task_id == task_id
        )
    }

    fn is_report_timed_out(&self, now: Instant) -> bool {
        matches!(
            &self.state,
            CompactionTrackState::InFlight {
                task_id: _,
                pending_commit_count_at_dispatch: _,
                report_deadline,
            } if now >= *report_deadline
        )
    }

    fn finish_success(&mut self, now: Instant) {
        match &self.state {
            CompactionTrackState::InFlight {
                task_id: _,
                pending_commit_count_at_dispatch,
                report_deadline: _,
            } => {
                self.pending_commit_count = self
                    .pending_commit_count
                    .saturating_sub(*pending_commit_count_at_dispatch);
                self.state = CompactionTrackState::Idle {
                    next_compaction_time: now + Duration::from_secs(self.trigger_interval_sec),
                };
            }
            CompactionTrackState::Idle { .. } => unreachable!("Cannot finish success when idle"),
            CompactionTrackState::PendingDispatch { .. } => {
                unreachable!("Cannot finish success before task dispatch")
            }
        }
    }

    fn finish_failed(&mut self, now: Instant) {
        match &self.state {
            CompactionTrackState::InFlight { .. } => {
                self.state = CompactionTrackState::Idle {
                    next_compaction_time: now,
                };
            }
            CompactionTrackState::Idle { .. } => unreachable!("Cannot finish failed when idle"),
            CompactionTrackState::PendingDispatch { .. } => {
                unreachable!("Cannot finish failed before task dispatch")
            }
        }
    }

    /// Restore the idle scheduling state after a pre-dispatch failure.
    ///
    /// `pending_commit_count` is intentionally preserved so commits that arrive
    /// while the track is pending dispatch are not lost if task dispatch fails
    /// before the compactor accepts the task.
    fn revert_pre_dispatch_failure(&mut self) {
        match &self.state {
            CompactionTrackState::PendingDispatch {
                next_compaction_time_on_failure,
                pending_commit_count_at_dispatch: _,
            } => {
                self.state = CompactionTrackState::Idle {
                    next_compaction_time: *next_compaction_time_on_failure,
                };
            }
            CompactionTrackState::Idle { .. } => {
                unreachable!("Cannot revert a pre-dispatch failure when idle")
            }
            CompactionTrackState::InFlight { .. } => {
                unreachable!("Cannot revert a pre-dispatch failure after dispatch")
            }
        }
    }

    fn update_interval(&mut self, new_interval_sec: u64, now: Instant) {
        if self.trigger_interval_sec == new_interval_sec {
            // Keep the existing deadline when the interval itself does not change.
            // This preserves the previous behavior where the interval bounds the maximum
            // wait since the last successful compaction or force trigger instead of
            // sliding forward on every commit.
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
            CompactionTrackState::PendingDispatch { .. }
            | CompactionTrackState::InFlight { .. } => {
                // Keep the current state. The next deadline is reset when the task completes.
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
}

impl IcebergCompactionHandle {
    fn new(
        sink_id: SinkId,
        task_type: TaskType,
        inner: Arc<RwLock<IcebergCompactionManagerInner>>,
        metadata_manager: MetadataManager,
    ) -> Self {
        Self {
            sink_id,
            task_type,
            inner,
            metadata_manager,
            handle_success: false,
        }
    }

    pub async fn send_compact_task(
        mut self,
        compactor: Arc<IcebergCompactor>,
        task_id: u64,
    ) -> MetaResult<()> {
        use risingwave_pb::iceberg_compaction::subscribe_iceberg_compaction_event_response::Event as IcebergResponseEvent;
        let Some(prost_sink_catalog) = self
            .metadata_manager
            .catalog_controller
            .get_sink_by_id(self.sink_id)
            .await?
        else {
            // The sink may be deleted, just return Ok.
            tracing::warn!("Sink not found: {}", self.sink_id);
            return Ok(());
        };
        let sink_catalog = SinkCatalog::from(prost_sink_catalog);
        let param = SinkParam::try_from_sink_catalog(sink_catalog)?;

        let result =
            compactor.send_event(IcebergResponseEvent::CompactTask(IcebergCompactionTask {
                task_id,
                sink_id: self.sink_id.as_raw_id(),
                props: param.properties,
                task_type: self.task_type as i32,
            }));

        if result.is_ok() {
            let mut guard = self.inner.write();
            let mut dispatched = false;
            if let Some(track) = guard.sink_schedules.get_mut(&self.sink_id)
                && track.is_pending_dispatch()
            {
                track.mark_dispatched(task_id, Instant::now());
                dispatched = true;
            }
            self.handle_success = dispatched;
            if !dispatched {
                tracing::warn!(
                    sink_id = %self.sink_id,
                    task_id,
                    "Iceberg compaction task send succeeded but track was no longer pending dispatch"
                );
            }
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
        if !self.handle_success
            && let Some(track) = guard.sink_schedules.get_mut(&self.sink_id)
            && track.is_pending_dispatch()
        {
            // If task dispatch fails before the compactor accepts the task, revert the
            // scheduling state back to idle without losing any commits that arrived while
            // the handle was in-flight.
            track.revert_pre_dispatch_failure();
        }
    }
}

struct IcebergCompactionManagerInner {
    pub sink_schedules: HashMap<SinkId, CompactionTrack>,
}

#[derive(Debug, Clone)]
pub struct IcebergCompactionScheduleStatus {
    pub sink_id: SinkId,
    pub task_type: String,
    pub trigger_interval_sec: u64,
    pub trigger_snapshot_count: usize,
    pub schedule_state: String,
    pub next_compaction_after_sec: Option<u64>,
    /// V1 approximates pending snapshot backlog with in-memory sink commit backlog.
    pub pending_snapshot_count: Option<usize>,
    pub is_triggerable: bool,
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
    fn report_timeout(&self) -> Duration {
        Duration::from_secs(self.env.opts.iceberg_compaction_report_timeout_sec)
    }

    fn config_refresh_interval(&self) -> Duration {
        Duration::from_secs(self.env.opts.iceberg_compaction_config_refresh_interval_sec)
    }

    fn refresh_schedule_config(
        &self,
        track: &mut CompactionTrack,
        iceberg_config: &IcebergConfig,
        now: Instant,
    ) {
        let (task_type, trigger_interval_sec, trigger_snapshot_count) =
            self.resolve_schedule_values(iceberg_config);
        track.task_type = task_type;
        track.trigger_snapshot_count = trigger_snapshot_count;
        track.update_interval(trigger_interval_sec, now);
        track.mark_config_refreshed(now);
    }

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
            force_compaction,
        } = msg;
        let now = Instant::now();
        let refresh_interval = self.config_refresh_interval();
        // Re-check after loading config because the track can be removed while this
        // async path is waiting on catalog reads.
        let (track_existed_at_check, should_refresh_config) = {
            let guard = self.inner.read();
            match guard.sink_schedules.get(&sink_id) {
                Some(track) => (true, track.should_refresh_config(now, refresh_interval)),
                None => (false, true),
            }
        };
        let refreshed_config = if should_refresh_config {
            match self.load_iceberg_config(sink_id).await {
                Ok(config) => Some(config),
                Err(e) => {
                    tracing::warn!(
                        error = ?e.as_report(),
                        "Failed to load iceberg config for sink {}",
                        sink_id
                    );
                    None
                }
            }
        } else {
            None
        };

        let mut guard = self.inner.write();
        self.apply_commit_update_with_loaded_config(
            &mut guard,
            sink_id,
            track_existed_at_check,
            refreshed_config.as_ref(),
            should_refresh_config,
            force_compaction,
            now,
            refresh_interval,
        );
    }

    fn apply_commit_update_with_loaded_config(
        &self,
        guard: &mut IcebergCompactionManagerInner,
        sink_id: SinkId,
        track_existed_at_check: bool,
        refreshed_config: Option<&IcebergConfig>,
        should_refresh_config: bool,
        force_compaction: bool,
        now: Instant,
        refresh_interval: Duration,
    ) {
        match guard.sink_schedules.entry(sink_id) {
            std::collections::hash_map::Entry::Occupied(entry) => {
                let track = entry.into_mut();
                if track.should_refresh_config(now, refresh_interval) {
                    if let Some(config) = refreshed_config.as_ref() {
                        self.refresh_schedule_config(track, config, now);
                    } else if should_refresh_config {
                        track.mark_config_refreshed(now);
                    }
                }

                if force_compaction {
                    track.record_force_compaction(now);
                } else {
                    track.record_commit();
                }
            }
            std::collections::hash_map::Entry::Vacant(entry) => {
                // If the track existed before the await but is now gone, treat the update as
                // stale instead of recreating scheduler state from old assumptions.
                if track_existed_at_check {
                    tracing::warn!(
                        sink_id = %sink_id,
                        "Ignoring iceberg compaction update because track disappeared before apply"
                    );
                    return;
                }

                let Some(config) = refreshed_config.as_ref() else {
                    tracing::warn!(
                        sink_id = %sink_id,
                        "Ignoring iceberg compaction update because sink config is unavailable"
                    );
                    return;
                };

                let track = entry.insert(self.create_compaction_track(config, now));
                if force_compaction {
                    track.record_force_compaction(now);
                } else {
                    track.record_commit();
                }
            }
        }
    }

    /// Create a compaction track for a sink based on its Iceberg configuration
    fn create_compaction_track(
        &self,
        iceberg_config: &IcebergConfig,
        now: Instant,
    ) -> CompactionTrack {
        let (task_type, trigger_interval_sec, trigger_snapshot_count) =
            self.resolve_schedule_values(iceberg_config);

        CompactionTrack::new(
            task_type,
            trigger_interval_sec,
            trigger_snapshot_count,
            self.report_timeout(),
            now,
        )
    }

    fn resolve_schedule_values(&self, iceberg_config: &IcebergConfig) -> (TaskType, u64, usize) {
        (
            if should_enable_iceberg_cow(iceberg_config.r#type.as_str(), iceberg_config.write_mode)
            {
                TaskType::Full
            } else {
                match iceberg_config.compaction_type() {
                    CompactionType::Full => TaskType::Full,
                    CompactionType::SmallFiles => TaskType::SmallFiles,
                    CompactionType::FilesWithDelete => TaskType::FilesWithDelete,
                }
            },
            iceberg_config.compaction_interval_sec(),
            iceberg_config.trigger_snapshot_count(),
        )
    }

    /// Get the top N compaction tasks to trigger
    /// Returns handles for tasks that are ready to be compacted
    /// Sorted by next compaction time
    pub fn get_top_n_iceberg_commit_sink_ids(&self, n: usize) -> Vec<IcebergCompactionHandle> {
        let now = Instant::now();
        let mut guard = self.inner.write();
        for (&sink_id, track) in &mut guard.sink_schedules {
            if track.is_report_timed_out(now) {
                tracing::warn!(sink_id = %sink_id, "Iceberg compaction task report timed out");
                track.finish_failed(now);
            }
        }

        // Collect all triggerable tasks with their priority info
        let mut candidates = Vec::new();
        for (sink_id, track) in &guard.sink_schedules {
            if track.should_trigger(now) {
                // Extract next_time from Idle state (triggerable means Idle)
                if let CompactionTrackState::Idle {
                    next_compaction_time,
                } = &track.state
                {
                    candidates.push((*sink_id, track.task_type, *next_compaction_time));
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

                track.start_processing();

                Some(IcebergCompactionHandle::new(
                    sink_id,
                    task_type,
                    self.inner.clone(),
                    self.metadata_manager.clone(),
                ))
            })
            .collect()
    }

    pub fn clear_iceberg_commits_by_sink_id(&self, sink_id: SinkId) {
        let mut guard = self.inner.write();
        guard.sink_schedules.remove(&sink_id);
    }

    pub fn list_compaction_statuses(&self) -> Vec<IcebergCompactionScheduleStatus> {
        let now = Instant::now();
        let schedules = {
            let guard = self.inner.read();
            guard
                .sink_schedules
                .iter()
                .map(|(&sink_id, track)| (sink_id, track.clone()))
                .collect_vec()
        };

        let mut statuses = schedules
            .into_iter()
            .map(|(sink_id, track)| {
                let next_compaction_after_sec = match &track.state {
                    CompactionTrackState::Idle {
                        next_compaction_time,
                    } => Some(
                        next_compaction_time
                            .saturating_duration_since(now)
                            .as_secs(),
                    ),
                    CompactionTrackState::PendingDispatch { .. }
                    | CompactionTrackState::InFlight { .. } => None,
                };
                let is_triggerable = track.should_trigger(now);

                IcebergCompactionScheduleStatus {
                    sink_id,
                    task_type: track.task_type.as_str_name().to_ascii_lowercase(),
                    trigger_interval_sec: track.trigger_interval_sec,
                    trigger_snapshot_count: track.trigger_snapshot_count,
                    schedule_state: match track.state {
                        CompactionTrackState::Idle { .. } => "idle".to_owned(),
                        CompactionTrackState::PendingDispatch { .. }
                        | CompactionTrackState::InFlight { .. } => "processing".to_owned(),
                    },
                    next_compaction_after_sec,
                    pending_snapshot_count: Some(track.pending_commit_count),
                    is_triggerable,
                }
            })
            .collect_vec();

        statuses.sort_by_key(|status| status.sink_id);
        statuses
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

    pub fn handle_report_task(&self, report: IcebergReportTask) {
        let sink_id = SinkId::from(report.sink_id);
        let task_id = report.task_id;
        let status = IcebergReportTaskStatus::try_from(report.status)
            .unwrap_or(IcebergReportTaskStatus::Unspecified);
        let now = Instant::now();

        let mut guard = self.inner.write();
        let Some(track) = guard.sink_schedules.get_mut(&sink_id) else {
            tracing::warn!(sink_id = %sink_id, task_id, "Received iceberg compaction report for unknown sink");
            return;
        };

        if !track.is_processing_task(task_id) {
            tracing::warn!(sink_id = %sink_id, task_id, "Ignoring stale iceberg compaction report");
            return;
        }

        match status {
            IcebergReportTaskStatus::Success => {
                track.finish_success(now);
            }
            IcebergReportTaskStatus::Failed | IcebergReportTaskStatus::Unspecified => {
                tracing::warn!(
                    sink_id = %sink_id,
                    task_id,
                    error_message = report.error_message.unwrap_or_default(),
                    "Iceberg compaction task reported failure"
                );
                track.finish_failed(now);
            }
        }
    }

    pub fn add_compactor_stream(
        &self,
        context_id: WorkerId,
        req_stream: Streaming<SubscribeIcebergCompactionEventRequest>,
    ) {
        if self
            .compactor_streams_change_tx
            .send((context_id, req_stream))
            .is_err()
        {
            tracing::warn!(context_id = %context_id, "Failed to enqueue iceberg compactor stream");
        }
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

        compactor.send_event(IcebergResponseEvent::CompactTask(IcebergCompactionTask {
            task_id,
            sink_id: sink_id.as_raw_id(),
            props: sink_param.properties,
            task_type: TaskType::Full as i32, // default to full compaction
        }))?;

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

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::sync::Arc;

    use prometheus::Registry;

    use super::*;
    use crate::controller::catalog::CatalogController;
    use crate::controller::cluster::ClusterController;
    use crate::hummock::IcebergCompactorManager;
    use crate::rpc::metrics::MetaMetrics;

    async fn build_test_manager() -> Arc<IcebergCompactionManager> {
        let env = MetaSrvEnv::for_test().await;
        let cluster_ctl = Arc::new(
            ClusterController::new(env.clone(), Duration::from_secs(1))
                .await
                .unwrap(),
        );
        let catalog_ctl = Arc::new(CatalogController::new(env.clone()).await.unwrap());
        let metadata_manager = MetadataManager::new(cluster_ctl, catalog_ctl);
        let iceberg_compactor_manager = Arc::new(IcebergCompactorManager::new());
        let metrics = Arc::new(MetaMetrics::for_test(&Registry::new()));
        let (manager, _) = IcebergCompactionManager::build(
            env,
            metadata_manager,
            iceberg_compactor_manager,
            metrics,
        );
        manager
    }

    fn new_track(
        now: Instant,
        trigger_interval_sec: u64,
        trigger_snapshot_count: usize,
        pending_commit_count: usize,
    ) -> CompactionTrack {
        CompactionTrack {
            task_type: TaskType::Full,
            trigger_interval_sec,
            trigger_snapshot_count,
            report_timeout: Duration::from_secs(30 * 60),
            last_config_refresh_at: now,
            pending_commit_count,
            state: CompactionTrackState::Idle {
                next_compaction_time: now + Duration::from_secs(trigger_interval_sec),
            },
        }
    }

    fn start_in_flight(track: &mut CompactionTrack, task_id: u64, now: Instant) {
        track.start_processing();
        track.mark_dispatched(task_id, now);
    }

    fn record_commits(track: &mut CompactionTrack, n: usize) {
        for _ in 0..n {
            track.record_commit();
        }
    }

    fn new_test_iceberg_config(
        trigger_interval_sec: u64,
        trigger_snapshot_count: usize,
        compaction_type: CompactionType,
    ) -> IcebergConfig {
        let mut values = BTreeMap::from([
            ("connector".to_owned(), "iceberg".to_owned()),
            ("type".to_owned(), "append-only".to_owned()),
            ("force_append_only".to_owned(), "true".to_owned()),
            ("catalog.name".to_owned(), "test-catalog".to_owned()),
            ("catalog.type".to_owned(), "storage".to_owned()),
            ("warehouse.path".to_owned(), "s3://iceberg".to_owned()),
            ("database.name".to_owned(), "test_db".to_owned()),
            ("table.name".to_owned(), "test_table".to_owned()),
        ]);
        values.insert(
            "compaction_interval_sec".to_owned(),
            trigger_interval_sec.to_string(),
        );
        values.insert(
            "compaction.trigger_snapshot_count".to_owned(),
            trigger_snapshot_count.to_string(),
        );
        values.insert(
            "compaction.type".to_owned(),
            match compaction_type {
                CompactionType::Full => "full",
                CompactionType::SmallFiles => "small-files",
                CompactionType::FilesWithDelete => "files-with-delete",
            }
            .to_owned(),
        );

        IcebergConfig::from_btreemap(values).unwrap()
    }

    #[test]
    fn test_should_trigger_by_pending_commit_threshold() {
        let now = Instant::now();
        let track = new_track(now, 300, 3, 3);

        assert!(track.should_trigger(now));
    }

    #[test]
    fn test_should_trigger_by_interval_only_with_pending_commits() {
        let now = Instant::now();
        let mut track = new_track(now, 60, 10, 1);
        track.state = CompactionTrackState::Idle {
            next_compaction_time: now - Duration::from_secs(1),
        };
        assert!(track.should_trigger(now));

        let mut empty_track = new_track(now, 60, 10, 0);
        empty_track.state = CompactionTrackState::Idle {
            next_compaction_time: now - Duration::from_secs(1),
        };
        assert!(!empty_track.should_trigger(now));
    }

    #[test]
    fn test_record_force_compaction_bootstraps_or_preserves_backlog() {
        let now = Instant::now();

        for (initial_backlog, expected_backlog) in [(0, 1), (4, 4)] {
            let mut track = new_track(now, 300, 10, initial_backlog);

            track.record_force_compaction(now);

            assert_eq!(track.pending_commit_count, expected_backlog);
            assert!(track.should_trigger(now));
        }
    }

    #[test]
    fn test_finish_success_clears_dispatched_baseline_and_starts_cooldown() {
        let now = Instant::now();
        let mut track = new_track(now, 120, 10, 12);
        start_in_flight(&mut track, 1, now);

        track.finish_success(now);

        assert_eq!(track.pending_commit_count, 0);
        assert!(!track.should_trigger(now));
        match track.state {
            CompactionTrackState::Idle {
                next_compaction_time,
            } => assert!(next_compaction_time >= now + Duration::from_secs(120)),
            CompactionTrackState::PendingDispatch { .. }
            | CompactionTrackState::InFlight { .. } => {
                panic!("track should be idle")
            }
        }
    }

    #[test]
    fn test_finish_failed_preserves_backlog_and_allows_retry() {
        let now = Instant::now();
        let mut track = new_track(now, 120, 10, 4);
        start_in_flight(&mut track, 1, now);

        track.finish_failed(now);

        assert_eq!(track.pending_commit_count, 4);
        assert!(track.should_trigger(now));
        match track.state {
            CompactionTrackState::Idle {
                next_compaction_time,
            } => assert!(next_compaction_time <= now),
            CompactionTrackState::PendingDispatch { .. }
            | CompactionTrackState::InFlight { .. } => {
                panic!("track should be idle")
            }
        }
    }

    #[test]
    fn test_report_timeout_is_based_on_processing_deadline() {
        let now = Instant::now();
        let mut track = new_track(now, 120, 10, 5);
        track.report_timeout = Duration::from_secs(17);
        start_in_flight(&mut track, 1, now);

        match track.state {
            CompactionTrackState::InFlight { .. } => {}
            CompactionTrackState::Idle { .. } => panic!("track should remain pending"),
            CompactionTrackState::PendingDispatch { .. } => {
                panic!("track should have been dispatched")
            }
        }
        assert!(!track.is_report_timed_out(now + track.report_timeout - Duration::from_secs(1)));
        assert!(track.is_report_timed_out(now + track.report_timeout));
    }

    #[test]
    fn test_revert_pre_dispatch_failure_restores_idle_without_losing_backlog() {
        let now = Instant::now();
        for additional_commits in [0, 3] {
            let mut track = new_track(now, 120, 10, 5);
            track.start_processing();
            record_commits(&mut track, additional_commits);

            track.revert_pre_dispatch_failure();

            assert_eq!(track.pending_commit_count, 5 + additional_commits);
            match track.state {
                CompactionTrackState::Idle {
                    next_compaction_time,
                } => assert_eq!(next_compaction_time, now + Duration::from_secs(120)),
                CompactionTrackState::PendingDispatch { .. }
                | CompactionTrackState::InFlight { .. } => {
                    panic!("track should be restored to idle")
                }
            }
        }
    }

    #[test]
    fn test_mark_dispatched_records_task_id_for_stale_report_filtering() {
        let now = Instant::now();
        let mut track = new_track(now, 120, 10, 5);
        track.start_processing();
        assert!(track.is_pending_dispatch());
        track.mark_dispatched(42, now);

        assert!(track.is_processing_task(42));
        assert!(!track.is_processing_task(43));
    }

    #[test]
    fn test_force_compaction_does_not_make_processing_track_triggerable() {
        let now = Instant::now();
        let mut track = new_track(now, 120, 10, 0);
        track.start_processing();

        track.record_force_compaction(now);

        assert!(!track.should_trigger(now));
    }

    #[test]
    fn test_finish_failed_after_force_keeps_force_backlog() {
        let now = Instant::now();
        let mut track = new_track(now, 120, 10, 0);
        track.record_force_compaction(now);
        start_in_flight(&mut track, 1, now);

        track.finish_failed(now);

        assert_eq!(track.pending_commit_count, 1);
        assert!(track.should_trigger(now));
    }

    #[test]
    fn test_finish_success_after_force_consumes_force_backlog() {
        let now = Instant::now();
        let mut track = new_track(now, 120, 10, 0);
        track.record_force_compaction(now);
        start_in_flight(&mut track, 1, now);

        track.finish_success(now);

        assert_eq!(track.pending_commit_count, 0);
        assert!(!track.should_trigger(now));
    }

    #[test]
    fn test_record_commit_during_processing_is_preserved_after_success() {
        let now = Instant::now();
        let mut track = new_track(now, 120, 10, 2);
        start_in_flight(&mut track, 1, now);
        record_commits(&mut track, 5);

        track.finish_success(now);

        assert_eq!(track.pending_commit_count, 5);
    }

    #[test]
    fn test_update_interval_resets_idle_deadline() {
        let now = Instant::now();
        let mut track = new_track(now, 120, 10, 1);

        track.update_interval(300, now);

        assert_eq!(track.trigger_interval_sec, 300);
        match track.state {
            CompactionTrackState::Idle {
                next_compaction_time,
            } => assert_eq!(next_compaction_time, now + Duration::from_secs(300)),
            CompactionTrackState::PendingDispatch { .. }
            | CompactionTrackState::InFlight { .. } => {
                panic!("track should stay idle")
            }
        }
    }

    #[test]
    fn test_update_interval_same_value_keeps_existing_idle_deadline() {
        let now = Instant::now();
        let mut track = new_track(now, 120, 10, 1);
        let original_deadline = match track.state {
            CompactionTrackState::Idle {
                next_compaction_time,
            } => next_compaction_time,
            CompactionTrackState::PendingDispatch { .. }
            | CompactionTrackState::InFlight { .. } => {
                panic!("track should start idle")
            }
        };

        track.update_interval(120, now + Duration::from_secs(30));

        match track.state {
            CompactionTrackState::Idle {
                next_compaction_time,
            } => assert_eq!(next_compaction_time, original_deadline),
            CompactionTrackState::PendingDispatch { .. }
            | CompactionTrackState::InFlight { .. } => {
                panic!("track should stay idle")
            }
        }
    }

    #[test]
    fn test_update_interval_does_not_interrupt_processing() {
        let now = Instant::now();
        let mut track = new_track(now, 120, 10, 1);
        track.start_processing();

        track.update_interval(300, now);

        assert_eq!(track.trigger_interval_sec, 300);
        match track.state {
            CompactionTrackState::PendingDispatch { .. } => {}
            CompactionTrackState::Idle { .. } => panic!("processing state should be preserved"),
            CompactionTrackState::InFlight { .. } => {
                panic!("track should remain pending dispatch")
            }
        }
    }

    #[test]
    fn test_needs_config_refresh_respects_ttl() {
        let now = Instant::now();
        let track = new_track(now, 120, 10, 1);

        let refresh_interval = Duration::from_secs(60);

        assert!(!track.needs_config_refresh(
            now + refresh_interval - Duration::from_secs(1),
            refresh_interval,
        ));
        assert!(track.needs_config_refresh(now + refresh_interval, refresh_interval));
    }

    #[test]
    fn test_should_refresh_config_requires_idle_state() {
        let now = Instant::now();
        let refresh_interval = Duration::from_secs(60);
        let mut track = new_track(now, 120, 10, 1);

        assert!(track.should_refresh_config(now + refresh_interval, refresh_interval));

        track.start_processing();

        assert!(!track.should_refresh_config(now + refresh_interval, refresh_interval));
    }

    #[tokio::test]
    async fn test_apply_commit_update_with_loaded_config_refreshes_existing_idle_track() {
        let manager = build_test_manager().await;
        let sink_id = SinkId::new(41);
        let now = Instant::now();
        let mut track = new_track(now, 120, 10, 1);
        track.last_config_refresh_at = now - manager.config_refresh_interval();
        manager.inner.write().sink_schedules.insert(sink_id, track);

        let refresh_at = now + Duration::from_secs(30);
        let config = new_test_iceberg_config(300, 3, CompactionType::SmallFiles);
        let mut guard = manager.inner.write();

        manager.apply_commit_update_with_loaded_config(
            &mut guard,
            sink_id,
            true,
            Some(&config),
            true,
            false,
            refresh_at,
            manager.config_refresh_interval(),
        );

        let track = guard.sink_schedules.get(&sink_id).unwrap();
        assert_eq!(track.task_type, TaskType::SmallFiles);
        assert_eq!(track.trigger_interval_sec, 300);
        assert_eq!(track.trigger_snapshot_count, 3);
        assert_eq!(track.last_config_refresh_at, refresh_at);
        assert_eq!(track.pending_commit_count, 2);
        match track.state {
            CompactionTrackState::Idle {
                next_compaction_time,
            } => assert_eq!(next_compaction_time, refresh_at + Duration::from_secs(300)),
            CompactionTrackState::PendingDispatch { .. }
            | CompactionTrackState::InFlight { .. } => {
                panic!("track should stay idle")
            }
        }
    }

    #[tokio::test]
    async fn test_update_iceberg_commit_info_skips_missing_track_on_load_failure() {
        let manager = build_test_manager().await;
        let sink_id = SinkId::new(42);

        manager
            .update_iceberg_commit_info(IcebergSinkCompactionUpdate {
                sink_id,
                force_compaction: false,
            })
            .await;

        let guard = manager.inner.read();
        assert!(!guard.sink_schedules.contains_key(&sink_id));
    }

    #[tokio::test]
    async fn test_update_iceberg_commit_info_refresh_failure_preserves_cached_schedule_config() {
        let manager = build_test_manager().await;
        let sink_id = SinkId::new(43);
        let now = Instant::now();
        let stale_refresh_at = now - manager.config_refresh_interval();
        let mut track = new_track(now, 120, 7, 3);
        track.last_config_refresh_at = stale_refresh_at;
        manager.inner.write().sink_schedules.insert(sink_id, track);

        manager
            .update_iceberg_commit_info(IcebergSinkCompactionUpdate {
                sink_id,
                force_compaction: false,
            })
            .await;

        let guard = manager.inner.read();
        let track = guard.sink_schedules.get(&sink_id).unwrap();
        assert_eq!(track.trigger_interval_sec, 120);
        assert_eq!(track.trigger_snapshot_count, 7);
        assert_eq!(track.pending_commit_count, 4);
        assert!(track.last_config_refresh_at > stale_refresh_at);
    }

    #[tokio::test]
    async fn test_apply_commit_update_with_loaded_config_creates_missing_track() {
        let manager = build_test_manager().await;
        let sink_id = SinkId::new(44);
        let now = Instant::now();
        let config = new_test_iceberg_config(300, 3, CompactionType::SmallFiles);
        let mut guard = IcebergCompactionManagerInner {
            sink_schedules: HashMap::new(),
        };

        manager.apply_commit_update_with_loaded_config(
            &mut guard,
            sink_id,
            false,
            Some(&config),
            true,
            false,
            now,
            manager.config_refresh_interval(),
        );

        let track = guard.sink_schedules.get(&sink_id).unwrap();
        assert_eq!(track.task_type, TaskType::SmallFiles);
        assert_eq!(track.trigger_interval_sec, 300);
        assert_eq!(track.trigger_snapshot_count, 3);
        assert_eq!(track.pending_commit_count, 1);
        assert_eq!(track.last_config_refresh_at, now);
        assert!(matches!(track.state, CompactionTrackState::Idle { .. }));
    }

    #[tokio::test]
    async fn test_apply_commit_update_with_loaded_config_does_not_resurrect_disappeared_track() {
        let manager = build_test_manager().await;
        let sink_id = SinkId::new(45);
        let now = Instant::now();
        let config = new_test_iceberg_config(300, 3, CompactionType::SmallFiles);
        let mut guard = IcebergCompactionManagerInner {
            sink_schedules: HashMap::new(),
        };

        manager.apply_commit_update_with_loaded_config(
            &mut guard,
            sink_id,
            true,
            Some(&config),
            true,
            false,
            now,
            manager.config_refresh_interval(),
        );

        assert!(!guard.sink_schedules.contains_key(&sink_id));
    }

    #[tokio::test]
    async fn test_get_top_n_creates_pending_dispatch_handle_and_drop_restores_idle() {
        let manager = build_test_manager().await;
        let sink_id = SinkId::new(46);
        let now = Instant::now();
        let mut track = new_track(now, 120, 3, 3);
        track.state = CompactionTrackState::Idle {
            next_compaction_time: now - Duration::from_secs(1),
        };
        manager.inner.write().sink_schedules.insert(sink_id, track);

        let handles = manager.get_top_n_iceberg_commit_sink_ids(1);

        assert_eq!(handles.len(), 1);
        {
            let guard = manager.inner.read();
            assert!(matches!(
                guard.sink_schedules.get(&sink_id).unwrap().state,
                CompactionTrackState::PendingDispatch { .. }
            ));
        }

        drop(handles);

        let guard = manager.inner.read();
        let track = guard.sink_schedules.get(&sink_id).unwrap();
        assert_eq!(track.pending_commit_count, 3);
        assert!(matches!(track.state, CompactionTrackState::Idle { .. }));
    }

    #[tokio::test]
    async fn test_handle_report_task_success_consumes_backlog_and_resets_to_idle() {
        let manager = build_test_manager().await;
        let sink_id = SinkId::new(47);
        let now = Instant::now();
        let mut track = new_track(now, 120, 10, 2);
        start_in_flight(&mut track, 9, now);
        record_commits(&mut track, 3);
        manager.inner.write().sink_schedules.insert(sink_id, track);

        manager.handle_report_task(IcebergReportTask {
            task_id: 9,
            sink_id: sink_id.as_raw_id(),
            status: IcebergReportTaskStatus::Success as i32,
            error_message: None,
        });

        let guard = manager.inner.read();
        let track = guard.sink_schedules.get(&sink_id).unwrap();
        assert_eq!(track.pending_commit_count, 3);
        assert!(matches!(track.state, CompactionTrackState::Idle { .. }));
    }

    #[tokio::test]
    async fn test_handle_report_task_failure_preserves_backlog_and_resets_to_idle() {
        let manager = build_test_manager().await;
        let sink_id = SinkId::new(471);
        let now = Instant::now();
        let mut track = new_track(now, 120, 10, 2);
        start_in_flight(&mut track, 9, now);
        record_commits(&mut track, 3);
        manager.inner.write().sink_schedules.insert(sink_id, track);

        manager.handle_report_task(IcebergReportTask {
            task_id: 9,
            sink_id: sink_id.as_raw_id(),
            status: IcebergReportTaskStatus::Failed as i32,
            error_message: Some("boom".to_owned()),
        });

        let guard = manager.inner.read();
        let track = guard.sink_schedules.get(&sink_id).unwrap();
        assert_eq!(track.pending_commit_count, 5);
        assert!(matches!(track.state, CompactionTrackState::Idle { .. }));
    }

    #[tokio::test]
    async fn test_handle_report_task_ignores_stale_task_id() {
        let manager = build_test_manager().await;
        let sink_id = SinkId::new(48);
        let now = Instant::now();
        let mut track = new_track(now, 120, 10, 2);
        start_in_flight(&mut track, 9, now);
        manager.inner.write().sink_schedules.insert(sink_id, track);

        manager.handle_report_task(IcebergReportTask {
            task_id: 10,
            sink_id: sink_id.as_raw_id(),
            status: IcebergReportTaskStatus::Success as i32,
            error_message: None,
        });

        let guard = manager.inner.read();
        let track = guard.sink_schedules.get(&sink_id).unwrap();
        assert_eq!(track.pending_commit_count, 2);
        assert!(matches!(
            track.state,
            CompactionTrackState::InFlight { task_id: 9, .. }
        ));
    }

    #[tokio::test]
    async fn test_get_top_n_retries_timed_out_inflight_task() {
        let manager = build_test_manager().await;
        let sink_id = SinkId::new(49);
        let now = Instant::now();
        let mut track = new_track(now, 120, 10, 2);
        track.report_timeout = Duration::from_secs(1);
        start_in_flight(&mut track, 7, now - Duration::from_secs(5));
        manager.inner.write().sink_schedules.insert(sink_id, track);

        let handles = manager.get_top_n_iceberg_commit_sink_ids(1);

        assert_eq!(handles.len(), 1);
        let guard = manager.inner.read();
        let track = guard.sink_schedules.get(&sink_id).unwrap();
        assert!(matches!(
            track.state,
            CompactionTrackState::PendingDispatch { .. }
        ));
    }
}
