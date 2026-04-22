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
use std::sync::Arc;
use std::time::{Duration, Instant};

use itertools::Itertools;
use parking_lot::RwLock;
use risingwave_connector::connector_common::IcebergSinkCompactionUpdate;
use risingwave_connector::sink::SinkParam;
use risingwave_connector::sink::catalog::{SinkCatalog, SinkId};
use risingwave_connector::sink::iceberg::{
    CompactionType, IcebergConfig, should_enable_iceberg_cow,
};
use risingwave_pb::iceberg_compaction::IcebergCompactionTask;
use risingwave_pb::iceberg_compaction::iceberg_compaction_task::TaskType;
use risingwave_pb::iceberg_compaction::subscribe_iceberg_compaction_event_request::ReportTask as IcebergReportTask;
use risingwave_pb::iceberg_compaction::subscribe_iceberg_compaction_event_request::report_task::Status as IcebergReportTaskStatus;
use thiserror_ext::AsReport;

use super::*;

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
pub(super) struct CompactionTrack {
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
        let next_compaction_time = match &self.state {
            CompactionTrackState::Idle {
                next_compaction_time,
            } => *next_compaction_time,
            CompactionTrackState::PendingDispatch { .. }
            | CompactionTrackState::InFlight { .. } => return false,
        };

        let time_ready = now >= next_compaction_time;
        let commit_ready = self.pending_commit_count >= self.trigger_snapshot_count;
        let has_commits = self.pending_commit_count > 0;

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
                pending_commit_count_at_dispatch,
                ..
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
                ..
            } if *current_task_id == task_id
        )
    }

    fn is_report_timed_out(&self, now: Instant) -> bool {
        matches!(
            &self.state,
            CompactionTrackState::InFlight {
                report_deadline,
                ..
            } if now >= *report_deadline
        )
    }

    fn finish_success(&mut self, now: Instant) {
        match &self.state {
            CompactionTrackState::InFlight {
                pending_commit_count_at_dispatch,
                ..
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
                ..
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
            return;
        }

        self.trigger_interval_sec = new_interval_sec;

        match &mut self.state {
            CompactionTrackState::Idle {
                next_compaction_time,
            } => {
                *next_compaction_time = now + Duration::from_secs(new_interval_sec);
            }
            CompactionTrackState::PendingDispatch { .. }
            | CompactionTrackState::InFlight { .. } => {}
        }
    }
}

pub(crate) struct IcebergCompactionHandle {
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
        compactor: Arc<crate::hummock::IcebergCompactor>,
        task_id: u64,
    ) -> MetaResult<()> {
        use risingwave_pb::iceberg_compaction::subscribe_iceberg_compaction_event_response::Event as IcebergResponseEvent;

        let Some(prost_sink_catalog) = self
            .metadata_manager
            .catalog_controller
            .get_sink_by_id(self.sink_id)
            .await?
        else {
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
}

impl Drop for IcebergCompactionHandle {
    fn drop(&mut self) {
        let mut guard = self.inner.write();
        if !self.handle_success
            && let Some(track) = guard.sink_schedules.get_mut(&self.sink_id)
            && track.is_pending_dispatch()
        {
            track.revert_pre_dispatch_failure();
        }
    }
}

#[derive(Debug, Clone, Copy)]
enum SinkUpdateKind {
    Commit,
    ForceCompaction,
}

impl SinkUpdateKind {
    fn apply_to_track(self, track: &mut CompactionTrack, now: Instant) {
        match self {
            SinkUpdateKind::Commit => track.record_commit(),
            SinkUpdateKind::ForceCompaction => track.record_force_compaction(now),
        }
    }
}

/// Result of the read-only preparation step before applying a sink update.
///
/// `allow_track_initialization` stays `true` only when the sink had no track
/// before the async config load. This lets the apply step initialize a new
/// track for first-time updates, while preventing a stale update from
/// resurrecting a track that disappeared during the async gap.
struct PreparedSinkUpdate {
    allow_track_initialization: bool,
    loaded_config: Option<IcebergConfig>,
}

#[derive(Debug, Clone)]
pub struct IcebergCompactionScheduleStatus {
    pub sink_id: SinkId,
    pub task_type: String,
    pub trigger_interval_sec: u64,
    pub trigger_snapshot_count: usize,
    pub schedule_state: String,
    pub next_compaction_after_sec: Option<u64>,
    pub pending_snapshot_count: Option<usize>,
    pub is_triggerable: bool,
}

impl IcebergCompactionManager {
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

    pub async fn update_iceberg_commit_info(&self, msg: IcebergSinkCompactionUpdate) {
        let IcebergSinkCompactionUpdate {
            sink_id,
            force_compaction,
        } = msg;
        let now = Instant::now();
        let refresh_interval = self.config_refresh_interval();
        let sink_update = if force_compaction {
            SinkUpdateKind::ForceCompaction
        } else {
            SinkUpdateKind::Commit
        };
        let prepared_update = self
            .prepare_sink_update(sink_id, now, refresh_interval)
            .await;

        let mut guard = self.inner.write();
        self.apply_sink_update(
            &mut guard,
            sink_id,
            sink_update,
            now,
            refresh_interval,
            prepared_update,
        );
    }

    async fn prepare_sink_update(
        &self,
        sink_id: SinkId,
        now: Instant,
        refresh_interval: Duration,
    ) -> PreparedSinkUpdate {
        let (allow_track_initialization, should_refresh_config) = {
            let guard = self.inner.read();
            match guard.sink_schedules.get(&sink_id) {
                Some(track) => (false, track.should_refresh_config(now, refresh_interval)),
                None => (true, true),
            }
        };

        let loaded_config = if should_refresh_config {
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

        PreparedSinkUpdate {
            allow_track_initialization,
            loaded_config,
        }
    }

    fn apply_sink_update(
        &self,
        guard: &mut IcebergCompactionManagerInner,
        sink_id: SinkId,
        sink_update: SinkUpdateKind,
        now: Instant,
        refresh_interval: Duration,
        prepared_update: PreparedSinkUpdate,
    ) {
        let PreparedSinkUpdate {
            allow_track_initialization,
            loaded_config,
        } = prepared_update;

        match guard.sink_schedules.entry(sink_id) {
            Entry::Occupied(entry) => {
                let track = entry.into_mut();
                if track.should_refresh_config(now, refresh_interval)
                    && let Some(config) = loaded_config.as_ref()
                {
                    self.refresh_schedule_config(track, config, now);
                }

                sink_update.apply_to_track(track, now);
            }
            Entry::Vacant(entry) => {
                if !allow_track_initialization {
                    tracing::warn!(
                        sink_id = %sink_id,
                        "Ignoring iceberg compaction update because track disappeared before apply"
                    );
                    return;
                }

                let Some(config) = loaded_config.as_ref() else {
                    tracing::warn!(
                        sink_id = %sink_id,
                        "Ignoring iceberg compaction update because sink config is unavailable"
                    );
                    return;
                };

                let track = entry.insert(self.create_compaction_track(config, now));
                sink_update.apply_to_track(track, now);
            }
        }
    }

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

    pub(crate) fn get_top_n_iceberg_commit_sink_ids(
        &self,
        n: usize,
    ) -> Vec<IcebergCompactionHandle> {
        let now = Instant::now();
        let mut guard = self.inner.write();
        for (&sink_id, track) in &mut guard.sink_schedules {
            if track.is_report_timed_out(now) {
                tracing::warn!(sink_id = %sink_id, "Iceberg compaction task report timed out");
                track.finish_failed(now);
            }
        }

        let mut candidates = Vec::new();
        for (sink_id, track) in &guard.sink_schedules {
            if track.should_trigger(now)
                && let CompactionTrackState::Idle {
                    next_compaction_time,
                } = &track.state
            {
                candidates.push((*sink_id, track.task_type, *next_compaction_time));
            }
        }

        candidates.sort_by(|a, b| a.2.cmp(&b.2));

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

    pub fn handle_report_task(&self, report: IcebergReportTask) {
        if self.complete_manual_task_if_any(&report) {
            return;
        }

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
}

#[cfg(test)]
mod tests;
