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
use std::sync::Arc;
use std::time::{Duration, Instant};

use itertools::Itertools;
use parking_lot::RwLock;
use risingwave_connector::connector_common::{
    IcebergCommittedSnapshot, IcebergSinkCompactionUpdate,
};
use risingwave_connector::sink::SinkParam;
use risingwave_connector::sink::catalog::{SinkCatalog, SinkId};
use risingwave_connector::sink::iceberg::{
    CompactionType, IcebergConfig, should_enable_iceberg_cow,
};
use risingwave_hummock_sdk::HummockContextId;
use risingwave_pb::iceberg_compaction::IcebergCompactionTask;
use risingwave_pb::iceberg_compaction::iceberg_compaction_task::TaskType;
use risingwave_pb::iceberg_compaction::subscribe_iceberg_compaction_event_request::ReportTask as IcebergReportTask;
use risingwave_pb::iceberg_compaction::subscribe_iceberg_compaction_event_request::report_task::Status as IcebergReportTaskStatus;
use thiserror_ext::AsReport;
use tokio::sync::oneshot;

use super::*;
use crate::manager::iceberg_v3_sink::is_iceberg_v3_sink;

/// Compaction track states using type-safe state machine pattern
#[derive(Debug, Clone)]
enum CompactionTrackState {
    /// Ready to accept commits and check for trigger conditions.
    ///
    /// `Idle` is not an active task state. A manual request may leave a
    /// one-shot task type override here for the next scheduler selection.
    Idle {
        next_compaction_time: Instant,
        /// `None` uses the configured track task type; `Some` is consumed when
        /// the next task enters `PendingDispatch`.
        next_task_type_override: Option<TaskType>,
    },
    /// Task has been selected locally but not yet accepted by a compactor.
    PendingDispatch {
        task_type: TaskType,
        next_compaction_time_on_failure: Instant,
        pending_commit_count_at_dispatch: usize,
        gc_watermark_snapshot: Option<IcebergCommittedSnapshot>,
    },
    /// Compaction task is in-flight. `report_deadline` acts as a lease; if it
    /// expires before a report arrives, the task becomes retryable.
    InFlight {
        task_id: u64,
        compactor_context_id: HummockContextId,
        task_type: TaskType,
        pending_commit_count_at_dispatch: usize,
        report_deadline: Instant,
        gc_watermark_snapshot: Option<IcebergCommittedSnapshot>,
    },
}

#[derive(Debug, Clone, Copy)]
struct ScheduledCompactionTask {
    task_id: u64,
    compactor_context_id: HummockContextId,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum CompactionTrackFinishAction {
    KeepTrack,
    RemoveTrack,
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
    latest_observed_snapshot: Option<IcebergCommittedSnapshot>,
    /// Track lifecycle policy after a selected task finishes. Manual compaction
    /// uses `RemoveTrack` only while automatic compaction is disabled; a later
    /// config refresh can turn the temporary track into a normal schedule track.
    finish_action: CompactionTrackFinishAction,
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
            latest_observed_snapshot: None,
            finish_action: CompactionTrackFinishAction::KeepTrack,
            state: CompactionTrackState::Idle {
                next_compaction_time: now + Duration::from_secs(trigger_interval_sec),
                next_task_type_override: None,
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
                ..
            } => *next_compaction_time,
            CompactionTrackState::PendingDispatch { .. }
            | CompactionTrackState::InFlight { .. } => return false,
        };

        let time_ready = now >= next_compaction_time;
        let commit_ready = self.pending_commit_count >= self.trigger_snapshot_count;
        let has_commits = self.pending_commit_count > 0;

        commit_ready || (time_ready && has_commits)
    }

    fn record_observed_snapshot(&mut self, observed_snapshot: IcebergCommittedSnapshot) {
        self.latest_observed_snapshot = Some(observed_snapshot);
    }

    fn record_commit(&mut self) {
        self.pending_commit_count = self.pending_commit_count.saturating_add(1);
    }

    fn record_force_compaction(&mut self, now: Instant, forced_task_type: Option<TaskType>) {
        if let CompactionTrackState::Idle {
            next_compaction_time,
            next_task_type_override,
        } = &mut self.state
        {
            if let Some(task_type) = forced_task_type {
                *next_task_type_override = Some(task_type);
            }
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

    fn current_task_type(&self) -> TaskType {
        match &self.state {
            CompactionTrackState::Idle {
                next_task_type_override,
                ..
            } => next_task_type_override.unwrap_or(self.task_type),
            CompactionTrackState::PendingDispatch { task_type, .. }
            | CompactionTrackState::InFlight { task_type, .. } => *task_type,
        }
    }

    fn start_processing(&mut self) -> TaskType {
        match &mut self.state {
            CompactionTrackState::Idle {
                next_compaction_time,
                next_task_type_override,
            } => {
                let task_type = next_task_type_override.take().unwrap_or(self.task_type);
                self.state = CompactionTrackState::PendingDispatch {
                    task_type,
                    next_compaction_time_on_failure: *next_compaction_time,
                    pending_commit_count_at_dispatch: self.pending_commit_count,
                    gc_watermark_snapshot: self.latest_observed_snapshot.clone(),
                };
                task_type
            }
            CompactionTrackState::PendingDispatch { .. }
            | CompactionTrackState::InFlight { .. } => {
                unreachable!("Cannot start processing when already processing")
            }
        }
    }

    fn mark_dispatched(
        &mut self,
        task_id: u64,
        compactor_context_id: HummockContextId,
        now: Instant,
    ) {
        let (task_type, pending_commit_count_at_dispatch, gc_watermark_snapshot) = match &mut self
            .state
        {
            CompactionTrackState::PendingDispatch {
                task_type,
                pending_commit_count_at_dispatch,
                gc_watermark_snapshot,
                ..
            } => {
                let gc_watermark_snapshot = gc_watermark_snapshot.take();
                (
                    *task_type,
                    *pending_commit_count_at_dispatch,
                    gc_watermark_snapshot,
                )
            }
            CompactionTrackState::Idle { .. } => unreachable!("Cannot mark dispatched when idle"),
            CompactionTrackState::InFlight { .. } => {
                unreachable!("Cannot mark dispatched when already in flight")
            }
        };
        self.state = CompactionTrackState::InFlight {
            task_id,
            compactor_context_id,
            task_type,
            pending_commit_count_at_dispatch,
            report_deadline: now + self.report_timeout,
            gc_watermark_snapshot,
        };
    }

    pub(super) fn processing_gc_watermark_snapshot(
        &self,
    ) -> Option<Option<&IcebergCommittedSnapshot>> {
        match &self.state {
            CompactionTrackState::PendingDispatch {
                gc_watermark_snapshot,
                ..
            }
            | CompactionTrackState::InFlight {
                gc_watermark_snapshot,
                ..
            } => Some(gc_watermark_snapshot.as_ref()),
            CompactionTrackState::Idle { .. } => None,
        }
    }

    fn is_pending_dispatch(&self) -> bool {
        matches!(self.state, CompactionTrackState::PendingDispatch { .. })
    }

    fn removes_track_after_finish(&self) -> bool {
        self.finish_action == CompactionTrackFinishAction::RemoveTrack
    }

    pub(super) fn is_processing_task(&self, task_id: u64) -> bool {
        matches!(
            &self.state,
            CompactionTrackState::InFlight {
                task_id: current_task_id,
                ..
            } if *current_task_id == task_id
        )
    }

    fn scheduled_task(&self) -> Option<ScheduledCompactionTask> {
        match &self.state {
            CompactionTrackState::InFlight {
                task_id,
                compactor_context_id,
                ..
            } => Some(ScheduledCompactionTask {
                task_id: *task_id,
                compactor_context_id: *compactor_context_id,
            }),
            CompactionTrackState::Idle { .. } | CompactionTrackState::PendingDispatch { .. } => {
                None
            }
        }
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

    fn finish_success(&mut self, now: Instant) -> CompactionTrackFinishAction {
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
                    next_task_type_override: None,
                };
                self.finish_action
            }
            CompactionTrackState::Idle { .. } => unreachable!("Cannot finish success when idle"),
            CompactionTrackState::PendingDispatch { .. } => {
                unreachable!("Cannot finish success before task dispatch")
            }
        }
    }

    fn finish_failed(&mut self, now: Instant) -> CompactionTrackFinishAction {
        match &self.state {
            CompactionTrackState::InFlight { .. } => {
                self.state = CompactionTrackState::Idle {
                    next_compaction_time: now,
                    next_task_type_override: None,
                };
                self.finish_action
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
    fn revert_pre_dispatch_failure(&mut self) -> CompactionTrackFinishAction {
        match &self.state {
            CompactionTrackState::PendingDispatch {
                next_compaction_time_on_failure,
                ..
            } => {
                self.state = CompactionTrackState::Idle {
                    next_compaction_time: *next_compaction_time_on_failure,
                    next_task_type_override: None,
                };
                self.finish_action
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
                ..
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

        // V3 pk-index sinks must use coordinated compaction: the compactor reports output/input
        // files to meta rather than committing the iceberg transaction itself, avoiding corruption
        // of the streaming PK index.
        let v3_coordinated = is_iceberg_v3_sink(&param.properties);

        let result =
            compactor.send_event(IcebergResponseEvent::CompactTask(IcebergCompactionTask {
                task_id,
                sink_id: self.sink_id.as_raw_id(),
                props: param.properties,
                task_type: self.task_type as i32,
                v3_coordinated,
            }));

        if result.is_ok() {
            let mut should_cancel_sent_task = false;
            let mut guard = self.inner.write();
            let mut dispatched = false;
            if let Some(track) = guard.sink_schedules.get_mut(&self.sink_id)
                && track.is_pending_dispatch()
            {
                track.mark_dispatched(task_id, compactor.context_id(), Instant::now());
                dispatched = true;
            }
            self.handle_success = dispatched;
            if !dispatched {
                should_cancel_sent_task = true;
                tracing::warn!(
                    sink_id = %self.sink_id,
                    task_id,
                    "Iceberg compaction task send succeeded but track was no longer pending dispatch"
                );
            }
            drop(guard);

            if should_cancel_sent_task {
                self.cancel_sent_task(&compactor, task_id);
            }
        }

        result
    }

    fn cancel_sent_task(&self, compactor: &crate::hummock::IcebergCompactor, task_id: u64) {
        if let Err(e) = compactor.cancel_task(task_id) {
            tracing::warn!(
                error = %e.as_report(),
                sink_id = %self.sink_id,
                task_id,
                "Failed to cancel iceberg compaction task after schedule removal",
            );
        }
    }
}

impl Drop for IcebergCompactionHandle {
    fn drop(&mut self) {
        let waiter = {
            let mut guard = self.inner.write();
            if !self.handle_success
                && let Some(track) = guard.sink_schedules.get_mut(&self.sink_id)
                && track.is_pending_dispatch()
            {
                let finish_action = track.revert_pre_dispatch_failure();
                let waiter = guard.manual_compaction_waiters.remove(&self.sink_id);
                IcebergCompactionManager::apply_track_finish_action(
                    &mut guard,
                    self.sink_id,
                    finish_action,
                );
                waiter
            } else {
                None
            }
        };

        if let Some(waiter) = waiter {
            let _ = waiter.send(Err(anyhow!(
                "Iceberg compaction task failed before dispatch for sink {}",
                self.sink_id
            )
            .into()));
        }
    }
}

#[derive(Debug, Clone)]
enum SinkUpdateKind {
    /// A normal sink commit. It increases the pending snapshot count.
    Commit {
        observed_snapshot: IcebergCommittedSnapshot,
    },
    /// A force signal from the sink update path. It triggers the configured
    /// compaction type and still follows the automatic-compaction config gate.
    ForceCompaction {
        observed_snapshot: IcebergCommittedSnapshot,
    },
    /// A user-triggered manual request. It can bypass disabled automatic
    /// compaction and supplies the task type selected for this request.
    ManualForceCompaction { task_type: TaskType },
}

impl SinkUpdateKind {
    fn apply_to_track(self, track: &mut CompactionTrack, now: Instant) {
        match self {
            SinkUpdateKind::Commit { observed_snapshot } => {
                track.record_observed_snapshot(observed_snapshot);
                track.record_commit();
            }
            SinkUpdateKind::ForceCompaction { observed_snapshot } => {
                if matches!(track.state, CompactionTrackState::Idle { .. }) {
                    track.record_observed_snapshot(observed_snapshot);
                }
                track.record_force_compaction(now, None);
            }
            SinkUpdateKind::ManualForceCompaction { task_type } => {
                track.record_force_compaction(now, Some(task_type))
            }
        }
    }

    fn allows_disabled_compaction(&self) -> bool {
        matches!(self, SinkUpdateKind::ManualForceCompaction { .. })
    }
}

/// Result of the read-only preparation step before applying a sink update.
///
/// This bundles the original update intent together with the metadata loaded
/// across the async gap, so the apply step can consume a single object.
///
/// `allow_track_initialization` stays `true` only when the sink had no track
/// before the async config load. This lets the apply step initialize a new
/// track for first-time updates, while preventing a stale update from
/// resurrecting a track that disappeared during the async gap.
struct PreparedSinkUpdate {
    sink_id: SinkId,
    kind: SinkUpdateKind,
    now: Instant,
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
    fn apply_track_finish_action(
        guard: &mut IcebergCompactionManagerInner,
        sink_id: SinkId,
        finish_action: CompactionTrackFinishAction,
    ) {
        match finish_action {
            CompactionTrackFinishAction::KeepTrack => {}
            CompactionTrackFinishAction::RemoveTrack => {
                guard.sink_schedules.remove(&sink_id);
            }
        }
    }

    pub(super) fn refresh_schedule_config(
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
            observed_snapshot,
        } = msg;
        let kind = if force_compaction {
            SinkUpdateKind::ForceCompaction { observed_snapshot }
        } else {
            SinkUpdateKind::Commit { observed_snapshot }
        };
        let prepared_update = self
            .prepare_sink_update(sink_id, kind, Instant::now())
            .await;

        let mut guard = self.inner.write();
        self.apply_sink_update(&mut guard, prepared_update);
    }

    async fn prepare_sink_update(
        &self,
        sink_id: SinkId,
        kind: SinkUpdateKind,
        now: Instant,
    ) -> PreparedSinkUpdate {
        let refresh_interval = self.config_refresh_interval();
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
            sink_id,
            kind,
            now,
            allow_track_initialization,
            loaded_config,
        }
    }

    fn apply_sink_update(
        &self,
        guard: &mut IcebergCompactionManagerInner,
        prepared_update: PreparedSinkUpdate,
    ) -> bool {
        let PreparedSinkUpdate {
            sink_id,
            kind,
            now,
            allow_track_initialization,
            loaded_config,
        } = prepared_update;
        let refresh_interval = self.config_refresh_interval();

        if let Some(config) = loaded_config.as_ref() {
            if config.enable_snapshot_expiration {
                guard.snapshot_expiration_sink_ids.insert(sink_id);
            } else {
                guard.snapshot_expiration_sink_ids.remove(&sink_id);
            }

            if !config.enable_compaction && !kind.allows_disabled_compaction() {
                if !guard.sink_schedules.get(&sink_id).is_some_and(|track| {
                    matches!(
                        &track.state,
                        CompactionTrackState::PendingDispatch { .. }
                            | CompactionTrackState::InFlight { .. }
                    ) || track.removes_track_after_finish()
                }) {
                    guard.sink_schedules.remove(&sink_id);
                }
                return false;
            }
        }

        match guard.sink_schedules.entry(sink_id) {
            Entry::Occupied(entry) => {
                let track = entry.into_mut();
                if track.removes_track_after_finish()
                    && !kind.allows_disabled_compaction()
                    && !loaded_config
                        .as_ref()
                        .is_some_and(|config| config.enable_compaction)
                {
                    return false;
                }
                if track.should_refresh_config(now, refresh_interval)
                    && let Some(config) = loaded_config.as_ref()
                {
                    self.refresh_schedule_config(track, config, now);
                }
                if let Some(config) = loaded_config.as_ref() {
                    track.finish_action =
                        if kind.allows_disabled_compaction() && !config.enable_compaction {
                            CompactionTrackFinishAction::RemoveTrack
                        } else {
                            CompactionTrackFinishAction::KeepTrack
                        };
                }

                kind.apply_to_track(track, now);
                true
            }
            Entry::Vacant(entry) => {
                if !allow_track_initialization {
                    tracing::warn!(
                        sink_id = %sink_id,
                        "Ignoring iceberg compaction update because track disappeared before apply"
                    );
                    return false;
                }

                let Some(config) = loaded_config.as_ref() else {
                    tracing::warn!(
                        sink_id = %sink_id,
                        "Ignoring iceberg compaction update because sink config is unavailable"
                    );
                    return false;
                };

                let track = entry.insert(self.create_compaction_track(config, now));
                track.finish_action =
                    if kind.allows_disabled_compaction() && !config.enable_compaction {
                        CompactionTrackFinishAction::RemoveTrack
                    } else {
                        CompactionTrackFinishAction::KeepTrack
                    };
                kind.apply_to_track(track, now);
                true
            }
        }
    }

    pub(super) fn create_compaction_track(
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

    pub(super) async fn start_manual_compaction(
        &self,
        sink_id: SinkId,
    ) -> MetaResult<oneshot::Receiver<MetaResult<u64>>> {
        let prepared_update = self
            .prepare_sink_update(
                sink_id,
                SinkUpdateKind::ManualForceCompaction {
                    task_type: TaskType::Full,
                },
                Instant::now(),
            )
            .await;
        let mut guard = self.inner.write();
        let now = Instant::now();
        if guard.manual_compaction_waiters.contains_key(&sink_id) {
            return Err(anyhow!(
                "manual iceberg compaction is already waiting for sink {}",
                sink_id
            )
            .into());
        }

        if let Some(track) = guard.sink_schedules.get(&sink_id) {
            match &track.state {
                CompactionTrackState::PendingDispatch {
                    pending_commit_count_at_dispatch,
                    ..
                } => {
                    return Err(anyhow!(
                        "iceberg compaction task is already running for sink {} \
                         (state=pending_dispatch, pending_commit_count_at_dispatch={}, \
                         pending_commit_count={})",
                        sink_id,
                        pending_commit_count_at_dispatch,
                        track.pending_commit_count
                    )
                    .into());
                }
                CompactionTrackState::InFlight {
                    task_id,
                    pending_commit_count_at_dispatch,
                    report_deadline,
                    ..
                } => {
                    return Err(anyhow!(
                        "iceberg compaction task is already running for sink {} \
                         (state=in_flight, task_id={}, pending_commit_count_at_dispatch={}, \
                         pending_commit_count={}, report_timeout_after_sec={})",
                        sink_id,
                        task_id,
                        pending_commit_count_at_dispatch,
                        track.pending_commit_count,
                        report_deadline.saturating_duration_since(now).as_secs()
                    )
                    .into());
                }
                CompactionTrackState::Idle { .. } => {}
            }
        }

        if self.apply_sink_update(&mut guard, prepared_update) {
            let (tx, rx) = oneshot::channel();
            guard.manual_compaction_waiters.insert(sink_id, tx);
            Ok(rx)
        } else {
            Err(anyhow!(
                "failed to trigger manual iceberg compaction for sink {}",
                sink_id
            )
            .into())
        }
    }

    pub(super) fn cancel_manual_compaction_waiter(&self, sink_id: SinkId) {
        self.inner
            .write()
            .manual_compaction_waiters
            .remove(&sink_id);
    }

    fn finish_timed_out_compaction_tasks(
        guard: &mut IcebergCompactionManagerInner,
        now: Instant,
    ) -> Vec<(SinkId, ManualCompactionWaiter)> {
        let mut timed_out_tasks = Vec::new();
        for (&sink_id, track) in &mut guard.sink_schedules {
            if track.is_report_timed_out(now) {
                tracing::warn!(sink_id = %sink_id, "Iceberg compaction task report timed out");
                timed_out_tasks.push((sink_id, track.finish_failed(now)));
            }
        }

        let mut timed_out_waiters = Vec::new();
        for (sink_id, finish_action) in timed_out_tasks {
            if let Some(waiter) = guard.manual_compaction_waiters.remove(&sink_id) {
                timed_out_waiters.push((sink_id, waiter));
            }
            Self::apply_track_finish_action(guard, sink_id, finish_action);
        }
        timed_out_waiters
    }

    pub(crate) fn get_top_n_iceberg_commit_sink_ids(
        &self,
        n: usize,
    ) -> Vec<IcebergCompactionHandle> {
        let now = Instant::now();
        let (handles, timed_out_waiters) = {
            let mut guard = self.inner.write();
            let timed_out_waiters = Self::finish_timed_out_compaction_tasks(&mut guard, now);

            let mut candidates = Vec::new();
            for (sink_id, track) in &guard.sink_schedules {
                // Skip sinks with an in-flight V3 compaction-resolve pipeline. The resolve attaches a
                // transient pipeline and only finishes (commits + detaches) some checkpoints later; until
                // then the sink still looks "dirty" and its track returns to Idle as soon as the
                // compactor report is processed. Without this guard a second compaction would be
                // dispatched while the first resolve is still attached, double-attaching to the same
                // writer fragment and wedging the attach barrier. The `attached_resolves` entry is
                // cleared by `detach_v3_resolve` once the resolve completes, re-enabling scheduling.
                if track.should_trigger(now)
                    && !guard.attached_resolves.contains_key(sink_id)
                    && let CompactionTrackState::Idle {
                        next_compaction_time,
                        ..
                    } = &track.state
                {
                    candidates.push((*sink_id, *next_compaction_time));
                }
            }

            candidates.sort_by_key(|c| c.1);

            let handles = candidates
                .into_iter()
                .take(n)
                .filter_map(|(sink_id, _)| {
                    let track = guard.sink_schedules.get_mut(&sink_id)?;
                    let task_type = track.start_processing();

                    Some(IcebergCompactionHandle::new(
                        sink_id,
                        task_type,
                        self.inner.clone(),
                        self.metadata_manager.clone(),
                    ))
                })
                .collect();

            (handles, timed_out_waiters)
        };

        for (sink_id, waiter) in timed_out_waiters {
            let _ = waiter.send(Err(anyhow!(
                "Iceberg compaction task report timed out for sink {}",
                sink_id
            )
            .into()));
        }

        handles
    }

    pub fn clear_iceberg_maintenance_by_sink_id(&self, sink_id: SinkId) {
        let (task_to_cancel, waiter) = {
            let mut guard = self.inner.write();
            let task_to_cancel = Self::remove_sink_schedule(&mut guard, sink_id);
            guard.snapshot_expiration_sink_ids.remove(&sink_id);
            let waiter = guard.manual_compaction_waiters.remove(&sink_id);
            (task_to_cancel, waiter)
        };
        self.cancel_scheduled_task_if_any(sink_id, task_to_cancel);

        if let Some(waiter) = waiter {
            let _ = waiter.send(Err(anyhow!(
                "Iceberg compaction maintenance was cleared for sink {}",
                sink_id
            )
            .into()));
        }
    }

    fn remove_sink_schedule(
        guard: &mut IcebergCompactionManagerInner,
        sink_id: SinkId,
    ) -> Option<ScheduledCompactionTask> {
        guard
            .sink_schedules
            .remove(&sink_id)
            .and_then(|track| track.scheduled_task())
    }

    fn cancel_scheduled_task_if_any(&self, sink_id: SinkId, task: Option<ScheduledCompactionTask>) {
        let Some(ScheduledCompactionTask {
            task_id,
            compactor_context_id,
        }) = task
        else {
            return;
        };

        let Some(compactor) = self
            .iceberg_compactor_manager
            .get_compactor(compactor_context_id)
        else {
            tracing::warn!(
                sink_id = %sink_id,
                task_id,
                compactor_context_id = %compactor_context_id,
                "Unable to cancel iceberg compaction task because compactor is no longer registered",
            );
            return;
        };

        tracing::info!(
            sink_id = %sink_id,
            task_id,
            compactor_context_id = %compactor_context_id,
            "Cancelling iceberg compaction task for removed schedule",
        );

        if let Err(e) = compactor.cancel_task(task_id) {
            tracing::warn!(
                error = %e.as_report(),
                sink_id = %sink_id,
                task_id,
                compactor_context_id = %compactor_context_id,
                "Failed to cancel iceberg compaction task for removed schedule",
            );
        }
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
                        ..
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
                    task_type: track.current_task_type().as_str_name().to_ascii_lowercase(),
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

    /// Best-effort routing of a V3 coordinated-compaction payload to the V3 sink manager.
    ///
    /// Returns `true` if the iceberg overwrite commit succeeded, `false` if the payload was
    /// malformed, the coordinator was not registered, or the commit itself failed. A `false`
    /// result signals the caller to use `finish_failed` so the scheduler retries the task.
    ///
    /// This must never panic or propagate errors upward; all failure paths are logged and converted
    /// to `false` so the compaction-task state machine in [`Self::handle_report_task`] always runs.
    async fn route_v3_compaction_report(
        &self,
        report: &IcebergReportTask,
        sink_id: SinkId,
        task_id: u64,
    ) -> bool {
        let Some(v3_output_bytes) = &report.v3_output_files else {
            // No V3 payload present; this is a non-V3 coordinated task. Nothing to do.
            return false;
        };
        let Some(read_snapshot_id) = report.v3_read_snapshot_id else {
            tracing::warn!(
                sink_id = %sink_id,
                task_id,
                "V3 compaction report carries output files but no read_snapshot_id; ignoring"
            );
            return false;
        };
        let v3_input_bytes = report.v3_input_files.as_deref().unwrap_or(&[]);

        // Validate the output-file payload up front so a malformed report is rejected before we
        // schedule an attach. The raw bytes (not the decoded value) are forwarded into the resolve
        // node, which re-decodes them on the compute side.
        if let Err(e) =
            serde_json::from_slice::<Vec<iceberg::spec::SerializedDataFile>>(v3_output_bytes)
        {
            tracing::warn!(
                sink_id = %sink_id,
                task_id,
                error = %e,
                "Failed to deserialize v3_output_files from compaction report; skipping V3 routing"
            );
            return false;
        }
        let input_file_paths: Vec<String> = match serde_json::from_slice(v3_input_bytes) {
            Ok(paths) => paths,
            Err(e) => {
                tracing::warn!(
                    sink_id = %sink_id,
                    task_id,
                    error = %e,
                    "Failed to deserialize v3_input_files from compaction report; skipping V3 routing"
                );
                return false;
            }
        };

        // Derive the PK column names from the sink's `downstream_pk` (indices into the sink's visible
        // columns), preserving that index order — it is the writer's index-key column order, which the
        // resolver must match when it packs PKs. The V3 sink never writes Iceberg identifier-field-ids,
        // so the schema cannot supply the PK; these named columns do.
        let sink_param = match self.get_sink_param(sink_id).await {
            Ok(p) => p,
            Err(e) => {
                tracing::warn!(
                    sink_id = %sink_id,
                    task_id,
                    error = %e.as_report(),
                    "Failed to load sink param for V3 compaction PK derivation; skipping V3 routing"
                );
                return false;
            }
        };
        let Some(downstream_pk) = sink_param
            .downstream_pk
            .as_ref()
            .filter(|pk| !pk.is_empty())
        else {
            tracing::warn!(
                sink_id = %sink_id,
                task_id,
                "V3 sink has no downstream PK; cannot resolve compaction"
            );
            return false;
        };
        let Some(pk_column_names) = downstream_pk
            .iter()
            .map(|&i| sink_param.columns.get(i).map(|c| c.name.clone()))
            .collect::<Option<Vec<_>>>()
        else {
            tracing::warn!(
                sink_id = %sink_id,
                task_id,
                "V3 sink downstream PK index out of range of sink columns; cannot resolve compaction"
            );
            return false;
        };

        // ATTACH a transient resolve pipeline onto the writer fragment so the writer repairs its
        // PK index and emits a coordinated overwrite folded into the bracketing checkpoint epoch.
        // `read_snapshot_id` is not used by the stream-resolve path: the resolver derives liveness
        // from the writer's pk-index, not the read snapshot.
        let _ = read_snapshot_id;
        let Some(barrier_scheduler) = self.barrier_scheduler.clone() else {
            tracing::warn!(
                sink_id = %sink_id,
                task_id,
                "no barrier scheduler available; cannot attach V3 resolve pipeline"
            );
            return false;
        };

        // Resolve the writer fragment + database for this sink, and pre-allocate the resolve
        // fragment id so we can record the attachment for a later DETACH.
        // The V3 writer fragment carries no `FragmentTypeFlag::Sink`, so it cannot be found via the
        // generic sink-fragment lookup; match on the `IcebergWithPkIndexWriter` node body instead.
        let writer_fragment_id = match self
            .metadata_manager
            .catalog_controller
            .get_iceberg_v3_writer_fragment_id(sink_id)
            .await
        {
            Ok(fragment_id) => fragment_id,
            Err(e) => {
                tracing::warn!(
                    sink_id = %sink_id,
                    task_id,
                    error = %e.as_report(),
                    "Failed to resolve writer fragment for V3 resolve attach; skipping"
                );
                return false;
            }
        };
        let database_id = match self
            .metadata_manager
            .catalog_controller
            .get_object_database_id(sink_id)
            .await
        {
            Ok(database_id) => database_id,
            Err(e) => {
                tracing::warn!(
                    sink_id = %sink_id,
                    task_id,
                    error = %e.as_report(),
                    "Failed to resolve database for V3 resolve attach; skipping"
                );
                return false;
            }
        };
        let resolve_fragment_id = risingwave_common::id::FragmentId::from(
            self.env
                .id_gen_manager()
                .generate::<{ crate::controller::id::IdCategory::Fragment }>() as u32,
        );

        // Decode the output files once for arming the coordinator (the raw bytes were validated
        // decodable above). The coordinator folds these into the bracketing epoch's overwrite when the
        // writer reports `Resolver`-done; the raw bytes still travel to the resolve node via the plan.
        let output_files: Vec<iceberg::spec::SerializedDataFile> =
            match serde_json::from_slice(v3_output_bytes) {
                Ok(files) => files,
                Err(e) => {
                    tracing::warn!(
                        sink_id = %sink_id,
                        task_id,
                        error = %e,
                        "Failed to decode v3_output_files for coordinator arm; skipping V3 routing"
                    );
                    return false;
                }
            };

        // Backstop against double-attach: if a resolve pipeline is already in flight for this sink
        // (attached but not yet committed/detached), do NOT attach a second one onto the same writer
        // fragment — that double-attach wedges the attach barrier. The scheduler already skips such
        // sinks, but the manual `VACUUM` path can reach here directly, so guard again. Returning false
        // re-queues the task; by the retry the first resolve has detached and the sink is compacted.
        if self.inner.read().attached_resolves.contains_key(&sink_id) {
            tracing::info!(
                sink_id = %sink_id,
                task_id,
                "a V3 resolve pipeline is already attached for this sink; skipping double-attach"
            );
            return false;
        }

        // Arm the coordinator BEFORE opening the resolve window so the bracketing-checkpoint
        // `Resolver`-done report always finds the held overwrite state.
        if let Err(e) = self
            .iceberg_v3_sink_manager
            .arm_compaction_resolve(sink_id, output_files, input_file_paths.clone())
            .await
        {
            tracing::warn!(
                sink_id = %sink_id,
                task_id,
                error = %e.as_report(),
                "Failed to arm V3 compaction coordinator; skipping V3 routing"
            );
            return false;
        }

        let plan = crate::barrier::IcebergV3ResolveAttachPlan {
            database_id,
            sink_id,
            writer_fragment_id,
            resolve_fragment_id,
            output_files: v3_output_bytes.clone(),
            input_file_paths: input_file_paths.clone(),
            pk_column_names,
            parallelism: None,
        };

        // Ordering is load-bearing: the writer DROPS remap-edge candidates while not in resolve-mode,
        // so the `Start` mutation MUST land on a barrier STRICTLY BEFORE the attach `Update` adds the
        // resolve actors. `Start` and the attach `Update` cannot ride the same barrier (one is a bare
        // mutation, the other an `Update`), so sequence them as two consecutive `run_command`s: the
        // first await returns only once `Start` is collected, guaranteeing it precedes the attach.
        tracing::info!(
            target: "iceberg_v3_resolve",
            sink_id = %sink_id,
            task_id,
            writer_fragment_id = %writer_fragment_id,
            resolve_fragment_id = %resolve_fragment_id,
            "injecting V3 resolve Start mutation (step 1/2)"
        );
        if let Err(e) = barrier_scheduler
            .run_command(
                database_id,
                crate::barrier::Command::IcebergV3ResolvePhase {
                    sink_id,
                    phase: risingwave_pb::stream_plan::iceberg_v3_resolve_mutation::Phase::Start,
                    input_file_paths,
                },
            )
            .await
        {
            tracing::warn!(
                sink_id = %sink_id,
                task_id,
                error = %e.as_report(),
                "V3 compaction resolve Start injection failed; compaction task will be retried"
            );
            return false;
        }

        tracing::info!(
            target: "iceberg_v3_resolve",
            sink_id = %sink_id,
            task_id,
            resolve_fragment_id = %resolve_fragment_id,
            "Start collected; injecting V3 AttachResolve Update mutation (step 2/2)"
        );
        match barrier_scheduler
            .run_command(
                database_id,
                crate::barrier::Command::IcebergV3AttachResolve(Box::new(plan)),
            )
            .await
        {
            Ok(()) => {
                tracing::info!(
                    target: "iceberg_v3_resolve",
                    sink_id = %sink_id,
                    task_id,
                    "V3 AttachResolve Update collected; resolve pipeline attached"
                );
                // Record the attachment so a later DETACH (a separate task) targets this fragment.
                self.inner.write().attached_resolves.insert(
                    sink_id,
                    crate::manager::iceberg_compaction::AttachedResolve {
                        writer_fragment_id,
                        resolve_fragment_id,
                    },
                );
                true
            }
            Err(e) => {
                tracing::warn!(
                    sink_id = %sink_id,
                    task_id,
                    error = %e.as_report(),
                    "V3 compaction resolve attach failed; compaction task will be retried"
                );
                false
            }
        }
    }

    /// Build and schedule the DETACH for a previously attached resolve pipeline. Injects the `End`
    /// resolve-phase mutation FIRST (so the writer exits resolve-mode and resumes) on its own barrier,
    /// then the detach `Update` that drops the resolve actors and clears the writer's remap `Merge`.
    /// Mirrors the ordering of ATTACH (`End` before detach, just as `Start` precedes attach). Removes
    /// the `attached_resolves` entry ONLY after both commands succeed, so a transient failure leaves
    /// the entry in place and [`Self::resolve_completion_loop`] retries on the next tick instead of
    /// leaking the transient fragment and wedging the writer. Keeping the entry until success also
    /// keeps the scheduler's double-attach guard active across the whole detach window.
    ///
    /// Reached only via [`Self::try_complete_v3_resolve`], driven by [`Self::resolve_completion_loop`].
    pub(crate) async fn detach_v3_resolve(&self, sink_id: SinkId) -> MetaResult<()> {
        // Read (do NOT remove yet); the entry is dropped only after the End + Detach commands land.
        let Some(attached) = self.inner.read().attached_resolves.get(&sink_id).copied() else {
            return Ok(());
        };
        let Some(barrier_scheduler) = self.barrier_scheduler.clone() else {
            return Ok(());
        };
        let database_id = self
            .metadata_manager
            .catalog_controller
            .get_object_database_id(sink_id)
            .await?;

        // `End` first: the writer leaves resolve-mode and resumes its normal path BEFORE the resolve
        // actors are dropped, symmetric to `Start` preceding attach.
        barrier_scheduler
            .run_command(
                database_id,
                crate::barrier::Command::IcebergV3ResolvePhase {
                    sink_id,
                    phase: risingwave_pb::stream_plan::iceberg_v3_resolve_mutation::Phase::End,
                    input_file_paths: vec![],
                },
            )
            .await?;

        let plan = crate::barrier::IcebergV3ResolveDetachPlan {
            database_id,
            sink_id,
            writer_fragment_id: attached.writer_fragment_id,
            resolve_fragment_id: attached.resolve_fragment_id,
        };
        barrier_scheduler
            .run_command(
                database_id,
                crate::barrier::Command::IcebergV3DetachResolve(plan),
            )
            .await?;

        // Both commands landed; now it is safe to forget the attachment. A failure above returns early
        // (via `?`) with the entry still present, so the completion loop retries the detach next tick.
        self.inner.write().attached_resolves.remove(&sink_id);
        Ok(())
    }

    /// Completion trigger: if the sink has an attached resolve pipeline whose overwrite has already
    /// committed (the coordinator cleared its armed `pending_compaction` when it folded the overwrite
    /// into the bracketing epoch), fire `End` + DETACH and tear the pipeline down. Idempotent and a
    /// no-op when nothing is attached or the overwrite has not committed yet.
    ///
    /// Driven by [`Self::resolve_completion_loop`], which polls every attached sink on a short timer.
    /// A timer (rather than an inline call from barrier-completion) is used deliberately: this method
    /// injects barriers/commands via the `barrier_scheduler`, so calling it from inside the
    /// barrier-completion critical section would risk a re-entrant deadlock.
    pub(crate) async fn try_complete_v3_resolve(&self, sink_id: SinkId) -> MetaResult<()> {
        let attached = self.inner.read().attached_resolves.contains_key(&sink_id);
        if !attached {
            return Ok(());
        }
        // The coordinator clears its armed window the moment it folds the overwrite into the bracketing
        // epoch's pre-commit. A still-armed window means the `Resolver`-done report has not yet been
        // processed, so the overwrite is not committed — hold the pipeline open.
        if self
            .iceberg_v3_sink_manager
            .has_pending_compaction(sink_id)
            .await
        {
            return Ok(());
        }
        self.detach_v3_resolve(sink_id).await
    }

    /// Periodic driver that completes attached V3 compaction-resolve pipelines. Runs in its OWN task
    /// (spawned at meta boot) rather than inline with barrier completion: [`Self::try_complete_v3_resolve`]
    /// injects barriers/commands through the `barrier_scheduler`, so driving it from within the
    /// barrier-completion critical section could re-enter and deadlock. The interval is kept short so a
    /// writer does not stay blocked-in-resolve much longer than necessary after its overwrite commits.
    pub fn resolve_completion_loop(
        manager: Arc<Self>,
        interval: Duration,
    ) -> (tokio::task::JoinHandle<()>, oneshot::Sender<()>) {
        let (shutdown_tx, mut shutdown_rx) = oneshot::channel();
        let join_handle = tokio::spawn(async move {
            tracing::info!(?interval, "Starting Iceberg V3 resolve-completion loop");
            let mut ticker = tokio::time::interval(interval);
            loop {
                tokio::select! {
                    _ = ticker.tick() => {
                        // Snapshot the keys under a short read lock and drop it before any await, so the
                        // lock is never held across the per-sink command injection inside
                        // `try_complete_v3_resolve`.
                        let sink_ids = {
                            let guard = manager.inner.read();
                            guard.attached_resolves.keys().copied().collect::<Vec<_>>()
                        };
                        for sink_id in sink_ids {
                            // One sink's failure must not stall completion of the others; log and continue.
                            if let Err(e) = manager.try_complete_v3_resolve(sink_id).await {
                                tracing::warn!(
                                    sink_id = %sink_id,
                                    error = %e.as_report(),
                                    "Failed to complete V3 compaction resolve; will retry next tick"
                                );
                            }
                        }
                    },
                    _ = &mut shutdown_rx => {
                        tracing::info!("Iceberg V3 resolve-completion loop is stopped");
                        return;
                    }
                }
            }
        });
        (join_handle, shutdown_tx)
    }

    pub async fn handle_report_task(&self, report: IcebergReportTask) {
        let sink_id = SinkId::from(report.sink_id);
        let task_id = report.task_id;
        let status = IcebergReportTaskStatus::try_from(report.status)
            .unwrap_or(IcebergReportTaskStatus::Unspecified);
        let now = Instant::now();

        // For SUCCESS reports with a V3 payload, attempt the iceberg overwrite commit BEFORE
        // touching the state machine. The commit outcome controls whether the track finishes
        // successfully (commit ok) or as failed (commit error, so the scheduler retries). For
        // non-SUCCESS reports no commit is attempted and `commit_ok` stays `false`.
        let commit_ok = if status == IcebergReportTaskStatus::Success {
            self.route_v3_compaction_report(&report, sink_id, task_id)
                .await
        } else {
            false
        };

        let waiter = {
            let mut guard = self.inner.write();
            let mut waiter = None;

            match guard.sink_schedules.get_mut(&sink_id) {
                Some(track) if track.is_processing_task(task_id) => {
                    let finish_action = match status {
                        IcebergReportTaskStatus::Success => {
                            // A V3 report whose commit failed should be retried; treat it as a
                            // failure so the scheduler re-dispatches on the next interval.
                            if report.v3_output_files.is_some() && !commit_ok {
                                tracing::warn!(
                                    sink_id = %sink_id,
                                    task_id,
                                    "V3 compaction commit did not succeed; marking task failed for retry"
                                );
                                track.finish_failed(now)
                            } else {
                                track.finish_success(now)
                            }
                        }
                        IcebergReportTaskStatus::Failed | IcebergReportTaskStatus::Unspecified => {
                            tracing::warn!(
                                sink_id = %sink_id,
                                task_id,
                                error_message = report.error_message.clone().unwrap_or_default(),
                                "Iceberg compaction task reported failure"
                            );
                            track.finish_failed(now)
                        }
                    };

                    Self::apply_track_finish_action(&mut guard, sink_id, finish_action);
                    waiter = guard.manual_compaction_waiters.remove(&sink_id);
                }
                Some(_) => {
                    tracing::warn!(sink_id = %sink_id, task_id, "Ignoring stale iceberg compaction report");
                }
                None => {
                    tracing::warn!(sink_id = %sink_id, task_id, "Received iceberg compaction report for unknown sink");
                }
            }

            waiter
        };

        if let Some(waiter) = waiter {
            Self::complete_manual_task_waiter(waiter, &report);
        }
    }
}

#[cfg(test)]
mod tests;
