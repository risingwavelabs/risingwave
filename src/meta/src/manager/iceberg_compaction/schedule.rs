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

use std::collections::BTreeMap;
use std::collections::hash_map::Entry;
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime};

use futures::StreamExt;
use itertools::Itertools;
use parking_lot::RwLock;
use risingwave_common::license::Feature;
use risingwave_connector::connector_common::{
    IcebergCommittedSnapshot, IcebergSinkCompactionUpdate,
};
use risingwave_connector::sink::SinkParam;
use risingwave_connector::sink::catalog::{SinkCatalog, SinkId};
use risingwave_connector::sink::iceberg::{
    CompactionType, IcebergConfig, IcebergWriteMode, commit_branch,
};
use risingwave_hummock_sdk::HummockContextId;
use risingwave_pb::catalog::PbStreamJobStatus;
use risingwave_pb::iceberg_compaction::IcebergCompactionTask;
use risingwave_pb::iceberg_compaction::iceberg_compaction_task::TaskType;
use risingwave_pb::iceberg_compaction::subscribe_iceberg_compaction_event_request::ReportTask as IcebergReportTask;
use risingwave_pb::iceberg_compaction::subscribe_iceberg_compaction_event_request::report_task::Status as IcebergReportTaskStatus;
use risingwave_pb::id::IcebergCompactionTaskId;
use thiserror_ext::AsReport;
use tokio::sync::oneshot;
use tokio::task::JoinHandle;

use super::*;
use crate::manager::iceberg_pk_index_sink::IcebergPkIndexSinkRegistration;

mod report;

const COMPACTION_RETRY_BACKOFF: Duration = Duration::from_secs(1);

/// Scheduler lifecycle for one sink.
#[derive(Debug, Clone)]
enum CompactionTrackState {
    /// Ready to accept commits and check for trigger conditions.
    ///
    /// `Idle` is not an active attempt state. A manual request may leave a
    /// one-shot task here for the next scheduler selection.
    Idle {
        next_compaction_time: Instant,
        /// A one-shot manual task, consumed when the next attempt starts.
        manual_task_type: Option<TaskType>,
    },
    /// An attempt has been selected, but its task has not been sent to a compactor.
    PendingDispatch { attempt: Arc<CompactionAttempt> },
    /// Compaction task is in-flight. `report_deadline` acts as a lease; if it
    /// expires before a report arrives, the task becomes retryable.
    InFlight {
        task_id: IcebergCompactionTaskId,
        compactor_context_id: HummockContextId,
        attempt: Arc<CompactionAttempt>,
        report_deadline: Instant,
    },
    /// A validated rewrite is being applied. Recovery discards the track and independently
    /// protects GC until the new coordinator has settled durable pending commits.
    Applying {
        task_id: IcebergCompactionTaskId,
        attempt: Arc<CompactionAttempt>,
    },
    /// An admitted apply has an unknown outcome; do not retry before recovery.
    NeedsRecovery {
        task_id: IcebergCompactionTaskId,
        attempt: Arc<CompactionAttempt>,
    },
}

/// Immutable task parameters captured when the scheduler selects an attempt.
///
/// The track and async dispatch handle share these parameters so later commits
/// or config updates cannot change the task being dispatched.
#[derive(Debug)]
struct CompactionAttempt {
    policy: CompactionPolicy,
    pk_index_coordinated: bool,
    max_file_sequence_number: Option<i64>,
    pending_commit_count_at_start: usize,
    gc_watermark_snapshot: Option<IcebergCommittedSnapshot>,
}

#[derive(Debug, Clone, Copy)]
struct ScheduledCompactionTask {
    task_id: IcebergCompactionTaskId,
    compactor_context_id: HummockContextId,
}

/// Outcome history of one sink's compaction attempts.
///
/// This is observability-only state: it never affects scheduling decisions, and it
/// is lost when meta restarts or the track is removed.
#[derive(Debug, Clone, Default)]
struct CompactionHealth {
    last_success_at: Option<SystemTime>,
    last_failure_at: Option<SystemTime>,
    last_error: Option<String>,
    consecutive_failures: u32,
    /// Latest observed snapshot whose commits have been consumed by a completed
    /// compaction.
    compacted_snapshot: Option<IcebergCommittedSnapshot>,
    /// Snapshot that started the active sequence-bounded round. It becomes
    /// `compacted_snapshot` when the round drains.
    round_start_snapshot: Option<IcebergCommittedSnapshot>,
}

impl CompactionHealth {
    fn record_progress(&mut self) {
        self.last_success_at = Some(SystemTime::now());
        self.consecutive_failures = 0;
    }

    fn record_completion(&mut self, compacted_snapshot: Option<IcebergCommittedSnapshot>) {
        self.record_progress();
        if compacted_snapshot.is_some() {
            self.compacted_snapshot = compacted_snapshot;
        }
    }

    fn record_failure(&mut self, error: String) {
        self.last_failure_at = Some(SystemTime::now());
        self.last_error = Some(error);
        self.consecutive_failures = self.consecutive_failures.saturating_add(1);
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum CompactionTrackFinishAction {
    KeepTrack,
    RemoveTrack,
}

/// Selection parameters are typed in memory and serialized only at dispatch.
#[derive(Debug, Clone, PartialEq, Eq)]
struct CompactionPolicy {
    task_type: TaskType,
    small_files_threshold_mb: u64,
    target_file_size_mb: u64,
    delete_ratio_percent: u32,
}

impl CompactionPolicy {
    fn properties(&self) -> BTreeMap<String, String> {
        BTreeMap::from([
            (
                "compaction.small_files_threshold_mb".to_owned(),
                self.small_files_threshold_mb.to_string(),
            ),
            (
                "compaction.target_file_size_mb".to_owned(),
                self.target_file_size_mb.to_string(),
            ),
            (
                "compaction.delete_ratio_percent".to_owned(),
                self.delete_ratio_percent.to_string(),
            ),
        ])
    }
}

/// Latest catalog configuration, independent of an in-flight attempt's frozen policy.
#[derive(Debug, Clone)]
struct CompactionScheduleConfig {
    policy: CompactionPolicy,
    write_mode: IcebergWriteMode,
    pk_index_coordinated: bool,
    automatic_enabled: bool,
    trigger_interval_sec: u64,
    trigger_snapshot_count: usize,
    report_timeout: Duration,
    refreshed_at: Instant,
}

#[derive(Debug, Clone, Default)]
struct CompactionDemand {
    commit_count: usize,
    since: Option<Instant>,
    reconciliation_pending: bool,
}

impl CompactionDemand {
    fn record_commit(&mut self, now: Instant) {
        self.since.get_or_insert(now);
        self.commit_count = self.commit_count.saturating_add(1);
    }

    fn consume_for_round(&mut self) {
        *self = Self::default();
    }

    fn force_check(&mut self, now: Instant, pk_index_coordinated: bool) {
        self.commit_count = self.commit_count.max(1);
        self.since.get_or_insert(now);
        self.reconciliation_pending |= pk_index_coordinated;
    }

    fn is_ready(&self, config: &CompactionScheduleConfig, now: Instant) -> bool {
        self.reconciliation_pending
            || self.commit_count >= config.trigger_snapshot_count
            || self.since.is_some_and(|since| {
                now.saturating_duration_since(since)
                    >= Duration::from_secs(config.trigger_interval_sec)
            })
    }
}

/// A boundary always exists while work is active. Beneficial PK-index batches can
/// advance it for new writes; PK-index Full keeps its boundary and policy until drained.
#[derive(Debug, Clone)]
struct CompactionRound {
    max_file_sequence_number: i64,
    policy: Option<CompactionPolicy>,
}

#[derive(Debug, Clone)]
struct CompactionBlocked {
    reason: IcebergReportTaskStatus,
    since: Instant,
}

#[derive(Debug, Clone, Default)]
struct CompactionRetry {
    not_before: Option<Instant>,
    consecutive_failures: u32,
    blocked: Option<CompactionBlocked>,
}

impl CompactionRetry {
    fn is_waiting(&self, now: Instant) -> bool {
        self.not_before.is_some_and(|deadline| now < deadline)
    }

    fn record_failure(&mut self, pk_index_coordinated: bool, now: Instant) -> Instant {
        let delay = if pk_index_coordinated {
            let delay = Self::delay(self.consecutive_failures, &mut rand::rng());
            self.consecutive_failures = self.consecutive_failures.saturating_add(1);
            delay
        } else {
            COMPACTION_RETRY_BACKOFF
        };
        let retry_at = now + delay;
        if pk_index_coordinated {
            self.not_before = Some(retry_at);
        }
        retry_at
    }

    fn record_blocked(&mut self, reason: IcebergReportTaskStatus, now: Instant) -> Instant {
        let delay = if reason == IcebergReportTaskStatus::Busy {
            COMPACTION_RETRY_BACKOFF
        } else {
            let since = self.blocked.as_ref().map_or(now, |blocked| blocked.since);
            self.blocked = Some(CompactionBlocked { reason, since });
            Duration::from_secs(60)
        };
        let retry_at = now + delay;
        self.not_before = Some(retry_at);
        retry_at
    }

    fn reset(&mut self) {
        *self = Self::default();
    }

    fn delay(failures: u32, rng: &mut impl rand::Rng) -> Duration {
        let base = Duration::from_secs(1_u64 << failures.min(6));
        let jitter = Duration::from_millis(rng.random_range(0..=base.as_millis() as u64 / 4));
        (base + jitter).min(Duration::from_secs(60))
    }
}

#[derive(Debug, Clone)]
pub(super) struct CompactionTrack {
    config: CompactionScheduleConfig,
    pub(super) registration: Option<Arc<IcebergPkIndexSinkRegistration>>,
    demand: CompactionDemand,
    round: Option<CompactionRound>,
    retry: CompactionRetry,
    latest_observed_snapshot: Option<IcebergCommittedSnapshot>,
    /// Minimum interval between new PK-index rounds, not between continuations.
    next_check_not_before: Option<Instant>,
    /// Legacy temporary/disabled tracks are removed after their task finishes.
    finish_action: CompactionTrackFinishAction,
    state: CompactionTrackState,
    health: CompactionHealth,
}

impl CompactionTrack {
    fn new(config: CompactionScheduleConfig, now: Instant) -> Self {
        let next_compaction_time = now + Duration::from_secs(config.trigger_interval_sec);
        Self {
            config,
            registration: None,
            demand: CompactionDemand::default(),
            round: None,
            retry: CompactionRetry::default(),
            latest_observed_snapshot: None,
            next_check_not_before: None,
            finish_action: CompactionTrackFinishAction::KeepTrack,
            state: CompactionTrackState::Idle {
                next_compaction_time,
                manual_task_type: None,
            },
            health: CompactionHealth::default(),
        }
    }

    pub(super) fn source_is_ready(&self) -> bool {
        !self.config.pk_index_coordinated
            || self
                .registration
                .as_ref()
                .is_some_and(|registration| registration.is_ready())
    }

    pub(super) fn pk_index_registration(&self) -> Option<&Arc<IcebergPkIndexSinkRegistration>> {
        self.registration
            .as_ref()
            .filter(|_| self.config.pk_index_coordinated)
    }

    /// Determines if compaction should be triggered.
    ///
    /// Trigger conditions:
    /// - An active automatic round follows its next-attempt time. PK-index batches
    ///   can also continue without new commits or trigger early at the commit threshold.
    /// - Otherwise, pending commit threshold or an interval with pending commits
    ///   can trigger a task.
    ///
    /// This ensures:
    /// - The configured commit threshold is an early trigger
    /// - The configured interval is the maximum dirty wait
    /// - Force compaction works by setting `next_compaction_time` to now
    /// - No legacy empty compaction runs (an active automatic round is intentionally retryable)
    fn should_trigger(&self, now: Instant) -> bool {
        if self.retry.is_waiting(now) {
            return false;
        }
        let next_compaction_time = match &self.state {
            CompactionTrackState::Idle {
                next_compaction_time,
                manual_task_type,
            } => {
                if self.config.pk_index_coordinated && manual_task_type.is_some() {
                    return true;
                }
                if self.config.pk_index_coordinated && !self.config.automatic_enabled {
                    return false;
                }
                *next_compaction_time
            }
            CompactionTrackState::PendingDispatch { .. }
            | CompactionTrackState::InFlight { .. }
            | CompactionTrackState::Applying { .. }
            | CompactionTrackState::NeedsRecovery { .. } => return false,
        };

        let time_ready = now >= next_compaction_time;
        let commit_ready = self.demand.commit_count >= self.config.trigger_snapshot_count;
        let has_commits = self.demand.commit_count > 0;
        if self.round.is_some() {
            // Continue immediately below the same PK-index boundary when no new
            // commits are pending. Advancing the boundary must honor the cooldown
            // or commit threshold, including commits received after success.
            // Neither continuation nor new demand can bypass failure backoff.
            return time_ready
                || (self.config.pk_index_coordinated && (commit_ready || !has_commits));
        }

        if self.config.pk_index_coordinated {
            // An empty manual Full can finish before the first snapshot is observed.
            return self.latest_observed_snapshot.is_some()
                && !self
                    .next_check_not_before
                    .is_some_and(|deadline| now < deadline)
                && self.demand.is_ready(&self.config, now);
        }
        commit_ready || (time_ready && has_commits)
    }

    fn record_observed_snapshot(&mut self, observed_snapshot: IcebergCommittedSnapshot) -> bool {
        if self.config.pk_index_coordinated
            && let Some(previous) = &self.latest_observed_snapshot
            && previous.branch == observed_snapshot.branch
            && (previous.snapshot_id == observed_snapshot.snapshot_id
                || matches!(
                    (previous.max_file_sequence_number, observed_snapshot.max_file_sequence_number),
                    (Some(previous), Some(current)) if current <= previous
                ))
        {
            return false;
        }
        self.latest_observed_snapshot = Some(observed_snapshot);
        true
    }

    #[cfg(test)]
    fn record_commit(&mut self) {
        self.record_commit_at(Instant::now());
    }

    fn record_commit_at(&mut self, now: Instant) {
        self.demand.record_commit(now);
    }

    fn record_force_compaction(&mut self, now: Instant, forced_task_type: Option<TaskType>) {
        if let CompactionTrackState::Idle {
            next_compaction_time,
            manual_task_type,
        } = &mut self.state
        {
            if let Some(task_type) = forced_task_type {
                *manual_task_type = Some(task_type);
            }
            *next_compaction_time = now;
            // A force signal during active work advances the next attempt without
            // adding another demand event.
            if self.round.is_none() {
                self.demand
                    .force_check(now, self.config.pk_index_coordinated);
            }
        }
    }

    fn needs_config_refresh(&self, now: Instant, refresh_interval: Duration) -> bool {
        now.saturating_duration_since(self.config.refreshed_at) >= refresh_interval
    }

    fn effective_task_type(&self) -> TaskType {
        match &self.state {
            CompactionTrackState::Idle {
                manual_task_type, ..
            } => manual_task_type.unwrap_or(self.config.policy.task_type),
            CompactionTrackState::PendingDispatch { attempt }
            | CompactionTrackState::InFlight { attempt, .. }
            | CompactionTrackState::Applying { attempt, .. }
            | CompactionTrackState::NeedsRecovery { attempt, .. } => attempt.policy.task_type,
        }
    }

    fn start_attempt(&mut self) -> Arc<CompactionAttempt> {
        let CompactionTrackState::Idle {
            manual_task_type, ..
        } = &mut self.state
        else {
            unreachable!("Cannot start an attempt while another attempt is active")
        };

        let manual_task_type = manual_task_type.take();
        let mut policy = self
            .round
            .as_ref()
            .and_then(|round| round.policy.as_ref())
            .unwrap_or(&self.config.policy)
            .clone();
        if let Some(task_type) = manual_task_type {
            policy.task_type = task_type;
        }
        let starts_round = self.round.is_none();
        let refresh_pk_index_batch = self.config.pk_index_coordinated
            && policy.task_type != TaskType::Full
            && (self.demand.commit_count > 0 || self.demand.reconciliation_pending);
        let new_round_boundary = if manual_task_type.is_none()
            && self.config.write_mode == IcebergWriteMode::MergeOnRead
            && (starts_round || refresh_pk_index_batch)
        {
            self.latest_observed_snapshot
                .as_ref()
                .expect("automatic merge-on-read compaction must start from an observed snapshot")
                .max_file_sequence_number
        } else {
            None
        };

        // Each PK-index batch uses the latest write demand and configured policy.
        // Maintenance alone must not advance the boundary, or a small-file rewrite
        // could repeatedly select its own outputs without any new ingestion.
        // Full must also retain a fixed scope because every file remains eligible.
        if let Some(max_file_sequence_number) = new_round_boundary {
            self.health.round_start_snapshot = self.latest_observed_snapshot.clone();
            self.round = Some(CompactionRound {
                max_file_sequence_number,
                policy: (self.config.pk_index_coordinated && policy.task_type == TaskType::Full)
                    .then(|| policy.clone()),
            });
            self.demand.consume_for_round();
            if self.config.pk_index_coordinated && starts_round {
                self.next_check_not_before = Some(Instant::now() + Duration::from_secs(10));
            }
        } else if self.config.pk_index_coordinated
            && policy.task_type == TaskType::Full
            && let Some(round) = &mut self.round
        {
            round.policy = Some(policy.clone());
        }
        let attempt = Arc::new(CompactionAttempt {
            policy,
            pk_index_coordinated: self.config.pk_index_coordinated,
            max_file_sequence_number: self.round_boundary(),
            pending_commit_count_at_start: self.demand.commit_count,
            gc_watermark_snapshot: self.latest_observed_snapshot.clone(),
        });
        self.state = CompactionTrackState::PendingDispatch {
            attempt: attempt.clone(),
        };
        attempt
    }

    fn mark_dispatched(
        &mut self,
        task_id: IcebergCompactionTaskId,
        compactor_context_id: HummockContextId,
        now: Instant,
    ) {
        let CompactionTrackState::PendingDispatch { attempt } = &self.state else {
            unreachable!("Only a pending attempt can be marked as dispatched")
        };
        self.state = CompactionTrackState::InFlight {
            task_id,
            compactor_context_id,
            attempt: attempt.clone(),
            report_deadline: now + self.config.report_timeout,
        };
    }

    pub(super) fn active_attempt_gc_watermark_snapshot(
        &self,
    ) -> Option<Option<&IcebergCommittedSnapshot>> {
        match &self.state {
            CompactionTrackState::PendingDispatch { attempt }
            | CompactionTrackState::InFlight { attempt, .. }
            | CompactionTrackState::Applying { attempt, .. }
            | CompactionTrackState::NeedsRecovery { attempt, .. } => {
                Some(attempt.gc_watermark_snapshot.as_ref())
            }
            CompactionTrackState::Idle { .. } => None,
        }
    }

    fn is_pending_dispatch(&self) -> bool {
        matches!(self.state, CompactionTrackState::PendingDispatch { .. })
    }

    fn removes_track_after_finish(&self) -> bool {
        self.finish_action == CompactionTrackFinishAction::RemoveTrack
    }

    fn has_queued_task_or_active_attempt(&self) -> bool {
        matches!(
            &self.state,
            CompactionTrackState::Idle {
                manual_task_type: Some(_),
                ..
            } | CompactionTrackState::PendingDispatch { .. }
                | CompactionTrackState::InFlight { .. }
                | CompactionTrackState::Applying { .. }
                | CompactionTrackState::NeedsRecovery { .. }
        )
    }

    pub(super) fn is_in_flight_task(&self, task_id: IcebergCompactionTaskId) -> bool {
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
            CompactionTrackState::Idle { .. }
            | CompactionTrackState::PendingDispatch { .. }
            | CompactionTrackState::Applying { .. }
            | CompactionTrackState::NeedsRecovery { .. } => None,
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

    fn finish_failed(&mut self, now: Instant, error: String) -> CompactionTrackFinishAction {
        if !matches!(
            self.state,
            CompactionTrackState::InFlight { .. } | CompactionTrackState::Applying { .. }
        ) {
            unreachable!("Only an in-flight or applying attempt can finish")
        }
        self.health.record_failure(error);
        let retry_at = self
            .retry
            .record_failure(self.config.pk_index_coordinated, now);
        self.state = CompactionTrackState::Idle {
            next_compaction_time: retry_at,
            manual_task_type: None,
        };
        self.end_disabled_round();
        self.finish_action
    }

    fn finish_blocked(
        &mut self,
        reason: IcebergReportTaskStatus,
        now: Instant,
    ) -> CompactionTrackFinishAction {
        let retry_at = self.retry.record_blocked(reason, now);
        self.state = CompactionTrackState::Idle {
            next_compaction_time: retry_at,
            manual_task_type: None,
        };
        self.end_disabled_round();
        self.finish_action
    }

    /// Re-queue the track as idle after a pre-dispatch failure.
    ///
    /// Pending demand is intentionally preserved so commits that arrive
    /// while the track is pending dispatch are not lost if task dispatch fails
    /// before the compactor accepts the task.
    ///
    /// `next_compaction_time` starts a short retry backoff rather than restoring
    /// its previous timestamp. Candidates are dispatched in ascending order of
    /// this field, so restoring a stale timestamp would let a repeatedly-failing
    /// track sort ahead of every healthy sink and monopolize dispatch slots.
    fn revert_pre_dispatch_failure(&mut self, now: Instant) -> CompactionTrackFinishAction {
        if !self.is_pending_dispatch() {
            unreachable!("Only a pending attempt can be reverted")
        }
        self.health
            .record_failure("compaction task failed before dispatch to a compactor".to_owned());
        let retry_at = self
            .retry
            .record_failure(self.config.pk_index_coordinated, now);
        self.state = CompactionTrackState::Idle {
            next_compaction_time: retry_at,
            manual_task_type: None,
        };
        self.end_disabled_round();
        self.finish_action
    }

    fn update_interval(&mut self, new_interval_sec: u64, now: Instant) {
        if self.config.trigger_interval_sec == new_interval_sec {
            return;
        }

        self.config.trigger_interval_sec = new_interval_sec;
        if self.round.is_some() {
            // The new interval applies after the next successful batch or when the
            // round drains. Preserve the current continuation/cooldown/retry deadline.
            return;
        }

        match &mut self.state {
            CompactionTrackState::Idle {
                next_compaction_time,
                ..
            } => {
                *next_compaction_time = now + Duration::from_secs(new_interval_sec);
            }
            CompactionTrackState::PendingDispatch { .. }
            | CompactionTrackState::InFlight { .. }
            | CompactionTrackState::Applying { .. }
            | CompactionTrackState::NeedsRecovery { .. } => {}
        }
    }

    fn finish_success(&mut self, now: Instant) -> CompactionTrackFinishAction {
        self.retry.reset();
        let (CompactionTrackState::InFlight { attempt, .. }
        | CompactionTrackState::Applying { attempt, .. }) = &self.state
        else {
            unreachable!("Only an in-flight or applying attempt can finish")
        };
        if attempt.max_file_sequence_number.is_some() {
            // Success means this attempt made progress, but only `Drained`
            // proves that no work remains below this attempt's boundary.
            self.health.record_progress();
            // Beneficial PK-index batches can advance their boundary on new commits,
            // so continuous ingestion may prevent the round from ever draining.
            // Keep the cooldown even if commits arrive only after this success.
            // should_trigger still allows immediate continuation without new commits
            // to finish deferred plans and the empty scan. Full keeps a fixed boundary.
            let delay =
                if attempt.pk_index_coordinated && attempt.policy.task_type != TaskType::Full {
                    Duration::from_secs(self.config.trigger_interval_sec)
                } else {
                    Duration::ZERO
                };
            self.state = CompactionTrackState::Idle {
                next_compaction_time: now + delay,
                manual_task_type: None,
            };
        } else {
            self.demand.commit_count = self
                .demand
                .commit_count
                .saturating_sub(attempt.pending_commit_count_at_start);
            self.health
                .record_completion(attempt.gc_watermark_snapshot.clone());
            self.state = CompactionTrackState::Idle {
                next_compaction_time: now + Duration::from_secs(self.config.trigger_interval_sec),
                manual_task_type: None,
            };
        }
        self.end_disabled_round();
        self.finish_action
    }

    fn end_disabled_round(&mut self) {
        if self.config.pk_index_coordinated
            && !self.config.automatic_enabled
            && matches!(self.state, CompactionTrackState::Idle { .. })
        {
            self.round = None;
            self.demand.reconciliation_pending = true;
        }
    }

    /// Completes a sequence-bounded round while preserving commits that arrived
    /// after the last attempt's boundary for subsequent work.
    fn finish_drained(&mut self, now: Instant) -> CompactionTrackFinishAction {
        self.retry.reset();
        let (CompactionTrackState::InFlight { attempt, .. }
        | CompactionTrackState::Applying { attempt, .. }) = &self.state
        else {
            unreachable!("Only an in-flight or applying attempt can finish")
        };
        debug_assert_eq!(attempt.max_file_sequence_number, self.round_boundary());
        debug_assert!(attempt.max_file_sequence_number.is_some());
        let round_start_snapshot = self.health.round_start_snapshot.take();
        self.health.record_completion(round_start_snapshot);
        self.round = None;
        self.state = CompactionTrackState::Idle {
            next_compaction_time: now + Duration::from_secs(self.config.trigger_interval_sec),
            manual_task_type: None,
        };
        self.finish_action
    }

    fn compaction_lag(&self) -> Option<Duration> {
        let latest = self.latest_observed_snapshot.as_ref()?;
        let compacted = self.health.compacted_snapshot.as_ref()?;
        let lag_ms = latest.timestamp_ms.saturating_sub(compacted.timestamp_ms);
        Some(Duration::from_millis(lag_ms.max(0) as u64))
    }

    fn round_boundary(&self) -> Option<i64> {
        self.round
            .as_ref()
            .map(|round| round.max_file_sequence_number)
    }

    fn is_bounded_attempt(&self) -> bool {
        matches!(
            &self.state,
            CompactionTrackState::InFlight { attempt, .. }
                | CompactionTrackState::Applying { attempt, .. }
                if attempt.max_file_sequence_number.is_some()
        )
    }
}

pub(crate) struct IcebergCompactionHandle {
    sink_id: SinkId,
    attempt: Arc<CompactionAttempt>,
    inner: Arc<RwLock<IcebergCompactionManagerInner>>,
    metadata_manager: MetadataManager,
    dispatched: bool,
}

impl IcebergCompactionHandle {
    fn new(
        sink_id: SinkId,
        attempt: Arc<CompactionAttempt>,
        inner: Arc<RwLock<IcebergCompactionManagerInner>>,
        metadata_manager: MetadataManager,
    ) -> Self {
        Self {
            sink_id,
            attempt,
            inner,
            metadata_manager,
            dispatched: false,
        }
    }

    pub async fn send_compact_task(
        mut self,
        compactor: Arc<crate::hummock::IcebergCompactor>,
        task_id: IcebergCompactionTaskId,
    ) -> MetaResult<()> {
        let Some(prost_sink_catalog) = self
            .metadata_manager
            .catalog_controller
            .get_sink_by_id(self.sink_id)
            .await?
        else {
            tracing::warn!(
                iceberg_component = "compaction_scheduler",
                iceberg_operation = "dispatch_task",
                sink_id = %self.sink_id,
                task_id = %task_id,
                "iceberg_compaction_dispatch_sink_not_found",
            );
            return Ok(());
        };
        let sink_catalog = SinkCatalog::from(prost_sink_catalog);
        let mut param = SinkParam::try_from_sink_catalog(sink_catalog)?;
        if self.attempt.pk_index_coordinated && self.attempt.max_file_sequence_number.is_some() {
            param.properties.extend(self.attempt.policy.properties());
        }

        self.try_dispatch_task(
            &compactor,
            IcebergCompactionTask {
                task_id,
                sink_id: self.sink_id.as_raw_id(),
                props: param.properties,
                task_type: self.attempt.policy.task_type as i32,
                pk_index_coordinated: self.attempt.pk_index_coordinated,
                max_file_sequence_number: self.attempt.max_file_sequence_number,
            },
        )
    }

    fn try_dispatch_task(
        &mut self,
        compactor: &crate::hummock::IcebergCompactor,
        task: IcebergCompactionTask,
    ) -> MetaResult<()> {
        use risingwave_pb::iceberg_compaction::subscribe_iceberg_compaction_event_response::Event as IcebergResponseEvent;

        let task_id = task.task_id;
        // Validate and send under the same lock so a cleared schedule cannot
        // dispatch a task after the sink has been removed.
        let mut guard = self.inner.write();
        let Some(track) = guard.sink_schedules.get_mut(&self.sink_id).filter(|track| {
            track.source_is_ready()
                && matches!(
                    &track.state,
                    CompactionTrackState::PendingDispatch { attempt }
                        if Arc::ptr_eq(attempt, &self.attempt)
                )
        }) else {
            tracing::warn!(
                iceberg_component = "compaction_scheduler",
                iceberg_operation = "dispatch_task",
                sink_id = %self.sink_id,
                task_id = %task_id,
                "iceberg_compaction_dispatch_track_not_pending",
            );
            return Ok(());
        };

        let result = compactor.send_event(IcebergResponseEvent::CompactTask(task));
        if result.is_ok() {
            track.mark_dispatched(task_id, compactor.context_id(), Instant::now());
            self.dispatched = true;
        }
        result
    }
}

impl Drop for IcebergCompactionHandle {
    fn drop(&mut self) {
        let waiter = {
            let mut guard = self.inner.write();
            let finish_action = if !self.dispatched
                && let Some(track) = guard.sink_schedules.get_mut(&self.sink_id)
                && matches!(
                    &track.state,
                    CompactionTrackState::PendingDispatch { attempt }
                        if Arc::ptr_eq(attempt, &self.attempt)
                ) {
                Some(track.revert_pre_dispatch_failure(Instant::now()))
            } else {
                None
            };
            if let Some(finish_action) = finish_action {
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
    Maintenance {
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
    /// Refresh policy without counting a commit or replacing the observed head.
    ConfigChange,
}

impl SinkUpdateKind {
    fn apply_to_track(self, track: &mut CompactionTrack, now: Instant) {
        match self {
            SinkUpdateKind::Commit { observed_snapshot } => {
                if track.record_observed_snapshot(observed_snapshot) {
                    track.record_commit_at(now);
                }
            }
            SinkUpdateKind::Maintenance { observed_snapshot } => {
                track.record_observed_snapshot(observed_snapshot);
            }
            SinkUpdateKind::ForceCompaction { observed_snapshot } => {
                track.record_observed_snapshot(observed_snapshot);
                track.record_force_compaction(now, None);
            }
            SinkUpdateKind::ManualForceCompaction { task_type } => {
                track.record_force_compaction(now, Some(task_type))
            }
            SinkUpdateKind::ConfigChange => {}
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
/// `recovery_generation` also rejects updates that cross recovery or registration.
struct PreparedSinkUpdate {
    sink_id: SinkId,
    kind: SinkUpdateKind,
    now: Instant,
    allow_track_initialization: bool,
    recovery_generation: u64,
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
    pub last_success_at: Option<SystemTime>,
    pub last_failure_at: Option<SystemTime>,
    pub last_error: Option<String>,
    pub consecutive_failures: u32,
    /// Commit-time gap between the latest observed snapshot and the snapshot
    /// consumed by the last completed compaction. For copy-on-write sinks, this
    /// is how far `main` lags behind the ingestion branch.
    pub compaction_lag: Option<Duration>,
    pub wait_reason: String,
    pub retry_after_sec: Option<u64>,
    pub blocked_age_sec: Option<u64>,
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
        let config = self.resolve_schedule_config(iceberg_config, now);
        debug_assert_eq!(
            track.config.write_mode, config.write_mode,
            "Iceberg write mode cannot change while a schedule track exists"
        );
        debug_assert_eq!(
            track.config.pk_index_coordinated,
            config.pk_index_coordinated
        );
        if track.config.pk_index_coordinated
            && track.latest_observed_snapshot.is_some()
            && (track.config.policy != config.policy
                || (!track.config.automatic_enabled && config.automatic_enabled))
        {
            track.demand.reconciliation_pending = true;
        }
        track.update_interval(config.trigger_interval_sec, now);
        track.config = config;
        track.end_disabled_round();
    }

    /// Schedules existing Iceberg sinks once after meta starts. See
    /// `iceberg_compaction_schedule_on_startup`.
    pub fn schedule_on_startup_task(manager: Arc<Self>) -> (JoinHandle<()>, oneshot::Sender<()>) {
        let (shutdown_tx, shutdown_rx) = oneshot::channel();
        let join_handle = tokio::spawn(async move {
            tokio::select! {
                _ = manager.schedule_existing_sinks() => {}
                _ = shutdown_rx => {}
            }
        });
        (join_handle, shutdown_tx)
    }

    async fn schedule_existing_sinks(&self) {
        let sinks = match self.metadata_manager.catalog_controller.list_sinks().await {
            Ok(sinks) => sinks,
            Err(e) => {
                tracing::warn!(
                    error = %e.as_report(),
                    "failed to list sinks to schedule iceberg compaction on startup"
                );
                return;
            }
        };
        let sink_ids = sinks
            .into_iter()
            .filter(|sink| {
                sink.stream_job_status() == PbStreamJobStatus::Created
                    && is_iceberg_sink(&sink.properties)
            })
            .map(|sink| sink.id);
        futures::stream::iter(sink_ids)
            .for_each_concurrent(16, |sink_id| async move {
                if let Err(e) = self.schedule_sink_on_startup(sink_id).await {
                    tracing::warn!(
                        %sink_id,
                        error = %e.as_report(),
                        "failed to schedule iceberg compaction on startup"
                    );
                }
            })
            .await;
    }

    /// Records the head of the commit branch as one sink commit, so the sink is compacted after
    /// the compaction interval. The compaction commits nothing if the table is already clean.
    async fn schedule_sink_on_startup(&self, sink_id: SinkId) -> MetaResult<()> {
        // A commit after the restart has already scheduled the sink.
        if self.inner.read().sink_schedules.contains_key(&sink_id) {
            return Ok(());
        }
        let config = self.load_iceberg_config(sink_id).await?;
        // PK-index scheduling is initialized only after its coordinator has settled recovery.
        if !config.enable_compaction || config.enable_pk_index {
            return Ok(());
        }
        let table = config.load_table().await?;
        let branch = commit_branch(config.r#type.as_str(), config.write_mode);
        // A branch without a snapshot has nothing to compact.
        let Some(observed_snapshot) =
            IcebergCommittedSnapshot::from_branch_head(table.metadata(), &branch)
        else {
            return Ok(());
        };
        let prepared_update = PreparedSinkUpdate {
            sink_id,
            kind: SinkUpdateKind::Commit { observed_snapshot },
            now: Instant::now(),
            allow_track_initialization: true,
            recovery_generation: 0,
            loaded_config: Some(config),
        };
        let mut guard = self.inner.write();
        // A commit during the loads above may have recorded a newer snapshot. Do not overwrite
        // it with the head read here.
        if !guard.sink_schedules.contains_key(&sink_id) {
            self.apply_sink_update(&mut guard, prepared_update);
        }
        Ok(())
    }

    pub async fn update_iceberg_commit_info(&self, msg: IcebergSinkCompactionUpdate) {
        let IcebergSinkCompactionUpdate {
            sink_id,
            force_compaction,
            has_new_data,
            observed_snapshot,
        } = msg;
        let kind = if force_compaction {
            SinkUpdateKind::ForceCompaction { observed_snapshot }
        } else if has_new_data {
            SinkUpdateKind::Commit { observed_snapshot }
        } else {
            SinkUpdateKind::Maintenance { observed_snapshot }
        };
        let prepared_update = self
            .prepare_sink_update(sink_id, kind, Instant::now())
            .await;

        let mut guard = self.inner.write();
        self.apply_sink_update(&mut guard, prepared_update);
    }

    /// Apply maintenance-only ALTER options to a registered PK-index sink, even when
    /// ingestion is idle. Readiness publication remains the only initialization path.
    pub fn on_sink_config_changed(&self, sink_id: SinkId, config: IcebergConfig) {
        debug_assert!(config.enable_pk_index);
        let mut guard = self.inner.write();
        if guard.is_recovering(sink_id) || !guard.sink_schedules.contains_key(&sink_id) {
            return;
        }
        let recovery_generation = guard.recovery_generation(sink_id);
        self.apply_sink_update(
            &mut guard,
            PreparedSinkUpdate {
                sink_id,
                kind: SinkUpdateKind::ConfigChange,
                now: Instant::now(),
                allow_track_initialization: false,
                recovery_generation,
                loaded_config: Some(config),
            },
        );
    }

    /// Discard PK-index scheduling before replacing coordinators. Persisted sink state, not the
    /// old attempt, determines which rewrites recovery commits.
    pub fn on_recovery_start(&self, sink_ids: Option<&[SinkId]>) {
        let mut guard = self.inner.write();
        let mut affected = guard
            .sink_schedules
            .iter()
            .filter(|(_, track)| track.config.pk_index_coordinated)
            .map(|(&sink_id, _)| sink_id)
            .chain(
                guard
                    .sink_recovery
                    .iter()
                    .filter(|(_, state)| state.recovering)
                    .map(|(&sink_id, _)| sink_id),
            )
            .filter(|sink_id| sink_ids.is_none_or(|ids| ids.contains(sink_id)))
            .collect::<Vec<_>>();
        affected.sort();
        affected.dedup();
        let mut cancel = Vec::new();
        for sink_id in affected {
            let generation = guard.next_recovery_generation();
            let state = guard.sink_recovery.entry(sink_id).or_default();
            state.generation = generation;
            state.recovering = true;
            if let Some(task) = Self::remove_sink_schedule(&mut guard, sink_id) {
                cancel.push((sink_id, task));
            }
            if let Some(waiter) = guard.manual_compaction_waiters.remove(&sink_id) {
                let _ = waiter.send(Err(anyhow!(
                    "manual compaction interrupted by recovery for sink {}",
                    sink_id
                )
                .into()));
            }
        }
        drop(guard);
        for (id, task) in cancel {
            self.cancel_scheduled_task_if_any(id, Some(task));
        }
    }

    /// Publish only after CREATE post-collect or successful database recovery unblocks its queue.
    /// The coordinator registry serializes publication with retirement. Keeping its registration
    /// also prevents updates/dispatch from using a retired instance before schedule cleanup.
    pub(crate) fn on_sink_ready(
        &self,
        sink_id: SinkId,
        registration: Arc<IcebergPkIndexSinkRegistration>,
        observed_snapshot: Option<IcebergCommittedSnapshot>,
    ) -> bool {
        let mut guard = self.inner.write();
        if !registration.is_ready() {
            return false;
        }
        let config = registration.config();
        debug_assert!(config.enable_pk_index);
        let now = Instant::now();
        let mut track = self.create_compaction_track(&config, now);
        // Reconcile existing data, but let an empty table's future ingestion follow
        // the normal commit threshold and interval.
        if let Some(snapshot) = observed_snapshot {
            track.record_observed_snapshot(snapshot);
            track.demand.reconciliation_pending = true;
        }
        track.registration = Some(registration);
        track.state = CompactionTrackState::Idle {
            next_compaction_time: now,
            manual_task_type: None,
        };
        let generation = guard.next_recovery_generation();
        let state = guard.sink_recovery.entry(sink_id).or_default();
        state.generation = generation;
        state.recovering = false;
        if config.enable_snapshot_expiration {
            guard.snapshot_expiration_sink_ids.insert(sink_id);
        } else {
            guard.snapshot_expiration_sink_ids.remove(&sink_id);
        }
        if config.enable_manifest_rewrite {
            guard.manifest_rewrite_sink_ids.insert(sink_id);
        } else {
            guard.manifest_rewrite_sink_ids.remove(&sink_id);
        }
        if config.enable_orphan_file_cleanup {
            guard.orphan_file_cleanup_sink_ids.insert(sink_id);
        } else {
            guard.orphan_file_cleanup_sink_ids.remove(&sink_id);
        }
        let task = Self::remove_sink_schedule(&mut guard, sink_id);
        guard.sink_schedules.insert(sink_id, track);
        if let Some(waiter) = guard.manual_compaction_waiters.remove(&sink_id) {
            let _ = waiter.send(Err(anyhow!(
                "manual compaction interrupted by coordinator registration for sink {}",
                sink_id
            )
            .into()));
        }
        drop(guard);
        self.cancel_scheduled_task_if_any(sink_id, task);
        true
    }

    async fn prepare_sink_update(
        &self,
        sink_id: SinkId,
        kind: SinkUpdateKind,
        now: Instant,
    ) -> PreparedSinkUpdate {
        let refresh_interval = self.config_refresh_interval();
        let (allow_track_initialization, should_refresh_config, recovery_generation) = {
            let guard = self.inner.read();
            let (allow_track_initialization, should_refresh_config) =
                match guard.sink_schedules.get(&sink_id) {
                    Some(track) => (false, track.needs_config_refresh(now, refresh_interval)),
                    None => (true, true),
                };
            (
                allow_track_initialization,
                should_refresh_config,
                guard.recovery_generation(sink_id),
            )
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
            recovery_generation,
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
            recovery_generation,
            loaded_config,
        } = prepared_update;
        if guard.is_recovering(sink_id) || guard.recovery_generation(sink_id) != recovery_generation
        {
            return false;
        }
        // A config load started before ALTER must not overwrite the newer policy.
        // Its commit observation is still applied below.
        let loaded_config = loaded_config.filter(|_| {
            guard
                .sink_schedules
                .get(&sink_id)
                .is_none_or(|track| track.config.refreshed_at <= now)
        });
        // Commit/manual/maintenance updates must never create PK-index scheduling or enable
        // metadata maintenance before the authoritative coordinator registration is ready.
        let track = guard.sink_schedules.get(&sink_id);
        if track.is_some_and(|track| !track.source_is_ready())
            || (loaded_config
                .as_ref()
                .is_some_and(|config| config.enable_pk_index)
                && !track.is_some_and(|track| {
                    track.config.pk_index_coordinated && track.source_is_ready()
                }))
        {
            return false;
        }
        let refresh_interval = self.config_refresh_interval();

        if let Some(config) = loaded_config.as_ref() {
            if config.enable_snapshot_expiration {
                guard.snapshot_expiration_sink_ids.insert(sink_id);
            } else {
                guard.snapshot_expiration_sink_ids.remove(&sink_id);
            }
            if config.enable_manifest_rewrite {
                guard.manifest_rewrite_sink_ids.insert(sink_id);
            } else {
                guard.manifest_rewrite_sink_ids.remove(&sink_id);
            }
            if config.enable_orphan_file_cleanup {
                guard.orphan_file_cleanup_sink_ids.insert(sink_id);
            } else {
                guard.orphan_file_cleanup_sink_ids.remove(&sink_id);
            }

            if !config.enable_compaction
                && !config.enable_pk_index
                && !kind.allows_disabled_compaction()
            {
                let keep_until_task_finishes =
                    guard.sink_schedules.get_mut(&sink_id).is_some_and(|track| {
                        let keep = track.has_queued_task_or_active_attempt();
                        if keep {
                            // Preserve the selected attempt, including its round boundary, so
                            // its report is interpreted consistently. The disabled track is
                            // removed as soon as that one-shot task finishes.
                            track.finish_action = CompactionTrackFinishAction::RemoveTrack;
                        }
                        keep
                    });
                if !keep_until_task_finishes {
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
                if (track.config.pk_index_coordinated
                    || track.needs_config_refresh(now, refresh_interval))
                    && let Some(config) = loaded_config.as_ref()
                {
                    self.refresh_schedule_config(track, config, now);
                }
                if let Some(config) = loaded_config.as_ref() {
                    track.finish_action = if kind.allows_disabled_compaction()
                        && !config.enable_compaction
                        && !config.enable_pk_index
                    {
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
                        iceberg_component = "compaction_scheduler",
                        iceberg_operation = "apply_sink_update",
                        sink_id = %sink_id,
                        "iceberg_compaction_update_ignored_track_missing",
                    );
                    return false;
                }

                let Some(config) = loaded_config.as_ref() else {
                    tracing::warn!(
                        iceberg_component = "compaction_scheduler",
                        iceberg_operation = "apply_sink_update",
                        sink_id = %sink_id,
                        "iceberg_compaction_update_ignored_config_unavailable",
                    );
                    return false;
                };

                let track = entry.insert(self.create_compaction_track(config, now));
                track.finish_action = if kind.allows_disabled_compaction()
                    && !config.enable_compaction
                    && !config.enable_pk_index
                {
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
        CompactionTrack::new(self.resolve_schedule_config(iceberg_config, now), now)
    }

    fn resolve_schedule_config(
        &self,
        iceberg_config: &IcebergConfig,
        now: Instant,
    ) -> CompactionScheduleConfig {
        // COW compaction type is an internal policy. Ignore the legacy persisted value so that
        // both existing and new COW sinks follow the license-based default.
        let configured_type = match iceberg_config.write_mode {
            IcebergWriteMode::CopyOnWrite => None,
            IcebergWriteMode::MergeOnRead => iceberg_config.compaction_type,
        };
        let compaction_type = match configured_type {
            Some(compaction_type) => compaction_type,
            None if Feature::IcebergCompaction.check_available().is_ok() => CompactionType::Auto,
            None => CompactionType::Full,
        };

        CompactionScheduleConfig {
            policy: CompactionPolicy {
                task_type: match compaction_type {
                    CompactionType::Auto => TaskType::Auto,
                    CompactionType::Full => TaskType::Full,
                    CompactionType::SmallFiles => TaskType::SmallFiles,
                    CompactionType::FilesWithDelete => TaskType::FilesWithDelete,
                },
                small_files_threshold_mb: iceberg_config.small_files_threshold_mb(),
                target_file_size_mb: iceberg_config.target_file_size_mb(),
                delete_ratio_percent: iceberg_config.delete_ratio_percent(),
            },
            write_mode: iceberg_config.write_mode,
            pk_index_coordinated: iceberg_config.enable_pk_index,
            automatic_enabled: iceberg_config.enable_compaction,
            trigger_interval_sec: iceberg_config.compaction_interval_sec(),
            trigger_snapshot_count: iceberg_config.trigger_snapshot_count(),
            report_timeout: self.report_timeout(),
            refreshed_at: now,
        }
    }

    pub(super) async fn start_manual_compaction(
        &self,
        sink_id: SinkId,
    ) -> MetaResult<oneshot::Receiver<MetaResult<IcebergCompactionTaskId>>> {
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

        if guard.is_recovering(sink_id) {
            return Err(anyhow!(
                "manual compaction is rejected during recovery for sink {}",
                sink_id
            )
            .into());
        }
        if let Some(track) = guard.sink_schedules.get(&sink_id) {
            if track.round.is_some() {
                return Err(anyhow!(
                    "manual Full compaction is rejected while an automatic round is active for sink {}",
                    sink_id
                )
                .into());
            }
            match &track.state {
                CompactionTrackState::PendingDispatch { attempt } => {
                    return Err(anyhow!(
                        "iceberg compaction task is already running for sink {} \
                         (state=pending_dispatch, pending_commit_count_at_start={}, \
                         pending_commit_count={})",
                        sink_id,
                        attempt.pending_commit_count_at_start,
                        track.demand.commit_count
                    )
                    .into());
                }
                CompactionTrackState::InFlight {
                    task_id,
                    attempt,
                    report_deadline,
                    ..
                } => {
                    return Err(anyhow!(
                        "iceberg compaction task is already running for sink {} \
                         (state=in_flight, task_id={}, pending_commit_count_at_start={}, \
                         pending_commit_count={}, report_timeout_after_sec={})",
                        sink_id,
                        task_id,
                        attempt.pending_commit_count_at_start,
                        track.demand.commit_count,
                        report_deadline.saturating_duration_since(now).as_secs()
                    )
                    .into());
                }
                CompactionTrackState::Idle { .. } => {}
                CompactionTrackState::Applying { task_id, .. }
                | CompactionTrackState::NeedsRecovery { task_id, .. } => {
                    return Err(anyhow!(
                        "iceberg compaction task {} is already applying for sink {}",
                        task_id,
                        sink_id
                    )
                    .into());
                }
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

    #[cfg(test)]
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
                tracing::warn!(
                    iceberg_component = "compaction_scheduler",
                    iceberg_operation = "report_timeout",
                    sink_id = %sink_id,
                    "iceberg_compaction_task_report_timed_out",
                );
                let error = format!(
                    "compaction task report timed out after {}s",
                    track.config.report_timeout.as_secs()
                );
                timed_out_tasks.push((sink_id, track.finish_failed(now, error)));
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
                if !guard.is_recovering(*sink_id)
                    && track.source_is_ready()
                    && track.should_trigger(now)
                    && let CompactionTrackState::Idle {
                        next_compaction_time,
                        ..
                    } = &track.state
                {
                    candidates.push((*sink_id, *next_compaction_time));
                }
            }

            candidates.sort_by_key(|c| (c.1, c.0));
            let handles = candidates
                .into_iter()
                .take(n)
                .filter_map(|(sink_id, _)| {
                    let track = guard.sink_schedules.get_mut(&sink_id)?;
                    let attempt = track.start_attempt();

                    Some(IcebergCompactionHandle::new(
                        sink_id,
                        attempt,
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
            guard.sink_recovery.remove(&sink_id);
            guard.snapshot_expiration_sink_ids.remove(&sink_id);
            guard.manifest_rewrite_sink_ids.remove(&sink_id);
            guard.orphan_file_cleanup_sink_ids.remove(&sink_id);
            let waiter = guard.manual_compaction_waiters.remove(&sink_id);
            (task_to_cancel, waiter)
        };
        self.orphan_file_cleanup_locks.lock().remove(&sink_id);
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
                task_id = %task_id,
                compactor_context_id = %compactor_context_id,
                "Unable to cancel iceberg compaction task because compactor is no longer registered",
            );
            return;
        };

        tracing::info!(
            sink_id = %sink_id,
            task_id = %task_id,
            compactor_context_id = %compactor_context_id,
            "Cancelling iceberg compaction task for removed schedule",
        );

        if let Err(e) = compactor.cancel_task(task_id) {
            tracing::warn!(
                error = %e.as_report(),
                sink_id = %sink_id,
                task_id = %task_id,
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
                    | CompactionTrackState::InFlight { .. }
                    | CompactionTrackState::Applying { .. }
                    | CompactionTrackState::NeedsRecovery { .. } => None,
                };
                let is_triggerable = track.should_trigger(now);
                let compaction_lag = track.compaction_lag();

                IcebergCompactionScheduleStatus {
                    sink_id,
                    task_type: track
                        .effective_task_type()
                        .as_str_name()
                        .to_ascii_lowercase(),
                    trigger_interval_sec: track.config.trigger_interval_sec,
                    trigger_snapshot_count: track.config.trigger_snapshot_count,
                    schedule_state: match track.state {
                        CompactionTrackState::Idle { .. } => "idle".to_owned(),
                        CompactionTrackState::PendingDispatch { .. }
                        | CompactionTrackState::InFlight { .. } => "processing".to_owned(),
                        CompactionTrackState::Applying { .. } => "applying".to_owned(),
                        CompactionTrackState::NeedsRecovery { .. } => "needs_recovery".to_owned(),
                    },
                    next_compaction_after_sec,
                    pending_snapshot_count: Some(track.demand.commit_count),
                    is_triggerable,
                    last_success_at: track.health.last_success_at,
                    last_failure_at: track.health.last_failure_at,
                    last_error: track.health.last_error,
                    consecutive_failures: track.health.consecutive_failures,
                    compaction_lag,
                    wait_reason: if matches!(
                        track.state,
                        CompactionTrackState::NeedsRecovery { .. }
                    ) {
                        "unknown_application_outcome"
                    } else if let Some(blocked) = &track.retry.blocked {
                        match blocked.reason {
                            IcebergReportTaskStatus::BlockedCapacity => "worker_capacity",
                            _ => "statistics",
                        }
                    } else if track.retry.is_waiting(now) {
                        "retry_backoff"
                    } else if !track.config.automatic_enabled {
                        "automatic_disabled"
                    } else {
                        "none"
                    }
                    .to_owned(),
                    retry_after_sec: track
                        .retry
                        .not_before
                        .map(|deadline| deadline.saturating_duration_since(now).as_secs()),
                    blocked_age_sec: track
                        .retry
                        .blocked
                        .as_ref()
                        .map(|blocked| now.saturating_duration_since(blocked.since).as_secs()),
                }
            })
            .collect_vec();

        statuses.sort_by_key(|status| status.sink_id);
        statuses
    }

    fn finish_report_task(&self, report: IcebergReportTask, applying: bool) {
        let sink_id = report.sink_id;
        let task_id = report.task_id;
        let status = IcebergReportTaskStatus::try_from(report.status)
            .unwrap_or(IcebergReportTaskStatus::Unspecified);
        let now = Instant::now();

        let waiter = {
            let mut guard = self.inner.write();
            let mut waiter = None;

            match guard.sink_schedules.get_mut(&sink_id) {
                Some(track)
                    if if applying {
                        matches!(
                            &track.state,
                            CompactionTrackState::Applying { task_id: current_task_id, .. }
                                if *current_task_id == task_id
                        )
                    } else {
                        track.is_in_flight_task(task_id)
                    } =>
                {
                    let finish_action = match status {
                        IcebergReportTaskStatus::Success => track.finish_success(now),
                        IcebergReportTaskStatus::Drained if track.is_bounded_attempt() => {
                            track.finish_drained(now)
                        }
                        IcebergReportTaskStatus::BlockedCapacity
                        | IcebergReportTaskStatus::Busy
                        | IcebergReportTaskStatus::BlockedStatistics
                            if track.config.pk_index_coordinated =>
                        {
                            track.finish_blocked(status, now)
                        }
                        IcebergReportTaskStatus::BlockedCapacity
                        | IcebergReportTaskStatus::Busy
                        | IcebergReportTaskStatus::BlockedStatistics
                        | IcebergReportTaskStatus::Drained
                        | IcebergReportTaskStatus::Failed
                        | IcebergReportTaskStatus::Unspecified => {
                            tracing::warn!(
                                iceberg_component = "compaction_scheduler",
                                iceberg_operation = "handle_report",
                                sink_id = %sink_id,
                                task_id = %task_id,
                                status = ?status,
                                error_message = report.error_message.as_deref().unwrap_or_default(),
                                "iceberg_compaction_task_reported_failure",
                            );
                            let error = report.error_message.clone().unwrap_or_else(|| {
                                format!("compaction task reported {}", status.as_str_name())
                            });
                            track.finish_failed(now, error)
                        }
                    };

                    Self::apply_track_finish_action(&mut guard, sink_id, finish_action);
                    waiter = guard.manual_compaction_waiters.remove(&sink_id);
                }
                Some(_) => {
                    tracing::warn!(
                        iceberg_component = "compaction_scheduler",
                        iceberg_operation = "handle_report",
                        sink_id = %sink_id,
                        task_id = %task_id,
                        status = ?status,
                        "iceberg_compaction_report_ignored_stale",
                    );
                }
                None => {
                    tracing::warn!(
                        iceberg_component = "compaction_scheduler",
                        iceberg_operation = "handle_report",
                        sink_id = %sink_id,
                        task_id = %task_id,
                        status = ?status,
                        "iceberg_compaction_report_unknown_sink",
                    );
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
