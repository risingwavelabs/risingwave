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

use std::collections::{HashMap, HashSet};
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
    CompactionType, IcebergConfig, get_current_snapshot_id, get_pending_snapshot_count_from_table,
    should_enable_iceberg_cow,
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

const ICEBERG_COMPACTION_REPORT_TIMEOUT: Duration = Duration::from_secs(30 * 60);
const ICEBERG_COMPACTION_RECONCILE_TTL: Duration = Duration::from_secs(10 * 60);

#[derive(Debug, Clone, Copy)]
struct PendingSnapshotState {
    pending_snapshot_count: usize,
    current_snapshot_id: Option<i64>,
}

/// Snapshot for restoring track state on failure
#[derive(Debug, Clone)]
struct CompactionTrackSnapshot {
    next_compaction_time: Instant,
    pending_commit_count: usize,
    last_observed_pending_snapshot_count: Option<usize>,
    last_snapshot_id: Option<i64>,
    last_reconciled_at: Option<Instant>,
    awaiting_timeout_reconcile: bool,
    force_trigger_pending: bool,
    needs_reconcile: bool,
}

/// Compaction track states using type-safe state machine pattern
#[derive(Debug, Clone)]
enum CompactionTrackState {
    /// Ready to accept commits and check for trigger conditions
    Idle { next_compaction_time: Instant },
    /// Compaction task is being processed. `report_deadline` acts as a lease;
    /// if it expires before a report arrives, the task becomes retryable.
    Processing {
        task_id: Option<u64>,
        pending_commit_count_at_dispatch: usize,
        report_deadline: Instant,
    },
}

#[derive(Debug, Clone)]
struct CompactionTrack {
    task_type: TaskType,
    trigger_interval_sec: u64,
    /// Minimum snapshot count threshold to trigger compaction (early trigger).
    /// Compaction triggers when `pending_commit_count` >= this threshold, even before interval expires.
    trigger_snapshot_count: usize,
    pending_commit_count: usize,
    last_observed_pending_snapshot_count: Option<usize>,
    last_snapshot_id: Option<i64>,
    last_reconciled_at: Option<Instant>,
    awaiting_timeout_reconcile: bool,
    force_trigger_pending: bool,
    needs_reconcile: bool,
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
    fn should_trigger(&self, now: Instant) -> bool {
        // Only Idle state can trigger.
        let next_compaction_time = match &self.state {
            CompactionTrackState::Idle {
                next_compaction_time,
            } => *next_compaction_time,
            CompactionTrackState::Processing { .. } => return false,
        };

        if self.awaiting_timeout_reconcile {
            return false;
        }

        if self.force_trigger_pending {
            return true;
        }

        // Check conditions
        let time_ready = now >= next_compaction_time;
        let snapshot_ready = self.pending_commit_count >= self.trigger_snapshot_count;
        let has_snapshots = self.pending_commit_count > 0;

        // OR logic: snapshot threshold triggers early,
        // time trigger requires at least 1 snapshot to avoid empty compaction
        snapshot_ready || (time_ready && has_snapshots)
    }

    fn record_commit(&mut self, commit_count_delta: usize) {
        self.pending_commit_count = self.pending_commit_count.saturating_add(commit_count_delta);
        if commit_count_delta > 0 {
            self.last_observed_pending_snapshot_count = None;
        }
    }

    fn record_force_compaction(&mut self, now: Instant) {
        if let CompactionTrackState::Idle {
            next_compaction_time,
        } = &mut self.state
        {
            *next_compaction_time = now;
            self.pending_commit_count = self.pending_commit_count.max(1);
            self.force_trigger_pending = true;
            self.last_observed_pending_snapshot_count = None;
        }
    }

    fn should_reconcile(&self, now: Instant) -> bool {
        if self.needs_reconcile {
            return true;
        }
        self.last_reconciled_at.is_some_and(|last| {
            now.saturating_duration_since(last) >= ICEBERG_COMPACTION_RECONCILE_TTL
        })
    }

    /// Create snapshot and transition to Processing state
    fn start_processing(&mut self) -> CompactionTrackSnapshot {
        match &self.state {
            CompactionTrackState::Idle {
                next_compaction_time,
            } => {
                let snapshot = CompactionTrackSnapshot {
                    next_compaction_time: *next_compaction_time,
                    pending_commit_count: self.pending_commit_count,
                    last_observed_pending_snapshot_count: self.last_observed_pending_snapshot_count,
                    last_snapshot_id: self.last_snapshot_id,
                    last_reconciled_at: self.last_reconciled_at,
                    awaiting_timeout_reconcile: self.awaiting_timeout_reconcile,
                    force_trigger_pending: self.force_trigger_pending,
                    needs_reconcile: self.needs_reconcile,
                };
                self.state = CompactionTrackState::Processing {
                    task_id: None,
                    pending_commit_count_at_dispatch: self.pending_commit_count,
                    report_deadline: Instant::now() + ICEBERG_COMPACTION_REPORT_TIMEOUT,
                };
                snapshot
            }
            CompactionTrackState::Processing { .. } => {
                unreachable!("Cannot start processing when already processing")
            }
        }
    }

    fn mark_dispatched(&mut self, task_id: u64, now: Instant) {
        match &mut self.state {
            CompactionTrackState::Processing {
                task_id: current_task_id,
                pending_commit_count_at_dispatch: _,
                report_deadline,
            } => {
                *current_task_id = Some(task_id);
                *report_deadline = now + ICEBERG_COMPACTION_REPORT_TIMEOUT;
            }
            CompactionTrackState::Idle { .. } => unreachable!("Cannot mark dispatched when idle"),
        }
    }

    fn is_processing_task(&self, task_id: u64) -> bool {
        matches!(
            &self.state,
            CompactionTrackState::Processing {
                task_id: Some(current_task_id),
                pending_commit_count_at_dispatch: _,
                ..
            } if *current_task_id == task_id
        )
    }

    fn is_report_timed_out(&self, now: Instant) -> bool {
        matches!(
            &self.state,
            CompactionTrackState::Processing {
                pending_commit_count_at_dispatch: _,
                report_deadline,
                ..
            } if now >= *report_deadline
        )
    }

    fn finish_success(&mut self, now: Instant, snapshot_state: PendingSnapshotState) {
        match &self.state {
            CompactionTrackState::Processing {
                pending_commit_count_at_dispatch,
                ..
            } => {
                self.pending_commit_count = self
                    .pending_commit_count
                    .saturating_sub(*pending_commit_count_at_dispatch);
                self.last_observed_pending_snapshot_count =
                    Some(snapshot_state.pending_snapshot_count);
                self.last_snapshot_id = snapshot_state.current_snapshot_id;
                self.last_reconciled_at = Some(now);
                self.awaiting_timeout_reconcile = false;
                self.force_trigger_pending = false;
                self.needs_reconcile = false;
                self.state = CompactionTrackState::Idle {
                    next_compaction_time: now + Duration::from_secs(self.trigger_interval_sec),
                };
            }
            CompactionTrackState::Idle { .. } => unreachable!("Cannot finish success when idle"),
        }
    }

    fn finish_failed(&mut self, now: Instant) {
        match &self.state {
            CompactionTrackState::Processing { .. } => {
                self.awaiting_timeout_reconcile = false;
                self.needs_reconcile = true;
                self.state = CompactionTrackState::Idle {
                    next_compaction_time: now,
                };
            }
            CompactionTrackState::Idle { .. } => unreachable!("Cannot finish failed when idle"),
        }
    }

    /// Restore from snapshot on failure
    fn restore_from_snapshot(&mut self, snapshot: CompactionTrackSnapshot) {
        self.pending_commit_count = snapshot.pending_commit_count;
        self.last_observed_pending_snapshot_count = snapshot.last_observed_pending_snapshot_count;
        self.last_snapshot_id = snapshot.last_snapshot_id;
        self.last_reconciled_at = snapshot.last_reconciled_at;
        self.awaiting_timeout_reconcile = snapshot.awaiting_timeout_reconcile;
        self.force_trigger_pending = snapshot.force_trigger_pending;
        self.needs_reconcile = snapshot.needs_reconcile;
        self.state = CompactionTrackState::Idle {
            next_compaction_time: snapshot.next_compaction_time,
        };
    }

    fn apply_idle_reconcile_result(&mut self, now: Instant, snapshot_state: PendingSnapshotState) {
        let was_awaiting_timeout_reconcile = self.awaiting_timeout_reconcile;
        self.pending_commit_count = snapshot_state.pending_snapshot_count;
        self.last_observed_pending_snapshot_count = Some(snapshot_state.pending_snapshot_count);
        self.last_snapshot_id = snapshot_state.current_snapshot_id;
        self.last_reconciled_at = Some(now);
        self.awaiting_timeout_reconcile = false;
        if was_awaiting_timeout_reconcile && snapshot_state.pending_snapshot_count == 0 {
            self.force_trigger_pending = false;
        }
        self.needs_reconcile = false;
    }

    fn observe_snapshot_state(&mut self, now: Instant, snapshot_state: PendingSnapshotState) {
        self.last_observed_pending_snapshot_count = Some(snapshot_state.pending_snapshot_count);
        self.last_snapshot_id = snapshot_state.current_snapshot_id;
        self.last_reconciled_at = Some(now);
    }

    fn expire_processing_lease(&mut self, now: Instant) {
        match &self.state {
            CompactionTrackState::Processing { .. } => {
                self.awaiting_timeout_reconcile = true;
                self.needs_reconcile = true;
                self.state = CompactionTrackState::Idle {
                    next_compaction_time: now,
                };
            }
            CompactionTrackState::Idle { .. } => {
                unreachable!("Cannot expire processing lease when idle")
            }
        }
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
            CompactionTrackState::Processing { .. } => {
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
            if let Some(track) = guard.sink_schedules.get_mut(&self.sink_id)
                && track.task_type == self.task_type
            {
                track.mark_dispatched(task_id, Instant::now());
            }
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
                if !self.handle_success {
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
            commit_count_delta,
            force_compaction,
        } = msg;

        // Check if track exists
        let track_exists = {
            let guard = self.inner.read();
            guard.sink_schedules.contains_key(&sink_id)
        };
        let mut seeded_from_catalog = false;

        // Create track if it doesn't exist
        if !track_exists {
            // Load config first (async operation outside of lock)
            let iceberg_config = self.load_iceberg_config(sink_id).await;
            let initial_snapshot_state = self.get_pending_snapshot_state(sink_id).await;
            seeded_from_catalog = initial_snapshot_state.is_some();

            let new_track = match iceberg_config {
                Ok(config) => {
                    // Call synchronous create function with the config
                    match self.create_compaction_track(
                        sink_id,
                        &config,
                        initial_snapshot_state.unwrap_or(PendingSnapshotState {
                            pending_snapshot_count: commit_count_delta
                                .max(force_compaction as usize),
                            current_snapshot_id: None,
                        }),
                    ) {
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
                                pending_commit_count: initial_snapshot_state
                                    .map(|state| state.pending_snapshot_count)
                                    .unwrap_or(commit_count_delta.max(force_compaction as usize)),
                                last_observed_pending_snapshot_count: initial_snapshot_state
                                    .map(|state| state.pending_snapshot_count),
                                last_snapshot_id: initial_snapshot_state
                                    .and_then(|state| state.current_snapshot_id),
                                last_reconciled_at: initial_snapshot_state.map(|_| Instant::now()),
                                awaiting_timeout_reconcile: false,
                                force_trigger_pending: false,
                                needs_reconcile: initial_snapshot_state.is_none(),
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
                        pending_commit_count: initial_snapshot_state
                            .map(|state| state.pending_snapshot_count)
                            .unwrap_or(commit_count_delta.max(force_compaction as usize)),
                        last_observed_pending_snapshot_count: initial_snapshot_state
                            .map(|state| state.pending_snapshot_count),
                        last_snapshot_id: initial_snapshot_state
                            .and_then(|state| state.current_snapshot_id),
                        last_reconciled_at: initial_snapshot_state.map(|_| Instant::now()),
                        awaiting_timeout_reconcile: false,
                        force_trigger_pending: false,
                        needs_reconcile: initial_snapshot_state.is_none(),
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
            if !seeded_from_catalog {
                track.record_commit(commit_count_delta);
            }
            // Force compaction: trigger immediately by setting next_compaction_time to now
            if force_compaction {
                track.record_force_compaction(Instant::now());
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
        initial_snapshot_state: PendingSnapshotState,
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
            pending_commit_count: initial_snapshot_state.pending_snapshot_count,
            last_observed_pending_snapshot_count: Some(
                initial_snapshot_state.pending_snapshot_count,
            ),
            last_snapshot_id: initial_snapshot_state.current_snapshot_id,
            last_reconciled_at: Some(Instant::now()),
            awaiting_timeout_reconcile: false,
            force_trigger_pending: false,
            needs_reconcile: false,
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

        let timed_out_sink_ids = {
            let mut guard = self.inner.write();
            let mut timed_out_sink_ids = HashSet::new();
            for (&sink_id, track) in &mut guard.sink_schedules {
                if track.is_report_timed_out(now) {
                    tracing::warn!(sink_id = %sink_id, "Iceberg compaction task report timed out");
                    track.expire_processing_lease(now);
                    timed_out_sink_ids.insert(sink_id);
                }
            }
            timed_out_sink_ids
        };

        let sinks_to_reconcile = {
            let guard = self.inner.read();
            guard
                .sink_schedules
                .iter()
                .filter_map(|(&sink_id, track)| {
                    if timed_out_sink_ids.contains(&sink_id) {
                        return None;
                    }
                    match &track.state {
                        CompactionTrackState::Idle { .. } if track.should_reconcile(now) => {
                            Some(sink_id)
                        }
                        _ => None,
                    }
                })
                .collect_vec()
        };

        let reconciled_states = futures::future::join_all(
            sinks_to_reconcile
                .iter()
                .map(|sink_id| self.get_pending_snapshot_state(*sink_id)),
        )
        .await;

        let mut guard = self.inner.write();

        for (sink_id, snapshot_state) in sinks_to_reconcile.into_iter().zip(reconciled_states) {
            if let Some(track) = guard.sink_schedules.get_mut(&sink_id)
                && let Some(snapshot_state) = snapshot_state
            {
                match &track.state {
                    CompactionTrackState::Idle { .. } => {
                        track.apply_idle_reconcile_result(now, snapshot_state);
                    }
                    CompactionTrackState::Processing { .. } => {
                        track.observe_snapshot_state(now, snapshot_state);
                    }
                }
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

    pub async fn list_compaction_statuses(&self) -> Vec<IcebergCompactionScheduleStatus> {
        let now = Instant::now();
        let schedules = {
            let guard = self.inner.read();
            guard
                .sink_schedules
                .iter()
                .map(|(&sink_id, track)| (sink_id, track.clone()))
                .collect_vec()
        };

        let mut statuses =
            futures::future::join_all(schedules.into_iter().map(|(sink_id, track)| async move {
                let pending_snapshot_count = track.last_observed_pending_snapshot_count;
                let next_compaction_after_sec = match &track.state {
                    CompactionTrackState::Idle {
                        next_compaction_time,
                    } => Some(
                        next_compaction_time
                            .saturating_duration_since(now)
                            .as_secs(),
                    ),
                    CompactionTrackState::Processing { .. } => None,
                };
                let is_triggerable = track.should_trigger(now);

                IcebergCompactionScheduleStatus {
                    sink_id,
                    task_type: track.task_type.as_str_name().to_ascii_lowercase(),
                    trigger_interval_sec: track.trigger_interval_sec,
                    trigger_snapshot_count: track.trigger_snapshot_count,
                    schedule_state: match track.state {
                        CompactionTrackState::Idle { .. } => "idle".to_owned(),
                        CompactionTrackState::Processing { .. } => "processing".to_owned(),
                    },
                    next_compaction_after_sec,
                    pending_snapshot_count,
                    is_triggerable,
                }
            }))
            .await;

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

    pub async fn handle_report_task(&self, report: IcebergReportTask) {
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
                let Some(post_commit_snapshot_count) = report.post_commit_snapshot_count else {
                    tracing::warn!(sink_id = %sink_id, task_id, "Missing post_commit_snapshot_count in success report");
                    track.finish_failed(now);
                    return;
                };
                track.finish_success(
                    now,
                    PendingSnapshotState {
                        pending_snapshot_count: post_commit_snapshot_count as usize,
                        current_snapshot_id: report.post_commit_snapshot_id,
                    },
                );
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
        self.compactor_streams_change_tx
            .send((context_id, req_stream))
            .unwrap();
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

    /// Get the number of snapshots pending compaction for a sink's Iceberg table.
    /// Returns None if the table cannot be loaded.
    ///
    /// Counts snapshots since last compaction:
    /// - For COW mode: Counts snapshots on `ingestion` branch with timestamp > current snapshot on main
    /// - For MORE mode: Counts snapshots since last `Replace` on main branch
    async fn get_pending_snapshot_state(&self, sink_id: SinkId) -> Option<PendingSnapshotState> {
        let iceberg_config = self.load_iceberg_config(sink_id).await.ok()?;
        let catalog = iceberg_config.create_catalog().await.ok()?;
        let table_name = iceberg_config.full_table_name().ok()?;
        let table = catalog.load_table(&table_name).await.ok()?;
        Some(PendingSnapshotState {
            pending_snapshot_count: get_pending_snapshot_count_from_table(&iceberg_config, &table)?,
            current_snapshot_id: get_current_snapshot_id(&table),
        })
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
    use super::*;

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
            pending_commit_count,
            last_observed_pending_snapshot_count: Some(pending_commit_count),
            last_snapshot_id: None,
            last_reconciled_at: Some(now),
            awaiting_timeout_reconcile: false,
            force_trigger_pending: false,
            needs_reconcile: false,
            state: CompactionTrackState::Idle {
                next_compaction_time: now + Duration::from_secs(trigger_interval_sec),
            },
        }
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
    fn test_force_compaction_makes_track_triggerable() {
        let now = Instant::now();
        let mut track = new_track(now, 300, 10, 0);

        track.record_force_compaction(now);

        assert_eq!(track.pending_commit_count, 1);
        assert!(track.should_trigger(now));
    }

    #[test]
    fn test_finish_success_updates_snapshot_state_and_cooldown() {
        let now = Instant::now();
        let mut track = new_track(now, 120, 10, 12);
        track.start_processing();

        track.finish_success(
            now,
            PendingSnapshotState {
                pending_snapshot_count: 2,
                current_snapshot_id: Some(42),
            },
        );

        assert_eq!(track.pending_commit_count, 0);
        assert_eq!(track.last_observed_pending_snapshot_count, Some(2));
        assert_eq!(track.last_snapshot_id, Some(42));
        assert_eq!(track.last_reconciled_at, Some(now));
        assert!(!track.needs_reconcile);
        assert!(!track.should_trigger(now));
        match track.state {
            CompactionTrackState::Idle {
                next_compaction_time,
            } => assert!(next_compaction_time >= now + Duration::from_secs(120)),
            CompactionTrackState::Processing { .. } => panic!("track should be idle"),
        }
    }

    #[test]
    fn test_finish_failed_preserves_backlog_and_marks_reconcile() {
        let now = Instant::now();
        let mut track = new_track(now, 120, 10, 4);
        track.start_processing();

        track.finish_failed(now);

        assert_eq!(track.pending_commit_count, 4);
        assert!(track.needs_reconcile);
        assert!(track.should_reconcile(now));
        match track.state {
            CompactionTrackState::Idle {
                next_compaction_time,
            } => assert!(next_compaction_time <= now),
            CompactionTrackState::Processing { .. } => panic!("track should be idle"),
        }
    }

    #[test]
    fn test_finish_success_preserves_commits_arrived_during_processing() {
        let now = Instant::now();
        let mut track = new_track(now, 120, 10, 5);
        track.start_processing();
        track.record_commit(2);

        track.finish_success(
            now,
            PendingSnapshotState {
                pending_snapshot_count: 0,
                current_snapshot_id: Some(100),
            },
        );

        assert_eq!(track.pending_commit_count, 2);
        assert_eq!(track.last_observed_pending_snapshot_count, Some(0));
        assert_eq!(track.last_snapshot_id, Some(100));
    }

    #[test]
    fn test_expire_processing_lease_makes_task_retryable() {
        let now = Instant::now();
        let mut track = new_track(now, 120, 10, 5);
        track.start_processing();

        track.expire_processing_lease(now);

        assert!(track.awaiting_timeout_reconcile);
        assert!(track.needs_reconcile);
        assert!(track.should_reconcile(now));
        assert!(!track.should_trigger(now));
        match track.state {
            CompactionTrackState::Idle {
                next_compaction_time,
            } => assert!(next_compaction_time <= now),
            CompactionTrackState::Processing { .. } => panic!("track should become retryable"),
        }
    }

    #[test]
    fn test_processing_task_remains_pending_without_timeout() {
        let now = Instant::now();
        let mut track = new_track(now, 120, 10, 5);
        track.start_processing();
        track.mark_dispatched(7, now);

        match track.state {
            CompactionTrackState::Processing { .. } => {}
            CompactionTrackState::Idle { .. } => panic!("track should remain pending"),
        }
        assert!(
            !track.is_report_timed_out(
                now + ICEBERG_COMPACTION_REPORT_TIMEOUT - Duration::from_secs(1)
            )
        );
    }

    #[test]
    fn test_reconcile_after_timeout_unblocks_retry() {
        let now = Instant::now();
        let mut track = new_track(now, 120, 3, 5);
        track.start_processing();
        track.expire_processing_lease(now);

        track.apply_idle_reconcile_result(
            now,
            PendingSnapshotState {
                pending_snapshot_count: 4,
                current_snapshot_id: Some(99),
            },
        );

        assert!(!track.awaiting_timeout_reconcile);
        assert!(!track.needs_reconcile);
        assert_eq!(track.pending_commit_count, 4);
        assert!(track.should_trigger(now));
    }

    #[test]
    fn test_force_compaction_survives_timeout_reconcile_with_backlog() {
        let now = Instant::now();
        let mut track = new_track(now, 120, 10, 0);
        track.record_force_compaction(now);
        track.start_processing();
        track.expire_processing_lease(now);

        track.apply_idle_reconcile_result(
            now,
            PendingSnapshotState {
                pending_snapshot_count: 1,
                current_snapshot_id: Some(7),
            },
        );

        assert!(track.force_trigger_pending);
        assert!(track.should_trigger(now));
    }

    #[test]
    fn test_timeout_reconcile_clears_force_when_backlog_is_gone() {
        let now = Instant::now();
        let mut track = new_track(now, 120, 10, 0);
        track.record_force_compaction(now);
        track.start_processing();
        track.expire_processing_lease(now);

        track.apply_idle_reconcile_result(
            now,
            PendingSnapshotState {
                pending_snapshot_count: 0,
                current_snapshot_id: Some(7),
            },
        );

        assert!(!track.force_trigger_pending);
        assert!(!track.should_trigger(now));
    }

    #[test]
    fn test_force_does_not_make_processing_track_triggerable() {
        let now = Instant::now();
        let mut track = new_track(now, 120, 10, 0);
        track.record_force_compaction(now);
        track.start_processing();

        assert!(!track.should_trigger(now));
    }
}
