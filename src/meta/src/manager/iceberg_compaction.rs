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
use std::time::Instant;

use anyhow::anyhow;
use iceberg::spec::Operation;
use iceberg::transaction::Transaction;
use itertools::Itertools;
use parking_lot::RwLock;
use risingwave_common::bail;
use risingwave_connector::connector_common::IcebergSinkCompactionUpdate;
use risingwave_connector::sink::catalog::{SinkCatalog, SinkId};
use risingwave_connector::sink::iceberg::{IcebergConfig, should_enable_iceberg_cow};
use risingwave_connector::sink::{SinkError, SinkParam};
use risingwave_pb::catalog::PbSink;
use risingwave_pb::iceberg_compaction::iceberg_compaction_task::TaskType;
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

type CompactorChangeTx = UnboundedSender<(u32, Streaming<SubscribeIcebergCompactionEventRequest>)>;

type CompactorChangeRx =
    UnboundedReceiver<(u32, Streaming<SubscribeIcebergCompactionEventRequest>)>;

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
    /// Minimum snapshot count threshold to trigger compaction
    trigger_snapshot_count: usize,
    state: CompactionTrackState,
}

impl CompactionTrack {
    fn should_trigger(&self, now: Instant, snapshot_count: usize) -> bool {
        // Only Idle state can trigger
        let next_compaction_time = match &self.state {
            CompactionTrackState::Idle {
                next_compaction_time,
            } => *next_compaction_time,
            CompactionTrackState::Processing => return false,
        };

        // Check both time and snapshot count conditions
        let time_ready = now >= next_compaction_time;
        let snapshot_ready = snapshot_count >= self.trigger_snapshot_count;

        time_ready && snapshot_ready
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

    pub async fn send_compact_task(
        mut self,
        compactor: Arc<IcebergCompactor>,
        task_id: u64,
    ) -> MetaResult<()> {
        use risingwave_pb::iceberg_compaction::subscribe_iceberg_compaction_event_response::Event as IcebergResponseEvent;
        let mut sinks = self
            .metadata_manager
            .catalog_controller
            .get_sink_by_ids(vec![self.sink_id.sink_id as i32])
            .await?;
        if sinks.is_empty() {
            // The sink may be deleted, just return Ok.
            tracing::warn!("Sink not found: {}", self.sink_id.sink_id);
            return Ok(());
        }
        let prost_sink_catalog: PbSink = sinks.remove(0);
        let sink_catalog = SinkCatalog::from(prost_sink_catalog);
        let param = SinkParam::try_from_sink_catalog(sink_catalog)?;

        let result =
            compactor.send_event(IcebergResponseEvent::CompactTask(IcebergCompactionTask {
                task_id,
                props: param.properties,
                task_type: self.task_type as i32,
            }));

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
            let iceberg_config = self.load_iceberg_config(&sink_id).await;

            let new_track = match iceberg_config {
                Ok(config) => {
                    // Call synchronous create function with the config
                    match self.create_compaction_track(sink_id, &config) {
                        Ok(track) => track,
                        Err(e) => {
                            tracing::error!(
                                error = ?e.as_report(),
                                "Failed to create compaction track from config for sink {}, using default Full track",
                                sink_id.sink_id
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
                        sink_id.sink_id
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
            // Force compaction: set to trigger immediately if in Idle state
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
                sink_id.sink_id
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

        // For COW mode, always use Full compaction regardless of config
        let task_type = if should_enable_iceberg_cow(
            iceberg_config.r#type.as_str(),
            iceberg_config.write_mode.as_str(),
        ) {
            TaskType::Full
        } else {
            // For MORE mode, use configured compaction_type
            match iceberg_config.compaction_type() {
                "full" => TaskType::Full,
                "small_files" => TaskType::SmallFiles,
                "files_with_delete" => TaskType::FilesWithDelete,
                unknown => {
                    tracing::warn!("Unknown compaction_type '{}', defaulting to Full", unknown);
                    TaskType::Full
                }
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
                let count = self.get_snapshot_count(sink_id).await?;
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

    pub async fn get_sink_param(&self, sink_id: &SinkId) -> MetaResult<SinkParam> {
        let mut sinks = self
            .metadata_manager
            .catalog_controller
            .get_sink_by_ids(vec![sink_id.sink_id as i32])
            .await?;
        if sinks.is_empty() {
            bail!("Sink not found: {}", sink_id.sink_id);
        }
        let prost_sink_catalog: PbSink = sinks.remove(0);
        let sink_catalog = SinkCatalog::from(prost_sink_catalog);
        let param = SinkParam::try_from_sink_catalog(sink_catalog)?;
        Ok(param)
    }

    pub async fn load_iceberg_config(&self, sink_id: &SinkId) -> MetaResult<IcebergConfig> {
        let sink_param = self.get_sink_param(sink_id).await?;
        let iceberg_config = IcebergConfig::from_btreemap(sink_param.properties)?;
        Ok(iceberg_config)
    }

    pub fn add_compactor_stream(
        &self,
        context_id: u32,
        req_stream: Streaming<SubscribeIcebergCompactionEventRequest>,
    ) {
        self.compactor_streams_change_tx
            .send((context_id, req_stream))
            .unwrap();
    }

    pub fn iceberg_compaction_event_loop(
        iceberg_compaction_manager: Arc<Self>,
        compactor_streams_change_rx: UnboundedReceiver<(
            u32,
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
        let iceberg_config = self.load_iceberg_config(&sink_id).await?;
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

        let sink_param = self.get_sink_param(&sink_id).await?;

        compactor.send_event(IcebergResponseEvent::CompactTask(IcebergCompactionTask {
            task_id,
            props: sink_param.properties,
            task_type: TaskType::Full as i32, // default to full compaction
        }))?;

        tracing::info!(
            "Manual compaction triggered for sink {} with task ID {}, waiting for completion...",
            sink_id.sink_id,
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

        let cow = should_enable_iceberg_cow(
            iceberg_config.r#type.as_str(),
            iceberg_config.write_mode.as_str(),
        );

        while elapsed_time < MAX_WAIT_TIME_SECS {
            let poll_interval = std::time::Duration::from_secs(current_interval_secs);
            tokio::time::sleep(poll_interval).await;
            elapsed_time += current_interval_secs;

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
                        return Ok(());
                    }
                } else if matches!(summary.operation, Operation::Replace) {
                    return Ok(());
                }
            }

            current_interval_secs = std::cmp::min(
                MAX_POLL_INTERVAL_SECS,
                ((current_interval_secs as f64) * BACKOFF_MULTIPLIER) as u64,
            );
        }

        Err(anyhow!(
            "Compaction did not complete within {} seconds for sink {} (task_id={})",
            MAX_WAIT_TIME_SECS,
            sink_id.sink_id,
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
            if let Err(e) = self.check_and_expire_snapshots(&sink_id).await {
                tracing::error!(error = ?e.as_report(), "Failed to perform GC for sink {}", sink_id.sink_id);
            }
        }

        tracing::info!("GC operations completed");
        Ok(())
    }

    /// Get the snapshot count for a sink's Iceberg table
    /// Returns None if the table cannot be loaded
    async fn get_snapshot_count(&self, sink_id: &SinkId) -> Option<usize> {
        let iceberg_config = self.load_iceberg_config(sink_id).await.ok()?;
        let catalog = iceberg_config.create_catalog().await.ok()?;
        let table_name = iceberg_config.full_table_name().ok()?;
        let table = catalog.load_table(&table_name).await.ok()?;

        let metadata = table.metadata();
        let mut snapshots = metadata.snapshots().collect_vec();

        if snapshots.is_empty() {
            return Some(0);
        }

        // Sort snapshots by timestamp
        snapshots.sort_by_key(|s| s.timestamp_ms());

        // Find the last Replace operation snapshot
        let last_replace_index = snapshots
            .iter()
            .rposition(|snapshot| matches!(snapshot.summary().operation, Operation::Replace));

        // Calculate count from last Replace to the latest snapshot
        let snapshot_count = match last_replace_index {
            Some(index) => snapshots.len() - index - 1,
            None => snapshots.len(), // No Replace found, count all snapshots
        };

        Some(snapshot_count)
    }

    pub async fn check_and_expire_snapshots(&self, sink_id: &SinkId) -> MetaResult<()> {
        const MAX_SNAPSHOT_AGE_MS_DEFAULT: i64 = 24 * 60 * 60 * 1000; // 24 hours
        let now = chrono::Utc::now().timestamp_millis();

        let iceberg_config = self.load_iceberg_config(sink_id).await?;
        if !iceberg_config.enable_snapshot_expiration {
            return Ok(());
        }

        let catalog = iceberg_config.create_catalog().await?;
        let table = catalog
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
            sink_id = sink_id.sink_id,
            snapshots_len = snapshots.len(),
            snapshot_expiration_timestamp_ms = snapshot_expiration_timestamp_ms,
            snapshot_expiration_retain_last = ?iceberg_config.snapshot_expiration_retain_last,
            clear_expired_files = ?iceberg_config.snapshot_expiration_clear_expired_files,
            clear_expired_meta_data = ?iceberg_config.snapshot_expiration_clear_expired_meta_data,
            "try trigger snapshots expiration",
        );

        let tx = Transaction::new(&table);

        let mut expired_snapshots = tx.expire_snapshot();

        expired_snapshots = expired_snapshots.expire_older_than(snapshot_expiration_timestamp_ms);

        if let Some(retain_last) = iceberg_config.snapshot_expiration_retain_last {
            expired_snapshots = expired_snapshots.retain_last(retain_last);
        }

        expired_snapshots = expired_snapshots
            .clear_expired_files(iceberg_config.snapshot_expiration_clear_expired_files);

        expired_snapshots = expired_snapshots
            .clear_expired_meta_data(iceberg_config.snapshot_expiration_clear_expired_meta_data);

        let tx = expired_snapshots
            .apply()
            .await
            .map_err(|e| SinkError::Iceberg(e.into()))?;

        tx.commit(catalog.as_ref())
            .await
            .map_err(|e| SinkError::Iceberg(e.into()))?;

        tracing::info!(
            catalog_name = iceberg_config.catalog_name(),
            table_name = iceberg_config.full_table_name()?.to_string(),
            sink_id = sink_id.sink_id,
            "Expired snapshots for iceberg table",
        );

        Ok(())
    }
}
