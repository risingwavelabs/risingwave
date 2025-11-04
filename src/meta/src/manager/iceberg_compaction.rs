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

#[derive(Debug, Clone)]
struct CompactionTrack {
    #[allow(dead_code)]
    task_type: TaskType,
    count: usize,
    next_compaction_time: Option<Instant>,
    trigger_interval_sec: u64,
    trigger_commit_threshold: usize,
    min_commit_for_trigger: usize,
}

impl CompactionTrack {
    fn should_trigger(&self, now: Instant) -> bool {
        // Minimum commit protection
        if self.count < self.min_commit_for_trigger {
            return false;
        }

        // Check time condition
        let time_ready = self
            .next_compaction_time
            .map(|next| now >= next)
            .unwrap_or(false);

        // Check commit count condition
        let commit_ready = self.count >= self.trigger_commit_threshold;

        // OR semantics: trigger if either condition is met
        time_ready || commit_ready
    }

    fn set_processing(&mut self) {
        self.count = 0;
        // Set next_compaction_time to None to indicate processing
        self.next_compaction_time.take();
    }

    fn initialize(&mut self) {
        self.count = 0;
        self.next_compaction_time =
            Some(Instant::now() + std::time::Duration::from_secs(self.trigger_interval_sec));
    }
}

#[derive(Debug, Clone)]
struct CompactionScheduleState {
    tracks: HashMap<TaskType, CompactionTrack>,
}

impl CompactionScheduleState {
    fn increase_count(&mut self) {
        for track in self.tracks.values_mut() {
            track.count += 1;
        }
    }

    fn get_triggerable_tasks(&self, now: Instant) -> Vec<TaskType> {
        self.tracks
            .iter()
            .filter(|(_, track)| track.should_trigger(now))
            .map(|(task_type, _)| *task_type)
            .collect()
    }

    fn update_trigger_interval(&mut self, task_type: TaskType, trigger_interval_sec: u64) {
        if let Some(track) = self.tracks.get_mut(&task_type) {
            track.trigger_interval_sec = trigger_interval_sec;
            // Reset the next compaction time
            track.next_compaction_time =
                Some(Instant::now() + std::time::Duration::from_secs(trigger_interval_sec));
        }
    }
}

pub struct IcebergCompactionHandle {
    sink_id: SinkId,
    task_type: TaskType,
    inner: Arc<RwLock<IcebergCompactionManagerInner>>,
    metadata_manager: MetadataManager,
    handle_success: bool,

    /// Snapshot of the compaction track for recovery.
    track_snapshot: CompactionTrack,
}

impl IcebergCompactionHandle {
    fn new(
        sink_id: SinkId,
        task_type: TaskType,
        inner: Arc<RwLock<IcebergCompactionManagerInner>>,
        metadata_manager: MetadataManager,
        track_snapshot: CompactionTrack,
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
        if let Some(schedule_state) = guard.sink_schedules.get_mut(&self.sink_id) {
            if let Some(track) = schedule_state.tracks.get_mut(&self.task_type) {
                if self.handle_success {
                    track.initialize();
                } else {
                    // Restore the original state
                    *track = self.track_snapshot.clone();
                }
            }
        }
    }
}

struct IcebergCompactionManagerInner {
    pub sink_schedules: HashMap<SinkId, CompactionScheduleState>,
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
                        manager.update_iceberg_commit_info(stat);
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

    pub fn update_iceberg_commit_info(&self, msg: IcebergSinkCompactionUpdate) {
        let mut guard = self.inner.write();

        let IcebergSinkCompactionUpdate {
            sink_id,
            compaction_interval,
            force_compaction,
        } = msg;

        let schedule_state = guard
            .sink_schedules
            .entry(sink_id)
            .or_insert_with(|| self.create_default_schedule_state(compaction_interval));

        schedule_state.increase_count();

        // Force compaction: set all tracks to trigger immediately
        if force_compaction {
            for track in schedule_state.tracks.values_mut() {
                track.next_compaction_time = Some(Instant::now());
            }
        } else {
            // Update trigger interval for the primary track (Small or Full based on license)
            let primary_task_type = if risingwave_common::license::Feature::IcebergCompaction
                .check_available()
                .is_ok()
            {
                TaskType::SmallFiles
            } else {
                TaskType::Full
            };

            if let Some(track) = schedule_state.tracks.get(&primary_task_type) {
                if track.trigger_interval_sec != compaction_interval {
                    schedule_state.update_trigger_interval(primary_task_type, compaction_interval);
                }
            }
        }
    }

    fn create_default_schedule_state(&self, compaction_interval: u64) -> CompactionScheduleState {
        let mut tracks = HashMap::new();

        // Check if licensed for advanced compaction features
        let is_licensed = risingwave_common::license::Feature::IcebergCompaction
            .check_available()
            .is_ok();

        if is_licensed {
            // Licensed: enable multi-track compaction with different frequencies

            // Track 1: SmallFiles - High frequency, low cost
            tracks.insert(
                TaskType::SmallFiles,
                CompactionTrack {
                    task_type: TaskType::SmallFiles,
                    count: 0,
                    next_compaction_time: Some(
                        Instant::now() + std::time::Duration::from_secs(compaction_interval),
                    ),
                    trigger_interval_sec: compaction_interval,
                    trigger_commit_threshold: 50,
                    min_commit_for_trigger: 10,
                },
            );

            // Track 2: FilesWithDelete - Medium frequency, medium cost
            tracks.insert(
                TaskType::FilesWithDelete,
                CompactionTrack {
                    task_type: TaskType::FilesWithDelete,
                    count: 0,
                    next_compaction_time: Some(
                        Instant::now() + std::time::Duration::from_secs(compaction_interval * 6),
                    ),
                    trigger_interval_sec: compaction_interval * 6, // 6x of SmallFiles interval
                    trigger_commit_threshold: 200,
                    min_commit_for_trigger: 30,
                },
            );

            // Track 3: Full - Low frequency, high cost (optional, can be disabled)
            // Uncomment if you want to enable periodic full compaction alongside small file compaction
            tracks.insert(
                TaskType::Full,
                CompactionTrack {
                    task_type: TaskType::Full,
                    count: 0,
                    next_compaction_time: Some(
                        Instant::now() + std::time::Duration::from_secs(compaction_interval * 24),
                    ),
                    trigger_interval_sec: compaction_interval * 24, // 24x of SmallFiles interval
                    trigger_commit_threshold: 1000,
                    min_commit_for_trigger: 100,
                },
            );
        } else {
            // Unlicensed: only enable Full compaction (backward compatible)
            tracks.insert(
                TaskType::Full,
                CompactionTrack {
                    task_type: TaskType::Full,
                    count: 0,
                    next_compaction_time: Some(
                        Instant::now() + std::time::Duration::from_secs(compaction_interval),
                    ),
                    trigger_interval_sec: compaction_interval,
                    trigger_commit_threshold: 100,
                    min_commit_for_trigger: 10,
                },
            );
        }

        CompactionScheduleState { tracks }
    }

    /// Get the top N compaction tasks to trigger
    /// Returns handles for tasks that are ready to be compacted
    /// Sorted by commit count and next compaction time
    pub fn get_top_n_iceberg_commit_sink_ids(&self, n: usize) -> Vec<IcebergCompactionHandle> {
        let now = Instant::now();
        let mut guard = self.inner.write();

        // Collect all triggerable tasks with their priority info
        let mut candidates = Vec::new();
        for (sink_id, schedule_state) in guard.sink_schedules.iter() {
            for task_type in schedule_state.get_triggerable_tasks(now) {
                if let Some(track) = schedule_state.tracks.get(&task_type) {
                    candidates.push((
                        sink_id.clone(),
                        task_type,
                        track.count,
                        track.next_compaction_time,
                    ));
                }
            }
        }

        // Sort by commit count (descending) and next_compaction_time (ascending)
        candidates.sort_by(|a, b| b.2.cmp(&a.2).then_with(|| a.3.cmp(&b.3)));

        // Take top N and create handles
        candidates
            .into_iter()
            .take(n)
            .filter_map(|(sink_id, task_type, _, _)| {
                let schedule_state = guard.sink_schedules.get_mut(&sink_id)?;
                let track = schedule_state.tracks.get_mut(&task_type)?;

                let track_snapshot = track.clone();
                track.set_processing();

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
