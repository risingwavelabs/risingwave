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
use iceberg::table::Table;
use iceberg::transaction::Transaction;
use itertools::Itertools;
use parking_lot::RwLock;
use risingwave_common::bail;
use risingwave_connector::connector_common::IcebergSinkCompactionUpdate;
use risingwave_connector::sink::catalog::{SinkCatalog, SinkId};
use risingwave_connector::sink::iceberg::IcebergConfig;
use risingwave_connector::sink::{SinkError, SinkParam};
use risingwave_pb::catalog::PbSink;
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
struct CommitInfo {
    count: usize,
    next_compaction_time: Option<Instant>,
    compaction_interval: u64,
}

impl CommitInfo {
    fn set_processing(&mut self) {
        self.count = 0;
        // `set next_compaction_time` to `None` value that means is processing
        self.next_compaction_time.take();
    }

    fn initialize(&mut self) {
        self.count = 0;
        self.next_compaction_time =
            Some(Instant::now() + std::time::Duration::from_secs(self.compaction_interval));
    }

    fn replace(&mut self, commit_info: CommitInfo) {
        self.count = commit_info.count;
        self.next_compaction_time = commit_info.next_compaction_time;
        self.compaction_interval = commit_info.compaction_interval;
    }

    fn increase_count(&mut self) {
        self.count += 1;
    }

    fn update_compaction_interval(&mut self, compaction_interval: u64) {
        self.compaction_interval = compaction_interval;

        // reset the next compaction time
        self.next_compaction_time =
            Some(Instant::now() + std::time::Duration::from_secs(compaction_interval));
    }
}

pub struct IcebergCompactionHandle {
    sink_id: SinkId,
    inner: Arc<RwLock<IcebergCompactionManagerInner>>,
    metadata_manager: MetadataManager,
    handle_success: bool,

    /// The commit info of the iceberg compaction handle for recovery.
    commit_info: CommitInfo,
}

impl IcebergCompactionHandle {
    fn new(
        sink_id: SinkId,
        inner: Arc<RwLock<IcebergCompactionManagerInner>>,
        metadata_manager: MetadataManager,
        commit_info: CommitInfo,
    ) -> Self {
        Self {
            sink_id,
            inner,
            metadata_manager,
            handle_success: false,
            commit_info,
        }
    }

    pub async fn send_compact_task(
        mut self,
        compactor: Arc<IcebergCompactor>,
        task_id: u64,
    ) -> MetaResult<()> {
        use risingwave_pb::iceberg_compaction::subscribe_iceberg_compaction_event_response::Event as IcebergResponseEvent;
        let prost_sink_catalog: PbSink = self
            .metadata_manager
            .catalog_controller
            .get_sink_by_ids(vec![self.sink_id.sink_id as i32])
            .await?
            .remove(0);
        let sink_catalog = SinkCatalog::from(prost_sink_catalog);
        let param = SinkParam::try_from_sink_catalog(sink_catalog)?;
        let result =
            compactor.send_event(IcebergResponseEvent::CompactTask(IcebergCompactionTask {
                // Todo! Use iceberg's compaction task ID
                task_id,
                props: param.properties,
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
        if self.handle_success {
            let mut guard = self.inner.write();
            if let Some(commit_info) = guard.iceberg_commits.get_mut(&self.sink_id) {
                commit_info.initialize();
            }
        } else {
            // If the handle is not successful, we need to reset the commit info
            // to the original state.
            // This is to avoid the case where the handle is dropped before the
            // compaction task is sent.
            let mut guard = self.inner.write();
            if let Some(commit_info) = guard.iceberg_commits.get_mut(&self.sink_id) {
                commit_info.replace(self.commit_info.clone());
            }
        }
    }
}

struct IcebergCompactionManagerInner {
    pub iceberg_commits: HashMap<SinkId, CommitInfo>,
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
                    iceberg_commits: HashMap::default(),
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
        } = msg;

        // if the compaction interval is changed, we need to reset the commit info when the compaction task is sent of initialized
        let commit_info = guard.iceberg_commits.entry(sink_id).or_insert(CommitInfo {
            count: 0,
            next_compaction_time: Some(
                Instant::now() + std::time::Duration::from_secs(compaction_interval),
            ),
            compaction_interval,
        });

        commit_info.increase_count();
        if commit_info.compaction_interval != compaction_interval {
            commit_info.update_compaction_interval(compaction_interval);
        }
    }

    /// Get the top N iceberg commit sink ids
    /// Sorted by commit count and next compaction time
    pub fn get_top_n_iceberg_commit_sink_ids(&self, n: usize) -> Vec<IcebergCompactionHandle> {
        let now = Instant::now();
        let mut guard = self.inner.write();
        guard
            .iceberg_commits
            .iter_mut()
            .filter(|(_, commit_info)| {
                commit_info.count > 0
                    && if let Some(next_compaction_time) = commit_info.next_compaction_time {
                        next_compaction_time <= now
                    } else {
                        false
                    }
            })
            .sorted_by(|a, b| {
                b.1.count
                    .cmp(&a.1.count)
                    .then_with(|| b.1.next_compaction_time.cmp(&a.1.next_compaction_time))
            })
            .take(n)
            .map(|(sink_id, commit_info)| {
                // reset the commit count and next compaction time and avoid double call
                let handle = IcebergCompactionHandle::new(
                    *sink_id,
                    self.inner.clone(),
                    self.metadata_manager.clone(),
                    commit_info.clone(),
                );

                commit_info.set_processing();

                handle
            })
            .collect::<Vec<_>>()
    }

    pub fn clear_iceberg_commits_by_sink_id(&self, sink_id: SinkId) {
        let mut guard = self.inner.write();
        guard.iceberg_commits.remove(&sink_id);
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

    #[allow(dead_code)]
    pub async fn load_iceberg_table(&self, sink_id: &SinkId) -> MetaResult<Table> {
        let sink_param = self.get_sink_param(sink_id).await?;
        let iceberg_config = IcebergConfig::from_btreemap(sink_param.properties.clone())?;
        let table = iceberg_config.load_table().await?;
        Ok(table)
    }

    pub async fn load_iceberg_config(&self, sink_id: &SinkId) -> MetaResult<IcebergConfig> {
        let sink_param = self.get_sink_param(sink_id).await?;
        let iceberg_config = IcebergConfig::from_btreemap(sink_param.properties.clone())?;
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
    pub fn gc_loop(manager: Arc<Self>) -> (JoinHandle<()>, Sender<()>) {
        let (shutdown_tx, mut shutdown_rx) = tokio::sync::oneshot::channel();
        let join_handle = tokio::spawn(async move {
            // Run GC every hour by default
            const GC_LOOP_INTERVAL_SECS: u64 = 3600;
            let mut interval =
                tokio::time::interval(std::time::Duration::from_secs(GC_LOOP_INTERVAL_SECS));

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
        let initial_table = self.load_iceberg_table(&sink_id).await?;
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
        }))?;

        tracing::info!(
            "Manual compaction triggered for sink {} with task ID {}, waiting for completion...",
            sink_id.sink_id,
            task_id
        );

        self.wait_for_compaction_completion(
            &sink_id,
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

        while elapsed_time < MAX_WAIT_TIME_SECS {
            let poll_interval = std::time::Duration::from_secs(current_interval_secs);
            tokio::time::sleep(poll_interval).await;
            elapsed_time += current_interval_secs;

            let current_table = self.load_iceberg_table(sink_id).await?;

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
                if matches!(summary.operation, Operation::Replace) {
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
            guard.iceberg_commits.keys().cloned().collect::<Vec<_>>()
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

    async fn check_and_expire_snapshots(&self, sink_id: &SinkId) -> MetaResult<()> {
        const MAX_SNAPSHOT_AGE_MS_DEFAULT: i64 = 24 * 60 * 60 * 1000;
        let now = chrono::Utc::now().timestamp_millis();
        let expired_older_than = now - MAX_SNAPSHOT_AGE_MS_DEFAULT;

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

        if snapshots.is_empty() || snapshots.first().unwrap().timestamp_ms() > expired_older_than {
            // avoid commit empty table updates
            return Ok(());
        }

        tracing::info!(
            "Catalog {} table {} sink-id {} has {} snapshots try trigger expiration",
            iceberg_config.catalog_name(),
            iceberg_config.full_table_name()?,
            sink_id.sink_id,
            snapshots.len(),
        );

        let tx = Transaction::new(&table);

        // TODO: use config
        let expired_snapshots = tx
            .expire_snapshot()
            .clear_expired_files(true)
            .clear_expired_meta_data(true);

        let tx = expired_snapshots
            .apply()
            .await
            .map_err(|e| SinkError::Iceberg(e.into()))?;
        tx.commit(catalog.as_ref())
            .await
            .map_err(|e| SinkError::Iceberg(e.into()))?;

        tracing::info!(
            "Expired snapshots for iceberg catalog {} table {} sink-id {}",
            iceberg_config.catalog_name(),
            iceberg_config.full_table_name()?,
            sink_id.sink_id,
        );

        Ok(())
    }
}
