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

use iceberg::table::Table;
use itertools::Itertools;
use parking_lot::RwLock;
use risingwave_connector::connector_common::IcebergSinkCompactionUpdate;
use risingwave_connector::sink::SinkParam;
use risingwave_connector::sink::catalog::{SinkCatalog, SinkId};
use risingwave_connector::sink::iceberg::IcebergConfig;
use risingwave_pb::catalog::PbSink;
use risingwave_pb::iceberg_compaction::{
    IcebergCompactionTask, SubscribeIcebergCompactionEventRequest,
};
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
        let prost_sink_catalog: PbSink = self
            .metadata_manager
            .catalog_controller
            .get_sink_by_ids(vec![sink_id.sink_id as i32])
            .await?
            .remove(0);
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
}
