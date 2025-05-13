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
use std::time::Instant;

use iceberg::table::Table;
use parking_lot::RwLock;
use risingwave_connector::connector_common::IcebergCompactionStat;
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
    first_commit_time: Instant,
}

pub struct IcebergCompactionHandle {
    sink_id: SinkId,
    inner: Arc<RwLock<IcebergCompactionManagerInner>>,
    metadata_manager: MetadataManager,
    handle_success: bool,
}

impl IcebergCompactionHandle {
    fn new(
        sink_id: SinkId,
        inner: Arc<RwLock<IcebergCompactionManagerInner>>,
        metadata_manager: MetadataManager,
    ) -> Self {
        Self {
            sink_id,
            inner,
            metadata_manager,
            handle_success: false,
        }
    }

    pub async fn send_compact_task(mut self, compactor: Arc<IcebergCompactor>) -> MetaResult<()> {
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
        let mut guard = self.inner.write();
        guard.processing_list.remove(&self.sink_id);
        if self.handle_success {
            guard.iceberg_commits.remove(&self.sink_id);
        }
    }
}

struct IcebergCompactionManagerInner {
    pub iceberg_commits: HashMap<SinkId, CommitInfo>,
    pub processing_list: HashSet<SinkId>,
}

pub struct IcebergCompactionManager {
    inner: Arc<RwLock<IcebergCompactionManagerInner>>,

    metadata_manager: MetadataManager,
    pub iceberg_compactor_manager: IcebergCompactorManagerRef,

    compactor_streams_change_tx: CompactorChangeTx,

    pub metrics: Arc<MetaMetrics>,
}

impl IcebergCompactionManager {
    pub fn build(
        metadata_manager: MetadataManager,
        iceberg_compactor_manager: IcebergCompactorManagerRef,
        metrics: Arc<MetaMetrics>,
    ) -> (Arc<Self>, CompactorChangeRx) {
        let (compactor_streams_change_tx, compactor_streams_change_rx) =
            tokio::sync::mpsc::unbounded_channel();
        (
            Arc::new(Self {
                inner: Arc::new(RwLock::new(IcebergCompactionManagerInner {
                    iceberg_commits: HashMap::new(),
                    processing_list: HashSet::new(),
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
        mut rx: UnboundedReceiver<IcebergCompactionStat>,
    ) -> (JoinHandle<()>, Sender<()>) {
        let (shutdown_tx, mut shutdown_rx) = tokio::sync::oneshot::channel();
        let join_handle = tokio::spawn(async move {
            loop {
                tokio::select! {
                    Some(stat) = rx.recv() => {
                        manager.record_iceberg_commit(stat.sink_id);
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

    pub fn record_iceberg_commit(&self, sink_id: SinkId) {
        let mut guard = self.inner.write();

        // Check if the sink_id is already in the processing list and avoid double counting
        if guard.processing_list.contains(&sink_id) {
            return;
        }

        let commit_info = guard.iceberg_commits.entry(sink_id).or_insert(CommitInfo {
            count: 0,
            first_commit_time: Instant::now(),
        });
        commit_info.count += 1;
    }

    /// Get the top N iceberg commit sink ids
    /// Sorted by commit count and first commit time
    /// The sink ids are filtered by the minimum interval
    pub fn get_top_n_iceberg_commit_sink_ids(&self, n: usize) -> Vec<IcebergCompactionHandle> {
        let mut sink_ids = {
            let guard = self.inner.read();
            guard
                .iceberg_commits
                .iter()
                .map(|(sink_id, commit_info)| (*sink_id, commit_info.clone()))
                .collect::<Vec<_>>()
        };

        sink_ids.sort_by(|a, b| {
            let a_commit_count = a.1.count;
            let b_commit_count = b.1.count;

            if a_commit_count != b_commit_count {
                return b_commit_count.cmp(&a_commit_count);
            }

            b.1.first_commit_time
                .duration_since(a.1.first_commit_time)
                .cmp(&std::time::Duration::from_secs(0))
        });

        const MIN_SINK_COMMIT_INTERVAL: u64 = 3600;
        let compaction_handles: Vec<IcebergCompactionHandle> = sink_ids
            .iter()
            .filter(|(_, commit_info)| {
                Instant::now().duration_since(commit_info.first_commit_time)
                    > std::time::Duration::from_secs(MIN_SINK_COMMIT_INTERVAL)
            })
            .take(n)
            .map(|(sink_id, _)| {
                // get sink-param
                IcebergCompactionHandle::new(
                    *sink_id,
                    self.inner.clone(),
                    self.metadata_manager.clone(),
                )
            })
            .collect();

        // Update the processing list
        {
            let mut guard = self.inner.write();
            for handle in &compaction_handles {
                guard.processing_list.insert(handle.sink_id());
            }
        }

        compaction_handles
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
