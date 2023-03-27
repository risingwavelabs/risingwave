// Copyright 2023 RisingWave Labs
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

use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;

use anyhow::anyhow;
use async_trait::async_trait;
use fail::fail_point;
use futures::stream::{BoxStream, Stream};
use futures::StreamExt;
use risingwave_hummock_sdk::compaction_group::StaticCompactionGroupId;
use risingwave_hummock_sdk::table_stats::{to_prost_table_stats_map, TableStatsMap};
use risingwave_hummock_sdk::{
    HummockContextId, HummockEpoch, HummockSstableObjectId, HummockVersionId, LocalSstableInfo,
    SstObjectIdRange,
};
use risingwave_pb::common::{HostAddress, WorkerType};
use risingwave_pb::hummock::subscribe_compact_tasks_response::Task;
use risingwave_pb::hummock::{
    compact_task, CompactTask, CompactTaskProgress, CompactorWorkload, HummockSnapshot,
    HummockVersion, SubscribeCompactTasksResponse, VacuumTask,
};
use risingwave_rpc_client::error::{Result, RpcError};
use risingwave_rpc_client::{CompactTaskItem, HummockMetaClient};
use tokio::task::JoinHandle;
use tokio_stream::wrappers::UnboundedReceiverStream;

use crate::hummock::compaction::{
    default_level_selector, LevelSelector, SpaceReclaimCompactionSelector,
};
use crate::hummock::compaction_scheduler::CompactionRequestChannel;
use crate::hummock::HummockManager;
use crate::storage::MemStore;

pub struct MockHummockMetaClient {
    hummock_manager: Arc<HummockManager<MemStore>>,
    context_id: HummockContextId,
    compact_context_id: AtomicU32,
}

impl MockHummockMetaClient {
    pub fn new(
        hummock_manager: Arc<HummockManager<MemStore>>,
        context_id: HummockContextId,
    ) -> MockHummockMetaClient {
        MockHummockMetaClient {
            hummock_manager,
            context_id,
            compact_context_id: AtomicU32::new(context_id),
        }
    }

    pub async fn get_compact_task(&self) -> Option<CompactTask> {
        self.hummock_manager
            .get_compact_task(
                StaticCompactionGroupId::StateDefault.into(),
                &mut default_level_selector(),
            )
            .await
            .unwrap_or(None)
    }
}

fn mock_err(error: super::error::Error) -> RpcError {
    anyhow!("mock error: {}", error).into()
}

#[async_trait]
impl HummockMetaClient for MockHummockMetaClient {
    async fn unpin_version_before(&self, unpin_version_before: HummockVersionId) -> Result<()> {
        self.hummock_manager
            .unpin_version_before(self.context_id, unpin_version_before)
            .await
            .map_err(mock_err)
    }

    async fn get_current_version(&self) -> Result<HummockVersion> {
        Ok(self.hummock_manager.get_current_version().await)
    }

    async fn pin_snapshot(&self) -> Result<HummockSnapshot> {
        self.hummock_manager
            .pin_snapshot(self.context_id)
            .await
            .map_err(mock_err)
    }

    async fn get_epoch(&self) -> Result<HummockSnapshot> {
        self.hummock_manager.get_last_epoch().map_err(mock_err)
    }

    async fn unpin_snapshot(&self) -> Result<()> {
        self.hummock_manager
            .unpin_snapshot(self.context_id)
            .await
            .map_err(mock_err)
    }

    async fn unpin_snapshot_before(&self, pinned_epochs: HummockEpoch) -> Result<()> {
        self.hummock_manager
            .unpin_snapshot_before(
                self.context_id,
                HummockSnapshot {
                    committed_epoch: pinned_epochs,
                    current_epoch: pinned_epochs,
                },
            )
            .await
            .map_err(mock_err)
    }

    async fn get_new_sst_ids(&self, number: u32) -> Result<SstObjectIdRange> {
        fail_point!("get_new_sst_ids_err", |_| Err(anyhow!(
            "failpoint get_new_sst_ids_err"
        )
        .into()));
        self.hummock_manager
            .get_new_sst_ids(number)
            .await
            .map_err(mock_err)
    }

    async fn report_compaction_task(
        &self,
        mut compact_task: CompactTask,
        table_stats_change: TableStatsMap,
    ) -> Result<()> {
        self.hummock_manager
            .report_compact_task(
                self.compact_context_id.load(Ordering::Acquire),
                &mut compact_task,
                Some(to_prost_table_stats_map(table_stats_change)),
            )
            .await
            .map(|_| ())
            .map_err(mock_err)
    }

    async fn commit_epoch(
        &self,
        epoch: HummockEpoch,
        sstables: Vec<LocalSstableInfo>,
    ) -> Result<()> {
        let sst_to_worker = sstables
            .iter()
            .map(|LocalSstableInfo { sst_info, .. }| (sst_info.get_object_id(), self.context_id))
            .collect();
        self.hummock_manager
            .commit_epoch(epoch, sstables, sst_to_worker)
            .await
            .map_err(mock_err)?;
        Ok(())
    }

    async fn update_current_epoch(&self, epoch: HummockEpoch) -> Result<()> {
        self.hummock_manager.update_current_epoch(epoch);
        Ok(())
    }

    async fn subscribe_compact_tasks(
        &self,
        _max_concurrent_task_number: u64,
        _cpu_core_num: u32,
    ) -> Result<BoxStream<'static, CompactTaskItem>> {
        let (sched_tx, mut sched_rx) = tokio::sync::mpsc::unbounded_channel();
        let sched_channel = Arc::new(CompactionRequestChannel::new(sched_tx));
        self.hummock_manager
            .init_compaction_scheduler(sched_channel.clone(), None);

        let worker_node = self
            .hummock_manager
            .cluster_manager()
            .add_worker_node(
                WorkerType::Compactor,
                HostAddress {
                    host: "compactor".to_string(),
                    port: 0,
                },
                1,
            )
            .await
            .unwrap();
        let context_id = worker_node.id;
        let _ = self
            .hummock_manager
            .compactor_manager_ref_for_test()
            .add_compactor(context_id, 8, 8);
        self.compact_context_id.store(context_id, Ordering::Release);

        let hummock_manager_compact = self.hummock_manager.clone();
        let (task_tx, task_rx) = tokio::sync::mpsc::unbounded_channel();
        let handle = tokio::spawn(async move {
            while let Some((group, task_type)) = sched_rx.recv().await {
                sched_channel.unschedule(group, task_type);

                let mut selector: Box<dyn LevelSelector> = match task_type {
                    compact_task::TaskType::Dynamic => default_level_selector(),
                    compact_task::TaskType::SpaceReclaim => {
                        Box::<SpaceReclaimCompactionSelector>::default()
                    }

                    _ => panic!("Error type when mock_hummock_meta_client subscribe_compact_tasks"),
                };
                if let Some(task) = hummock_manager_compact
                    .get_compact_task(group, &mut selector)
                    .await
                    .unwrap()
                {
                    hummock_manager_compact
                        .assign_compaction_task(&task, context_id)
                        .await
                        .unwrap();
                    let resp = SubscribeCompactTasksResponse {
                        task: Some(Task::CompactTask(task)),
                    };
                    let _ = task_tx.send(Ok(resp));
                }
            }
        });
        let s = UnboundedReceiverStream::new(task_rx);
        Ok(Box::pin(CompactTaskItemStream {
            inner: s,
            _handle: handle,
        }))
    }

    async fn compactor_heartbeat(
        &self,
        _progress: Vec<CompactTaskProgress>,
        _workload: CompactorWorkload,
    ) -> Result<()> {
        Ok(())
    }

    async fn report_vacuum_task(&self, _vacuum_task: VacuumTask) -> Result<()> {
        Ok(())
    }

    async fn trigger_manual_compaction(
        &self,
        _compaction_group_id: u64,
        _table_id: u32,
        _level: u32,
    ) -> Result<()> {
        todo!()
    }

    async fn report_full_scan_task(&self, _object_ids: Vec<HummockSstableObjectId>) -> Result<()> {
        unimplemented!()
    }

    async fn trigger_full_gc(&self, _sst_retention_time_sec: u64) -> Result<()> {
        unimplemented!()
    }
}

impl MockHummockMetaClient {
    pub fn hummock_manager_ref(&self) -> Arc<HummockManager<MemStore>> {
        self.hummock_manager.clone()
    }
}

pub struct CompactTaskItemStream {
    inner: UnboundedReceiverStream<CompactTaskItem>,
    _handle: JoinHandle<()>,
}

impl Drop for CompactTaskItemStream {
    fn drop(&mut self) {
        self.inner.close();
    }
}

impl Stream for CompactTaskItemStream {
    type Item = CompactTaskItem;

    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        self.inner.poll_next_unpin(cx)
    }
}
