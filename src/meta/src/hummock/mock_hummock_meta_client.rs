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
use risingwave_pb::hummock::{
    compact_task, CompactTask, CompactTaskProgress, CompactorWorkload, HummockSnapshot,
    HummockVersion, SubscribeCompactionEventRequest, SubscribeCompactionEventResponse, VacuumTask,
};
use risingwave_rpc_client::error::{Result, RpcError};
use risingwave_rpc_client::HummockMetaClient;
use tokio::sync::mpsc::UnboundedSender;
use tokio::task::JoinHandle;
use tokio_stream::wrappers::UnboundedReceiverStream;
use tonic::Streaming;

use crate::hummock::compaction::{
    default_level_selector, LevelSelector, SpaceReclaimCompactionSelector,
};
use crate::hummock::HummockManager;
use crate::storage::MemStore;

pub struct MockHummockMetaClient {
    hummock_manager: Arc<HummockManager<MemStore>>,
    context_id: HummockContextId,
    compact_context_id: AtomicU32,
    // used for hummock replay to avoid collision with existing sst files
    sst_offset: u64,
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
            sst_offset: 0,
        }
    }

    pub fn with_sst_offset(
        hummock_manager: Arc<HummockManager<MemStore>>,
        context_id: HummockContextId,
        sst_offset: u64,
    ) -> Self {
        Self {
            hummock_manager,
            context_id,
            compact_context_id: AtomicU32::new(context_id),
            sst_offset,
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
            .map(|range| SstObjectIdRange {
                start_id: range.start_id + self.sst_offset,
                end_id: range.end_id + self.sst_offset,
            })
    }

    async fn report_compaction_task(
        &self,
        mut compact_task: CompactTask,
        table_stats_change: TableStatsMap,
    ) -> Result<()> {
        self.hummock_manager
            .report_compact_task(
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

    async fn compactor_heartbeat(
        &self,
        _progress: Vec<CompactTaskProgress>,
        _workload: CompactorWorkload,
        _pull_task_count: Option<u32>,
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

    async fn subscribe_compaction_event(
        &self,
    ) -> Result<(
        UnboundedSender<SubscribeCompactionEventRequest>,
        Streaming<SubscribeCompactionEventResponse>,
    )> {
        todo!();
    }
}

impl MockHummockMetaClient {
    pub fn hummock_manager_ref(&self) -> Arc<HummockManager<MemStore>> {
        self.hummock_manager.clone()
    }
}