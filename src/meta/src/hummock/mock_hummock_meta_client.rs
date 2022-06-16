// Copyright 2022 Singularity Data
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::sync::Arc;

use anyhow::anyhow;
use async_trait::async_trait;
use risingwave_hummock_sdk::compaction_group::StaticCompactionGroupId;
use risingwave_hummock_sdk::{HummockContextId, HummockEpoch, HummockSSTableId, HummockVersionId};
use risingwave_pb::hummock::{
    CompactTask, CompactionGroup, HummockSnapshot, HummockVersion, SstableInfo,
    SubscribeCompactTasksResponse, VacuumTask,
};
use risingwave_rpc_client::error::{Result, RpcError};
use risingwave_rpc_client::HummockMetaClient;
use tonic::Streaming;

use crate::hummock::HummockManager;
use crate::storage::MemStore;

pub struct MockHummockMetaClient {
    hummock_manager: Arc<HummockManager<MemStore>>,
    context_id: HummockContextId,
}

impl MockHummockMetaClient {
    pub fn new(
        hummock_manager: Arc<HummockManager<MemStore>>,
        context_id: HummockContextId,
    ) -> MockHummockMetaClient {
        MockHummockMetaClient {
            hummock_manager,
            context_id,
        }
    }

    pub async fn get_compact_task(&self) -> Option<CompactTask> {
        self.hummock_manager
            .get_compact_task(StaticCompactionGroupId::StateDefault.into())
            .await
            .unwrap_or(None)
    }
}

fn mock_err(error: super::error::Error) -> RpcError {
    anyhow!("mock error: {}", error).into()
}

#[async_trait]
impl HummockMetaClient for MockHummockMetaClient {
    async fn pin_version(&self, last_pinned: HummockVersionId) -> Result<HummockVersion> {
        self.hummock_manager
            .pin_version(self.context_id, last_pinned)
            .await
            .map_err(mock_err)
    }

    async fn unpin_version(&self, pinned_version_id: &[HummockVersionId]) -> Result<()> {
        self.hummock_manager
            .unpin_version(self.context_id, pinned_version_id)
            .await
            .map_err(mock_err)
    }

    async fn pin_snapshot(&self, last_pinned: HummockEpoch) -> Result<HummockEpoch> {
        self.hummock_manager
            .pin_snapshot(self.context_id, last_pinned)
            .await
            .map(|e| e.epoch)
            .map_err(mock_err)
    }

    async fn unpin_snapshot(&self, pinned_epochs: &[HummockEpoch]) -> Result<()> {
        let snapshots: Vec<HummockSnapshot> = pinned_epochs
            .iter()
            .map(|epoch| HummockSnapshot {
                epoch: epoch.to_owned(),
            })
            .collect();
        self.hummock_manager
            .unpin_snapshot(self.context_id, snapshots)
            .await
            .map_err(mock_err)
    }

    async fn unpin_snapshot_before(&self, pinned_epochs: HummockEpoch) -> Result<()> {
        self.hummock_manager
            .unpin_snapshot_before(
                self.context_id,
                HummockSnapshot {
                    epoch: pinned_epochs,
                },
            )
            .await
            .map_err(mock_err)
    }

    async fn get_new_table_id(&self) -> Result<HummockSSTableId> {
        self.hummock_manager
            .get_new_table_id()
            .await
            .map_err(mock_err)
    }

    async fn report_compaction_task(&self, compact_task: CompactTask) -> Result<()> {
        self.hummock_manager
            .report_compact_task(&compact_task)
            .await
            .map(|_| ())
            .map_err(mock_err)
    }

    async fn commit_epoch(&self, epoch: HummockEpoch, sstables: Vec<SstableInfo>) -> Result<()> {
        self.hummock_manager
            .commit_epoch(epoch, sstables)
            .await
            .map_err(mock_err)
    }

    async fn subscribe_compact_tasks(&self) -> Result<Streaming<SubscribeCompactTasksResponse>> {
        unimplemented!()
    }

    async fn report_vacuum_task(&self, _vacuum_task: VacuumTask) -> Result<()> {
        Ok(())
    }

    async fn get_compaction_groups(&self) -> Result<Vec<CompactionGroup>> {
        todo!()
    }

    async fn trigger_manual_compaction(&self, _compaction_group_id: u64) -> Result<()> {
        todo!()
    }
}

impl MockHummockMetaClient {
    pub fn hummock_manager_ref(&self) -> Arc<HummockManager<MemStore>> {
        self.hummock_manager.clone()
    }
}
