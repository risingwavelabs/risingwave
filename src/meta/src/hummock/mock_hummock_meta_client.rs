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

use async_trait::async_trait;
use risingwave_common::error::Result;
use risingwave_hummock_sdk::{HummockContextId, HummockEpoch, HummockSSTableId, HummockVersionId};
use risingwave_pb::hummock::{
    CompactTask, HummockSnapshot, HummockVersion, SstableInfo, SubscribeCompactTasksResponse,
    VacuumTask,
};
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
}

#[async_trait]
impl HummockMetaClient for MockHummockMetaClient {
    async fn pin_version(&self, last_pinned: HummockVersionId) -> Result<HummockVersion> {
        self.hummock_manager
            .pin_version(self.context_id, last_pinned)
            .await
    }

    async fn unpin_version(&self, pinned_version_id: &[HummockVersionId]) -> Result<()> {
        self.hummock_manager
            .unpin_version(self.context_id, pinned_version_id)
            .await
    }

    async fn pin_snapshot(&self, last_pinned: HummockEpoch) -> Result<HummockEpoch> {
        self.hummock_manager
            .pin_snapshot(self.context_id, last_pinned)
            .await
            .map(|e| e.epoch)
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
    }

    async fn get_new_table_id(&self) -> Result<HummockSSTableId> {
        self.hummock_manager.get_new_table_id().await
    }

    async fn add_tables(
        &self,
        epoch: HummockEpoch,
        sstables: Vec<SstableInfo>,
    ) -> Result<HummockVersion> {
        self.hummock_manager
            .add_tables(self.context_id, sstables, epoch)
            .await
    }

    async fn report_compaction_task(&self, compact_task: CompactTask) -> Result<()> {
        self.hummock_manager
            .report_compact_task(compact_task)
            .await
            .map(|_| ())
    }

    async fn commit_epoch(&self, epoch: HummockEpoch) -> Result<()> {
        self.hummock_manager.commit_epoch(epoch).await
    }

    async fn abort_epoch(&self, epoch: HummockEpoch) -> Result<()> {
        self.hummock_manager.abort_epoch(epoch).await
    }

    async fn subscribe_compact_tasks(&self) -> Result<Streaming<SubscribeCompactTasksResponse>> {
        unimplemented!()
    }

    async fn report_vacuum_task(&self, _vacuum_task: VacuumTask) -> Result<()> {
        Ok(())
    }
}

impl MockHummockMetaClient {
    pub fn hummock_manager_ref(&self) -> Arc<HummockManager<MemStore>> {
        self.hummock_manager.clone()
    }
}
