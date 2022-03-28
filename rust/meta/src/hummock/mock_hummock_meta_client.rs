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
use risingwave_pb::hummock::{
    CompactTask, HummockSnapshot, HummockVersion, SstableInfo, SubscribeCompactTasksResponse,
    VacuumTask,
};
use risingwave_storage::hummock::hummock_meta_client::HummockMetaClient;
use risingwave_storage::hummock::{
    HummockContextId, HummockEpoch, HummockError, HummockResult, HummockSSTableId, HummockVersionId,
};
use tonic::Streaming;

use crate::hummock::HummockManager;
use crate::storage::MemStore;

pub(crate) struct MockHummockMetaClient {
    hummock_manager: Arc<HummockManager<MemStore>>,
    context_id: HummockContextId,
}

impl MockHummockMetaClient {
    #[allow(dead_code)]
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
    async fn pin_version(&self, last_pinned: HummockVersionId) -> HummockResult<HummockVersion> {
        self.hummock_manager
            .pin_version(self.context_id, last_pinned)
            .await
            .map_err(HummockError::meta_error)
    }

    async fn unpin_version(&self, pinned_version_id: HummockVersionId) -> HummockResult<()> {
        self.hummock_manager
            .unpin_version(self.context_id, pinned_version_id)
            .await
            .map_err(HummockError::meta_error)
    }

    async fn pin_snapshot(&self, last_pinned: HummockEpoch) -> HummockResult<HummockEpoch> {
        self.hummock_manager
            .pin_snapshot(self.context_id, last_pinned)
            .await
            .map(|e| e.epoch)
            .map_err(HummockError::meta_error)
    }

    async fn unpin_snapshot(&self, pinned_epoch: HummockEpoch) -> HummockResult<()> {
        self.hummock_manager
            .unpin_snapshot(
                self.context_id,
                HummockSnapshot {
                    epoch: pinned_epoch,
                },
            )
            .await
            .map_err(HummockError::meta_error)
    }

    async fn get_new_table_id(&self) -> HummockResult<HummockSSTableId> {
        self.hummock_manager
            .get_new_table_id()
            .await
            .map_err(HummockError::meta_error)
    }

    async fn add_tables(
        &self,
        epoch: HummockEpoch,
        sstables: Vec<SstableInfo>,
    ) -> HummockResult<HummockVersion> {
        self.hummock_manager
            .add_tables(self.context_id, sstables, epoch)
            .await
            .map_err(HummockError::meta_error)
    }

    async fn report_compaction_task(
        &self,
        compact_task: CompactTask,
        task_result: bool,
    ) -> HummockResult<()> {
        self.hummock_manager
            .report_compact_task(compact_task, task_result)
            .await
            .map_err(HummockError::meta_error)
            .map(|_| ())
    }

    async fn commit_epoch(&self, epoch: HummockEpoch) -> HummockResult<()> {
        self.hummock_manager
            .commit_epoch(epoch)
            .await
            .map_err(HummockError::meta_error)
    }

    async fn abort_epoch(&self, epoch: HummockEpoch) -> HummockResult<()> {
        self.hummock_manager
            .abort_epoch(epoch)
            .await
            .map_err(HummockError::meta_error)
    }

    async fn subscribe_compact_tasks(
        &self,
    ) -> HummockResult<Streaming<SubscribeCompactTasksResponse>> {
        unimplemented!()
    }

    async fn report_vacuum_task(&self, _vacuum_task: VacuumTask) -> HummockResult<()> {
        unimplemented!()
    }
}

impl MockHummockMetaClient {
    #[allow(dead_code)]
    pub fn hummock_manager_ref(&self) -> Arc<HummockManager<MemStore>> {
        self.hummock_manager.clone()
    }
}
