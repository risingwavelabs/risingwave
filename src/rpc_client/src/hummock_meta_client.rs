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

use async_trait::async_trait;
use risingwave_common::error::Result;
use risingwave_common::storage::{HummockEpoch, HummockSSTableId, HummockVersionId};
use risingwave_pb::hummock::{
    CompactTask, HummockVersion, SstableInfo, SubscribeCompactTasksResponse, VacuumTask,
};
use tonic::Streaming;

#[async_trait]
pub trait HummockMetaClient: Send + Sync + 'static {
    async fn pin_version(&self, last_pinned: HummockVersionId) -> Result<HummockVersion>;
    async fn unpin_version(&self, pinned_version_ids: &[HummockVersionId]) -> Result<()>;
    async fn pin_snapshot(&self, last_pinned: HummockEpoch) -> Result<HummockEpoch>;
    async fn unpin_snapshot(&self, pinned_epochs: &[HummockEpoch]) -> Result<()>;
    async fn get_new_table_id(&self) -> Result<HummockSSTableId>;
    async fn add_tables(
        &self,
        epoch: HummockEpoch,
        sstables: Vec<SstableInfo>,
    ) -> Result<HummockVersion>;
    async fn report_compaction_task(&self, compact_task: CompactTask) -> Result<()>;
    async fn commit_epoch(&self, epoch: HummockEpoch) -> Result<()>;
    async fn abort_epoch(&self, epoch: HummockEpoch) -> Result<()>;
    async fn subscribe_compact_tasks(&self) -> Result<Streaming<SubscribeCompactTasksResponse>>;
    async fn report_vacuum_task(&self, vacuum_task: VacuumTask) -> Result<()>;
}
