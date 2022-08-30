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
use risingwave_hummock_sdk::{
    HummockEpoch, HummockSstableId, HummockVersionId, LocalSstableInfo, SstIdRange,
};
use risingwave_pb::hummock::{
    pin_version_response, CompactTask, CompactionGroup, HummockSnapshot,
    SubscribeCompactTasksResponse, VacuumTask,
};
use tonic::Streaming;

use crate::error::Result;

#[async_trait]
pub trait HummockMetaClient: Send + Sync + 'static {
    async fn pin_version(
        &self,
        last_pinned: HummockVersionId,
    ) -> Result<pin_version_response::Payload>;
    async fn unpin_version(&self) -> Result<()>;
    async fn unpin_version_before(&self, unpin_version_before: HummockVersionId) -> Result<()>;
    async fn pin_snapshot(&self) -> Result<HummockSnapshot>;
    async fn unpin_snapshot(&self) -> Result<()>;
    async fn unpin_snapshot_before(&self, pinned_epochs: HummockEpoch) -> Result<()>;
    async fn get_epoch(&self) -> Result<HummockSnapshot>;
    async fn get_new_sst_ids(&self, number: u32) -> Result<SstIdRange>;
    async fn report_compaction_task(&self, compact_task: CompactTask) -> Result<()>;
    // We keep `commit_epoch` only for test/benchmark like ssbench.
    async fn commit_epoch(
        &self,
        epoch: HummockEpoch,
        sstables: Vec<LocalSstableInfo>,
    ) -> Result<()>;
    async fn subscribe_compact_tasks(
        &self,
        max_concurrent_task_number: u64,
    ) -> Result<Streaming<SubscribeCompactTasksResponse>>;
    async fn report_vacuum_task(&self, vacuum_task: VacuumTask) -> Result<()>;
    async fn get_compaction_groups(&self) -> Result<Vec<CompactionGroup>>;
    async fn trigger_manual_compaction(
        &self,
        compaction_group_id: u64,
        table_id: u32,
        level: u32,
    ) -> Result<()>;
    async fn report_full_scan_task(&self, sst_ids: Vec<HummockSstableId>) -> Result<()>;
}
