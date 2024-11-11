// Copyright 2024 RisingWave Labs
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

use async_trait::async_trait;
use futures::stream::BoxStream;
use risingwave_hummock_sdk::version::HummockVersion;
use risingwave_hummock_sdk::{HummockEpoch, HummockVersionId, SstObjectIdRange, SyncResult};
use risingwave_pb::hummock::{
    PbHummockVersion, SubscribeCompactionEventRequest, SubscribeCompactionEventResponse,
};
use tokio::sync::mpsc::UnboundedSender;

pub type CompactionEventItem = std::result::Result<SubscribeCompactionEventResponse, tonic::Status>;

use crate::error::Result;

#[async_trait]
pub trait HummockMetaClient: Send + Sync + 'static {
    async fn unpin_version_before(&self, unpin_version_before: HummockVersionId) -> Result<()>;
    async fn get_current_version(&self) -> Result<HummockVersion>;
    async fn get_new_sst_ids(&self, number: u32) -> Result<SstObjectIdRange>;
    // We keep `commit_epoch` only for test/benchmark.
    async fn commit_epoch(
        &self,
        epoch: HummockEpoch,
        sync_result: SyncResult,
        is_log_store: bool,
    ) -> Result<()>;
    async fn trigger_manual_compaction(
        &self,
        compaction_group_id: u64,
        table_id: u32,
        level: u32,
        sst_ids: Vec<u64>,
    ) -> Result<()>;
    async fn trigger_full_gc(
        &self,
        sst_retention_time_sec: u64,
        prefix: Option<String>,
    ) -> Result<()>;

    async fn subscribe_compaction_event(
        &self,
    ) -> Result<(
        UnboundedSender<SubscribeCompactionEventRequest>,
        BoxStream<'static, CompactionEventItem>,
    )>;

    async fn get_version_by_epoch(
        &self,
        epoch: HummockEpoch,
        table_id: u32,
    ) -> Result<PbHummockVersion>;
}
