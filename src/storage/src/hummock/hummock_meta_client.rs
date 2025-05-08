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

use std::sync::Arc;

use async_trait::async_trait;
use futures::stream::BoxStream;
use risingwave_hummock_sdk::version::HummockVersion;
use risingwave_hummock_sdk::{SstObjectIdRange, SyncResult};
use risingwave_pb::hummock::{
    PbHummockVersion, SubscribeCompactionEventRequest, SubscribeIcebergCompactionEventRequest,
};
use risingwave_rpc_client::error::Result;
use risingwave_rpc_client::{
    CompactionEventItem, HummockMetaClient, HummockMetaClientChangeLogInfo,
    IcebergCompactionEventItem, MetaClient,
};
use tokio::sync::mpsc::UnboundedSender;

use crate::hummock::{HummockEpoch, HummockVersionId};
use crate::monitor::HummockMetrics;

pub struct MonitoredHummockMetaClient {
    meta_client: MetaClient,

    stats: Arc<HummockMetrics>,
}

impl MonitoredHummockMetaClient {
    pub fn new(meta_client: MetaClient, stats: Arc<HummockMetrics>) -> MonitoredHummockMetaClient {
        MonitoredHummockMetaClient { meta_client, stats }
    }

    pub fn get_inner(&self) -> &MetaClient {
        &self.meta_client
    }
}

#[async_trait]
impl HummockMetaClient for MonitoredHummockMetaClient {
    async fn unpin_version_before(&self, unpin_version_before: HummockVersionId) -> Result<()> {
        self.meta_client
            .unpin_version_before(unpin_version_before)
            .await
    }

    async fn get_current_version(&self) -> Result<HummockVersion> {
        self.meta_client.get_current_version().await
    }

    async fn get_new_sst_ids(&self, number: u32) -> Result<SstObjectIdRange> {
        self.stats.get_new_sst_ids_counts.inc();
        let timer = self.stats.get_new_sst_ids_latency.start_timer();
        let res = self.meta_client.get_new_sst_ids(number).await;
        timer.observe_duration();
        res
    }

    async fn commit_epoch_with_change_log(
        &self,
        _epoch: HummockEpoch,
        _sync_result: SyncResult,
        _change_log_info: Option<HummockMetaClientChangeLogInfo>,
    ) -> Result<()> {
        panic!("Only meta service can commit_epoch in production.")
    }

    async fn trigger_manual_compaction(
        &self,
        compaction_group_id: u64,
        table_id: u32,
        level: u32,
        sst_ids: Vec<u64>,
    ) -> Result<()> {
        self.meta_client
            .trigger_manual_compaction(compaction_group_id, table_id, level, sst_ids)
            .await
    }

    async fn trigger_full_gc(
        &self,
        sst_retention_time_sec: u64,
        prefix: Option<String>,
    ) -> Result<()> {
        self.meta_client
            .trigger_full_gc(sst_retention_time_sec, prefix)
            .await
    }

    async fn subscribe_compaction_event(
        &self,
    ) -> Result<(
        UnboundedSender<SubscribeCompactionEventRequest>,
        BoxStream<'static, CompactionEventItem>,
    )> {
        self.meta_client.subscribe_compaction_event().await
    }

    async fn get_version_by_epoch(
        &self,
        epoch: HummockEpoch,
        table_id: u32,
    ) -> Result<PbHummockVersion> {
        self.meta_client.get_version_by_epoch(epoch, table_id).await
    }

    async fn subscribe_iceberg_compaction_event(
        &self,
    ) -> Result<(
        UnboundedSender<SubscribeIcebergCompactionEventRequest>,
        BoxStream<'static, IcebergCompactionEventItem>,
    )> {
        self.meta_client.subscribe_iceberg_compaction_event().await
    }
}
