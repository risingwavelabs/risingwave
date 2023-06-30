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

use std::sync::Arc;

use async_trait::async_trait;
use futures::stream::BoxStream;
use risingwave_hummock_sdk::table_stats::TableStatsMap;
use risingwave_hummock_sdk::{HummockSstableObjectId, LocalSstableInfo, SstObjectIdRange};
use risingwave_pb::hummock::{
    CompactTask, CompactTaskProgress, CompactorWorkload, HummockSnapshot, HummockVersion,
    VacuumTask,
};
use risingwave_rpc_client::error::Result;
use risingwave_rpc_client::{CompactTaskItem, HummockMetaClient, MetaClient};

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
        self.stats.unpin_version_before_counts.inc();
        let timer = self.stats.unpin_version_before_latency.start_timer();
        let res = self
            .meta_client
            .unpin_version_before(unpin_version_before)
            .await;
        timer.observe_duration();
        res
    }

    async fn get_current_version(&self) -> Result<HummockVersion> {
        self.meta_client.get_current_version().await
    }

    async fn pin_snapshot(&self) -> Result<HummockSnapshot> {
        self.stats.pin_snapshot_counts.inc();
        let timer = self.stats.pin_snapshot_latency.start_timer();
        let res = self.meta_client.pin_snapshot().await;
        timer.observe_duration();
        res
    }

    async fn get_epoch(&self) -> Result<HummockSnapshot> {
        self.stats.pin_snapshot_counts.inc();
        let timer = self.stats.pin_snapshot_latency.start_timer();
        let res = self.meta_client.get_epoch().await;
        timer.observe_duration();
        res
    }

    async fn unpin_snapshot(&self) -> Result<()> {
        self.stats.unpin_snapshot_counts.inc();
        let timer = self.stats.unpin_snapshot_latency.start_timer();
        let res = self.meta_client.unpin_snapshot().await;
        timer.observe_duration();
        res
    }

    async fn unpin_snapshot_before(&self, _min_epoch: HummockEpoch) -> Result<()> {
        unreachable!("Currently CNs should not call this function")
    }

    async fn get_new_sst_ids(&self, number: u32) -> Result<SstObjectIdRange> {
        self.stats.get_new_sst_ids_counts.inc();
        let timer = self.stats.get_new_sst_ids_latency.start_timer();
        let res = self.meta_client.get_new_sst_ids(number).await;
        timer.observe_duration();
        res
    }

    async fn report_compaction_task(
        &self,
        compact_task: CompactTask,
        table_stats_change: TableStatsMap,
    ) -> Result<()> {
        self.stats.report_compaction_task_counts.inc();
        let timer = self.stats.report_compaction_task_latency.start_timer();
        let res = self
            .meta_client
            .report_compaction_task(compact_task, table_stats_change)
            .await;
        timer.observe_duration();
        res
    }

    async fn commit_epoch(
        &self,
        _epoch: HummockEpoch,
        _sstables: Vec<LocalSstableInfo>,
    ) -> Result<()> {
        panic!("Only meta service can commit_epoch in production.")
    }

    async fn subscribe_compact_tasks(
        &self,
        cpu_core_num: u32,
    ) -> Result<BoxStream<'static, CompactTaskItem>> {
        self.meta_client.subscribe_compact_tasks(cpu_core_num).await
    }

    async fn compactor_heartbeat(
        &self,
        progress: Vec<CompactTaskProgress>,
        workload: CompactorWorkload,
        pull_task_count: Option<u32>,
    ) -> Result<()> {
        self.meta_client
            .compactor_heartbeat(progress, workload, pull_task_count)
            .await
    }

    async fn report_vacuum_task(&self, vacuum_task: VacuumTask) -> Result<()> {
        self.meta_client.report_vacuum_task(vacuum_task).await
    }

    async fn trigger_manual_compaction(
        &self,
        compaction_group_id: u64,
        table_id: u32,
        level: u32,
    ) -> Result<()> {
        self.meta_client
            .trigger_manual_compaction(compaction_group_id, table_id, level)
            .await
    }

    async fn report_full_scan_task(&self, object_ids: Vec<HummockSstableObjectId>) -> Result<()> {
        self.meta_client.report_full_scan_task(object_ids).await
    }

    async fn trigger_full_gc(&self, sst_retention_time_sec: u64) -> Result<()> {
        self.meta_client
            .trigger_full_gc(sst_retention_time_sec)
            .await
    }

    async fn update_current_epoch(&self, epoch: HummockEpoch) -> Result<()> {
        self.meta_client.update_current_epoch(epoch).await
    }
}
