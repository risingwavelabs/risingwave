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
use risingwave_pb::hummock::{
    CompactTask, HummockVersion, SstableInfo, SubscribeCompactTasksResponse, VacuumTask,
};
use risingwave_rpc_client::{HummockMetaClient, MetaClient};
use tonic::Streaming;

use crate::hummock::{HummockEpoch, HummockError, HummockSSTableId, HummockVersionId};
use crate::monitor::HummockMetrics;

#[derive(Default)]
pub struct RetryableError {}

impl tokio_retry::Condition<HummockError> for RetryableError {
    fn should_retry(&mut self, _error: &HummockError) -> bool {
        // TODO #2745 define retryable error here
        false
    }
}

pub struct MonitoredHummockMetaClient {
    meta_client: MetaClient,

    stats: Arc<HummockMetrics>,
}

impl MonitoredHummockMetaClient {
    pub fn new(meta_client: MetaClient, stats: Arc<HummockMetrics>) -> MonitoredHummockMetaClient {
        MonitoredHummockMetaClient { meta_client, stats }
    }
}

#[async_trait]
impl HummockMetaClient for MonitoredHummockMetaClient {
    async fn pin_version(&self, last_pinned: HummockVersionId) -> Result<HummockVersion> {
        self.stats.pin_version_counts.inc();
        let timer = self.stats.pin_version_latency.start_timer();
        let res = self.meta_client.pin_version(last_pinned).await;
        timer.observe_duration();
        res
    }

    async fn unpin_version(&self, pinned_version_ids: &[HummockVersionId]) -> Result<()> {
        self.stats.unpin_version_counts.inc();
        let timer = self.stats.unpin_version_latency.start_timer();
        let res = self.meta_client.unpin_version(pinned_version_ids).await;
        timer.observe_duration();
        res
    }

    async fn pin_snapshot(&self, last_pinned: HummockEpoch) -> Result<HummockEpoch> {
        self.stats.pin_snapshot_counts.inc();
        let timer = self.stats.pin_snapshot_latency.start_timer();
        let res = self.meta_client.pin_snapshot(last_pinned).await;
        timer.observe_duration();
        res
    }

    async fn unpin_snapshot(&self, pinned_epochs: &[HummockEpoch]) -> Result<()> {
        self.stats.unpin_snapshot_counts.inc();
        let timer = self.stats.unpin_snapshot_latency.start_timer();
        let res = self.meta_client.unpin_snapshot(pinned_epochs).await;
        timer.observe_duration();
        res
    }

    async fn get_new_table_id(&self) -> Result<HummockSSTableId> {
        self.stats.get_new_table_id_counts.inc();
        let timer = self.stats.get_new_table_id_latency.start_timer();
        let res = self.meta_client.get_new_table_id().await;
        timer.observe_duration();
        res
    }

    async fn add_tables(
        &self,
        epoch: HummockEpoch,
        sstables: Vec<SstableInfo>,
    ) -> Result<HummockVersion> {
        self.stats.add_tables_counts.inc();
        let timer = self.stats.add_tables_latency.start_timer();
        let res = self.meta_client.add_tables(epoch, sstables).await;
        timer.observe_duration();
        res
    }

    async fn report_compaction_task(&self, compact_task: CompactTask) -> Result<()> {
        self.stats.report_compaction_task_counts.inc();
        let timer = self.stats.report_compaction_task_latency.start_timer();
        let res = self.meta_client.report_compaction_task(compact_task).await;
        timer.observe_duration();
        res
    }

    async fn commit_epoch(&self, _epoch: HummockEpoch) -> Result<()> {
        unimplemented!()
    }

    async fn abort_epoch(&self, _epoch: HummockEpoch) -> Result<()> {
        unimplemented!()
    }

    async fn subscribe_compact_tasks(&self) -> Result<Streaming<SubscribeCompactTasksResponse>> {
        self.meta_client.subscribe_compact_tasks().await
    }

    async fn report_vacuum_task(&self, vacuum_task: VacuumTask) -> Result<()> {
        self.meta_client.report_vacuum_task(vacuum_task).await
    }
}
