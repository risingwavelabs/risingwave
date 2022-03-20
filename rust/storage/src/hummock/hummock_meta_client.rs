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
//
use std::sync::Arc;

use async_trait::async_trait;
use risingwave_pb::hummock::{
    AddTablesRequest, CompactTask, GetNewTableIdRequest, HummockSnapshot, HummockVersion,
    PinSnapshotRequest, PinVersionRequest, ReportCompactionTasksRequest, ReportVacuumTaskRequest,
    SstableInfo, SubscribeCompactTasksRequest, SubscribeCompactTasksResponse, UnpinSnapshotRequest,
    UnpinVersionRequest, VacuumTask,
};
use risingwave_rpc_client::MetaClient;
use tonic::Streaming;

use crate::hummock::{
    HummockEpoch, HummockError, HummockResult, HummockSSTableId, HummockVersionId,
    TracedHummockError,
};
use crate::monitor::HummockMetrics;

#[derive(Default)]
pub struct RetryableError {}

impl tokio_retry::Condition<TracedHummockError> for RetryableError {
    fn should_retry(&mut self, _error: &TracedHummockError) -> bool {
        // TODO #2745 define retryable error here
        false
    }
}

/// Define the rpc client trait to ease unit test.
#[async_trait]
pub trait HummockMetaClient: Send + Sync + 'static {
    async fn pin_version(&self, last_pinned: HummockVersionId) -> HummockResult<HummockVersion>;
    async fn unpin_version(&self, pinned_version_id: HummockVersionId) -> HummockResult<()>;
    async fn pin_snapshot(&self, last_pinned: HummockEpoch) -> HummockResult<HummockEpoch>;
    async fn unpin_snapshot(&self, pinned_epoch: HummockEpoch) -> HummockResult<()>;
    async fn get_new_table_id(&self) -> HummockResult<HummockSSTableId>;
    async fn add_tables(
        &self,
        epoch: HummockEpoch,
        sstables: Vec<SstableInfo>,
    ) -> HummockResult<HummockVersion>;
    async fn report_compaction_task(
        &self,
        compact_task: CompactTask,
        task_result: bool,
    ) -> HummockResult<()>;
    async fn commit_epoch(&self, epoch: HummockEpoch) -> HummockResult<()>;
    async fn abort_epoch(&self, epoch: HummockEpoch) -> HummockResult<()>;
    async fn subscribe_compact_tasks(
        &self,
    ) -> HummockResult<Streaming<SubscribeCompactTasksResponse>>;
    async fn report_vacuum_task(&self, vacuum_task: VacuumTask) -> HummockResult<()>;
}

pub struct RpcHummockMetaClient {
    meta_client: MetaClient,

    // TODO: should be separated `HummockStats` instead of `StateStoreMetrics`.
    stats: Arc<HummockMetrics>,
}

impl RpcHummockMetaClient {
    pub fn new(meta_client: MetaClient, stats: Arc<HummockMetrics>) -> RpcHummockMetaClient {
        RpcHummockMetaClient { meta_client, stats }
    }
}

#[async_trait]
impl HummockMetaClient for RpcHummockMetaClient {
    async fn pin_version(&self, last_pinned: HummockVersionId) -> HummockResult<HummockVersion> {
        self.stats.pin_version_counts.inc();
        let timer = self.stats.pin_version_latency.start_timer();
        let result = self
            .meta_client
            .inner
            .pin_version(PinVersionRequest {
                context_id: self.meta_client.worker_id(),
                last_pinned,
            })
            .await
            .map_err(HummockError::meta_error)?;
        timer.observe_duration();
        Ok(result.pinned_version.unwrap())
    }

    async fn unpin_version(&self, pinned_version_id: HummockVersionId) -> HummockResult<()> {
        self.stats.unpin_version_counts.inc();
        let timer = self.stats.unpin_version_latency.start_timer();
        self.meta_client
            .inner
            .unpin_version(UnpinVersionRequest {
                context_id: self.meta_client.worker_id(),
                pinned_version_id,
            })
            .await
            .map_err(HummockError::meta_error)?;
        timer.observe_duration();
        Ok(())
    }

    async fn pin_snapshot(&self, last_pinned: HummockEpoch) -> HummockResult<HummockEpoch> {
        self.stats.pin_snapshot_counts.inc();
        let timer = self.stats.pin_snapshot_latency.start_timer();
        let result = self
            .meta_client
            .inner
            .pin_snapshot(PinSnapshotRequest {
                context_id: self.meta_client.worker_id(),
                last_pinned,
            })
            .await
            .map_err(HummockError::meta_error)?;
        timer.observe_duration();
        Ok(result.snapshot.unwrap().epoch)
    }

    async fn unpin_snapshot(&self, pinned_epoch: HummockEpoch) -> HummockResult<()> {
        self.stats.unpin_snapshot_counts.inc();
        let timer = self.stats.unpin_snapshot_latency.start_timer();
        self.meta_client
            .inner
            .unpin_snapshot(UnpinSnapshotRequest {
                context_id: self.meta_client.worker_id(),
                snapshot: Some(HummockSnapshot {
                    epoch: pinned_epoch,
                }),
            })
            .await
            .map_err(HummockError::meta_error)?;
        timer.observe_duration();
        Ok(())
    }

    async fn get_new_table_id(&self) -> HummockResult<HummockSSTableId> {
        self.stats.get_new_table_id_counts.inc();
        let timer = self.stats.get_new_table_id_latency.start_timer();
        let result = self
            .meta_client
            .inner
            .get_new_table_id(GetNewTableIdRequest {})
            .await
            .map_err(HummockError::meta_error)?;
        timer.observe_duration();
        Ok(result.table_id)
    }

    async fn add_tables(
        &self,
        epoch: HummockEpoch,
        sstables: Vec<SstableInfo>,
    ) -> HummockResult<HummockVersion> {
        self.stats.add_tables_counts.inc();
        let timer = self.stats.add_tables_latency.start_timer();
        let resp = self
            .meta_client
            .inner
            .add_tables(AddTablesRequest {
                context_id: self.meta_client.worker_id(),
                tables: sstables,
                epoch,
            })
            .await
            .map_err(HummockError::meta_error)?;
        timer.observe_duration();
        Ok(resp.version.unwrap())
    }

    async fn report_compaction_task(
        &self,
        compact_task: CompactTask,
        task_result: bool,
    ) -> HummockResult<()> {
        self.stats.report_compaction_task_counts.inc();
        let timer = self.stats.report_compaction_task_latency.start_timer();
        self.meta_client
            .inner
            .report_compaction_tasks(ReportCompactionTasksRequest {
                compact_task: Some(compact_task),
                task_result,
            })
            .await
            .map_err(HummockError::meta_error)?;
        timer.observe_duration();
        Ok(())
    }

    async fn commit_epoch(&self, _epoch: HummockEpoch) -> HummockResult<()> {
        unimplemented!()
    }

    async fn abort_epoch(&self, _epoch: HummockEpoch) -> HummockResult<()> {
        unimplemented!()
    }

    async fn subscribe_compact_tasks(
        &self,
    ) -> HummockResult<Streaming<SubscribeCompactTasksResponse>> {
        let stream = self
            .meta_client
            .to_owned()
            .inner
            .subscribe_compact_tasks(SubscribeCompactTasksRequest {})
            .await
            .map_err(HummockError::meta_error)?;
        Ok(stream)
    }

    async fn report_vacuum_task(&self, vacuum_task: VacuumTask) -> HummockResult<()> {
        self.meta_client
            .to_owned()
            .inner
            .report_vacuum_task(ReportVacuumTaskRequest {
                vacuum_task: Some(vacuum_task),
            })
            .await
            .map_err(HummockError::meta_error)?;
        Ok(())
    }
}
