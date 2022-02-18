use std::sync::Arc;

use async_trait::async_trait;
use risingwave_pb::hummock::{
    AddTablesRequest, CompactTask, GetCompactionTasksRequest, GetNewTableIdRequest,
    HummockSnapshot, HummockVersion, PinSnapshotRequest, PinVersionRequest,
    ReportCompactionTasksRequest, SstableInfo, UnpinSnapshotRequest, UnpinVersionRequest,
};
use risingwave_rpc_client::MetaClient;

use crate::hummock::{
    HummockEpoch, HummockError, HummockResult, HummockSSTableId, HummockVersionId,
    TracedHummockError,
};
use crate::monitor::{StateStoreStats, DEFAULT_STATE_STORE_STATS};

pub const GLOBAL_COLUMN_FAMILY_ID: &str = "global";

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
    async fn pin_version(&self) -> HummockResult<HummockVersion>;
    async fn unpin_version(&self, _pinned_version_id: HummockVersionId) -> HummockResult<()>;
    async fn pin_snapshot(&self) -> HummockResult<HummockEpoch>;
    async fn unpin_snapshot(&self, _pinned_epoch: HummockEpoch) -> HummockResult<()>;
    async fn get_new_table_id(&self) -> HummockResult<HummockSSTableId>;
    async fn add_tables(
        &self,
        epoch: HummockEpoch,
        sstables: Vec<SstableInfo>,
    ) -> HummockResult<HummockVersion>;
    async fn get_compaction_task(&self) -> HummockResult<Option<CompactTask>>;
    async fn report_compaction_task(
        &self,
        compact_task: CompactTask,
        task_result: bool,
    ) -> HummockResult<()>;
    async fn commit_epoch(&self, epoch: HummockEpoch) -> HummockResult<()>;
    async fn abort_epoch(&self, epoch: HummockEpoch) -> HummockResult<()>;
}

pub struct RPCHummockMetaClient {
    meta_client: MetaClient,
    stats: Arc<StateStoreStats>,
}

impl RPCHummockMetaClient {
    pub fn new(meta_client: MetaClient) -> RPCHummockMetaClient {
        RPCHummockMetaClient {
            meta_client,
            stats: DEFAULT_STATE_STORE_STATS.clone(),
        }
    }
}

// TODO #93 idempotent retry
#[async_trait]
impl HummockMetaClient for RPCHummockMetaClient {
    async fn pin_version(&self) -> HummockResult<HummockVersion> {
        self.stats.pin_version_counts.inc();
        let timer = self.stats.pin_version_latency.start_timer();
        let result = self
            .meta_client
            .to_owned()
            .hummock_client
            .pin_version(PinVersionRequest {
                column_family: String::from(GLOBAL_COLUMN_FAMILY_ID),
                context_id: self.meta_client.worker_id(),
            })
            .await
            .map_err(HummockError::meta_error)?
            .into_inner();
        timer.observe_duration();
        Ok(result.pinned_version.unwrap())
    }

    async fn unpin_version(&self, pinned_version_id: HummockVersionId) -> HummockResult<()> {
        self.stats.unpin_version_counts.inc();
        let timer = self.stats.unpin_version_latency.start_timer();
        self.meta_client
            .to_owned()
            .hummock_client
            .unpin_version(UnpinVersionRequest {
                column_family: String::from(GLOBAL_COLUMN_FAMILY_ID),
                context_id: self.meta_client.worker_id(),
                pinned_version_id,
            })
            .await
            .map_err(HummockError::meta_error)?;
        timer.observe_duration();
        Ok(())
    }

    async fn pin_snapshot(&self) -> HummockResult<HummockEpoch> {
        self.stats.pin_snapshot_counts.inc();
        let timer = self.stats.pin_snapshot_latency.start_timer();
        let result = self
            .meta_client
            .to_owned()
            .hummock_client
            .pin_snapshot(PinSnapshotRequest {
                column_family: String::from(GLOBAL_COLUMN_FAMILY_ID),
                context_id: self.meta_client.worker_id(),
            })
            .await
            .map_err(HummockError::meta_error)?
            .into_inner();
        timer.observe_duration();
        Ok(result.snapshot.unwrap().epoch)
    }

    async fn unpin_snapshot(&self, pinned_epoch: HummockEpoch) -> HummockResult<()> {
        self.stats.unpin_snapshot_counts.inc();
        let timer = self.stats.unpin_snapshot_latency.start_timer();
        self.meta_client
            .to_owned()
            .hummock_client
            .unpin_snapshot(UnpinSnapshotRequest {
                column_family: String::from(GLOBAL_COLUMN_FAMILY_ID),
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
            .to_owned()
            .hummock_client
            .get_new_table_id(GetNewTableIdRequest {})
            .await
            .map_err(HummockError::meta_error)?
            .into_inner();
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
            .to_owned()
            .hummock_client
            .add_tables(AddTablesRequest {
                column_family: String::from(GLOBAL_COLUMN_FAMILY_ID),
                context_id: self.meta_client.worker_id(),
                sstable_info: sstables,
                epoch,
            })
            .await
            .map_err(HummockError::meta_error)?;
        timer.observe_duration();
        Ok(resp.into_inner().version.unwrap())
    }

    async fn get_compaction_task(&self) -> HummockResult<Option<CompactTask>> {
        self.stats.get_compaction_task_counts.inc();
        let timer = self.stats.get_compaction_task_latency.start_timer();
        let result = self
            .meta_client
            .to_owned()
            .hummock_client
            .get_compaction_tasks(GetCompactionTasksRequest {
                column_family: String::from(GLOBAL_COLUMN_FAMILY_ID),
                context_id: self.meta_client.worker_id(),
            })
            .await
            .map_err(HummockError::meta_error)?
            .into_inner();
        timer.observe_duration();
        Ok(result.compact_task)
    }

    async fn report_compaction_task(
        &self,
        compact_task: CompactTask,
        task_result: bool,
    ) -> HummockResult<()> {
        self.stats.report_compaction_task_counts.inc();
        let timer = self.stats.report_compaction_task_latency.start_timer();
        self.meta_client
            .to_owned()
            .hummock_client
            .report_compaction_tasks(ReportCompactionTasksRequest {
                column_family: String::from(GLOBAL_COLUMN_FAMILY_ID),
                context_id: self.meta_client.worker_id(),
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
}
