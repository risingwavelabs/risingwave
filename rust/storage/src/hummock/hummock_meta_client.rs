use async_trait::async_trait;
use risingwave_pb::hummock::{
    AddTablesRequest, CompactTask, GetCompactionTasksRequest, GetNewTableIdRequest,
    HummockSnapshot, HummockVersion, PinSnapshotRequest, PinVersionRequest,
    ReportCompactionTasksRequest, SstableInfo, UnpinSnapshotRequest, UnpinVersionRequest,
};
use risingwave_rpc_client::MetaClient;

use crate::hummock::{
    HummockContextId, HummockEpoch, HummockError, HummockResult, HummockSSTableId,
    HummockVersionId, TracedHummockError,
};

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
    async fn pin_version(&self) -> HummockResult<(HummockVersionId, HummockVersion)>;
    async fn unpin_version(&self, _pinned_version_id: HummockVersionId) -> HummockResult<()>;
    async fn pin_snapshot(&self) -> HummockResult<HummockEpoch>;
    async fn unpin_snapshot(&self, _pinned_epoch: HummockEpoch) -> HummockResult<()>;
    async fn get_new_table_id(&self) -> HummockResult<HummockSSTableId>;
    async fn add_tables(
        &self,
        epoch: HummockEpoch,
        sstables: Vec<SstableInfo>,
    ) -> HummockResult<()>;
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
    context_id: HummockContextId,
}

impl RPCHummockMetaClient {
    // TODO use worker node id as context_id
    pub fn new(meta_client: MetaClient) -> RPCHummockMetaClient {
        RPCHummockMetaClient {
            meta_client,
            context_id: 0,
        }
    }
}

// TODO #2720 idempotent retry
#[async_trait]
impl HummockMetaClient for RPCHummockMetaClient {
    async fn pin_version(&self) -> HummockResult<(HummockVersionId, HummockVersion)> {
        let result = self
            .meta_client
            .to_owned()
            .hummock_client
            .pin_version(PinVersionRequest {
                context_id: self.context_id,
            })
            .await
            .map_err(HummockError::meta_error)?
            .into_inner();
        Ok((result.pinned_version_id, result.pinned_version.unwrap()))
    }

    async fn unpin_version(&self, pinned_version_id: HummockVersionId) -> HummockResult<()> {
        self.meta_client
            .to_owned()
            .hummock_client
            .unpin_version(UnpinVersionRequest {
                context_id: self.context_id,
                pinned_version_id,
            })
            .await
            .map_err(HummockError::meta_error)?;
        Ok(())
    }

    async fn pin_snapshot(&self) -> HummockResult<HummockEpoch> {
        let result = self
            .meta_client
            .to_owned()
            .hummock_client
            .pin_snapshot(PinSnapshotRequest {
                context_id: self.context_id,
            })
            .await
            .map_err(HummockError::meta_error)?
            .into_inner();
        Ok(result.snapshot.unwrap().epoch)
    }

    async fn unpin_snapshot(&self, pinned_epoch: HummockEpoch) -> HummockResult<()> {
        self.meta_client
            .to_owned()
            .hummock_client
            .unpin_snapshot(UnpinSnapshotRequest {
                context_id: self.context_id,
                snapshot: Some(HummockSnapshot {
                    epoch: pinned_epoch,
                }),
            })
            .await
            .map_err(HummockError::meta_error)?;
        Ok(())
    }

    async fn get_new_table_id(&self) -> HummockResult<HummockSSTableId> {
        let result = self
            .meta_client
            .to_owned()
            .hummock_client
            .get_new_table_id(GetNewTableIdRequest {})
            .await
            .map_err(HummockError::meta_error)?
            .into_inner();
        Ok(result.table_id)
    }

    async fn add_tables(
        &self,
        epoch: HummockEpoch,
        sstables: Vec<SstableInfo>,
    ) -> HummockResult<()> {
        self.meta_client
            .to_owned()
            .hummock_client
            .add_tables(AddTablesRequest {
                context_id: self.context_id,
                tables: sstables,
                epoch,
            })
            .await
            .map_err(HummockError::meta_error)?;
        Ok(())
    }

    async fn get_compaction_task(&self) -> HummockResult<Option<CompactTask>> {
        let result = self
            .meta_client
            .to_owned()
            .hummock_client
            .get_compaction_tasks(GetCompactionTasksRequest {
                context_id: self.context_id,
            })
            .await
            .map_err(HummockError::meta_error)?
            .into_inner();
        Ok(result.compact_task)
    }

    async fn report_compaction_task(
        &self,
        compact_task: CompactTask,
        task_result: bool,
    ) -> HummockResult<()> {
        self.meta_client
            .to_owned()
            .hummock_client
            .report_compaction_tasks(ReportCompactionTasksRequest {
                context_id: self.context_id,
                compact_task: Some(compact_task),
                task_result,
            })
            .await
            .map_err(HummockError::meta_error)?;
        Ok(())
    }

    async fn commit_epoch(&self, _epoch: HummockEpoch) -> HummockResult<()> {
        unimplemented!()
    }

    async fn abort_epoch(&self, _epoch: HummockEpoch) -> HummockResult<()> {
        unimplemented!()
    }
}
