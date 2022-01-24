use async_trait::async_trait;
use risingwave_pb::hummock::{
    AddTablesRequest, GetNewTableIdRequest, HummockSnapshot, HummockVersion, PinSnapshotRequest,
    PinVersionRequest, SstableInfo, UnpinSnapshotRequest, UnpinVersionRequest,
};
use risingwave_rpc_client::MetaClient;

use crate::hummock::{
    HummockEpoch, HummockError, HummockResult, HummockSSTableId, HummockVersionId,
    TracedHummockError,
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
    async fn unpin_version(&self, pinned_version_id: HummockVersionId) -> HummockResult<()>;
    async fn pin_snapshot(&self) -> HummockResult<HummockEpoch>;
    async fn unpin_snapshot(&self, pinned_epoch: HummockEpoch) -> HummockResult<()>;
    async fn get_new_table_id(&self) -> HummockResult<HummockSSTableId>;
    async fn add_tables(
        &self,
        epoch: HummockEpoch,
        sstables: Vec<SstableInfo>,
    ) -> HummockResult<HummockVersionId>;
}

pub struct RPCHummockMetaClient {
    meta_client: MetaClient,
}

impl RPCHummockMetaClient {
    pub fn new(meta_client: MetaClient) -> RPCHummockMetaClient {
        RPCHummockMetaClient { meta_client }
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
                context_identifier: 0,
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
                context_identifier: 0,
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
                context_identifier: 0,
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
                context_identifier: 0,
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
    ) -> HummockResult<HummockVersionId> {
        let result = self
            .meta_client
            .to_owned()
            .hummock_client
            .add_tables(AddTablesRequest {
                context_identifier: 0,
                tables: sstables,
                epoch,
            })
            .await
            .map_err(HummockError::meta_error)?
            .into_inner();
        Ok(result.version_id)
    }
}
