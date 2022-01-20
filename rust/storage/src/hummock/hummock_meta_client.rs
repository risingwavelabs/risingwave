use async_trait::async_trait;
use risingwave_pb::hummock::{HummockVersion, SstableInfo};

use crate::hummock::{
    HummockEpoch, HummockResult, HummockSSTableId, HummockVersionId, TracedHummockError,
};

#[derive(Default)]
pub struct RetryableError {}

impl tokio_retry::Condition<TracedHummockError> for RetryableError {
    fn should_retry(&mut self, _error: &TracedHummockError) -> bool {
        // TODO define retryable error here
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
