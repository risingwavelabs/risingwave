use async_trait::async_trait;
use risingwave_pb::hummock::{HummockVersion, SstableInfo};
use risingwave_storage::hummock::hummock_meta_client::HummockMetaClient;
use risingwave_storage::hummock::{
    HummockEpoch, HummockResult, HummockSSTableId, HummockVersionId,
};

use crate::hummock::HummockManager;

pub(crate) struct MockHummockMetaClient {
    hummock_manager: HummockManager,
}

impl MockHummockMetaClient {
    pub fn new(hummock_manager: HummockManager) -> MockHummockMetaClient {
        MockHummockMetaClient { hummock_manager }
    }
}

#[async_trait]
impl HummockMetaClient for MockHummockMetaClient {
    async fn pin_version(&self) -> HummockResult<(HummockVersionId, HummockVersion)> {
        todo!()
    }

    async fn unpin_version(&self, _pinned_version_id: HummockVersionId) -> HummockResult<()> {
        todo!()
    }

    async fn pin_snapshot(&self) -> HummockResult<HummockEpoch> {
        todo!()
    }

    async fn unpin_snapshot(&self, _pinned_epoch: HummockEpoch) -> HummockResult<()> {
        todo!()
    }

    async fn get_new_table_id(&self) -> HummockResult<HummockSSTableId> {
        todo!()
    }

    async fn add_tables(
        &self,
        _epoch: HummockEpoch,
        _sstables: Vec<SstableInfo>,
    ) -> HummockResult<()> {
        todo!()
    }
}

mod tests {
    use crate::hummock::mock_hummock_meta_client::MockHummockMetaClient;
    use crate::hummock::HummockManager;
    use crate::manager::MetaSrvEnv;

    #[tokio::test]
    async fn test_mock_hummock_meta_client_creation() -> risingwave_common::error::Result<()> {
        let sled_root = tempfile::tempdir().unwrap();
        let env = MetaSrvEnv::for_test_with_sled(sled_root).await;
        let instance = HummockManager::new(env).await?;
        MockHummockMetaClient::new(instance);
        Ok(())
    }
}
