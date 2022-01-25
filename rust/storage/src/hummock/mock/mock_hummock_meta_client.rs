use std::sync::Arc;

use async_trait::async_trait;
use risingwave_pb::hummock::{
    AddTablesRequest, GetNewTableIdRequest, HummockSnapshot, HummockVersion, PinSnapshotRequest,
    PinVersionRequest, SstableInfo, UnpinSnapshotRequest, UnpinVersionRequest,
};

use crate::hummock::hummock_meta_client::HummockMetaClient;
use crate::hummock::mock::MockHummockMetaService;
use crate::hummock::{HummockEpoch, HummockResult, HummockSSTableId, HummockVersionId};

pub struct MockHummockMetaClient {
    mock_hummock_meta_service: Arc<MockHummockMetaService>,
}

impl MockHummockMetaClient {
    pub fn new(mock_hummock_meta_service: Arc<MockHummockMetaService>) -> MockHummockMetaClient {
        MockHummockMetaClient {
            mock_hummock_meta_service,
        }
    }
}

#[async_trait]
impl HummockMetaClient for MockHummockMetaClient {
    async fn pin_version(&self) -> HummockResult<(HummockVersionId, HummockVersion)> {
        let response = self
            .mock_hummock_meta_service
            .pin_version(PinVersionRequest {
                context_identifier: 0,
            });
        Ok((response.pinned_version_id, response.pinned_version.unwrap()))
    }

    async fn unpin_version(&self, pinned_version_id: HummockVersionId) -> HummockResult<()> {
        self.mock_hummock_meta_service
            .unpin_version(UnpinVersionRequest {
                context_identifier: 0,
                pinned_version_id,
            });
        Ok(())
    }

    async fn pin_snapshot(&self) -> HummockResult<HummockEpoch> {
        let epoch = self
            .mock_hummock_meta_service
            .pin_snapshot(PinSnapshotRequest {
                context_identifier: 0,
            })
            .snapshot
            .unwrap()
            .epoch;
        Ok(epoch)
    }

    async fn unpin_snapshot(&self, pinned_epoch: HummockEpoch) -> HummockResult<()> {
        self.mock_hummock_meta_service
            .unpin_snapshot(UnpinSnapshotRequest {
                context_identifier: 0,
                snapshot: Some(HummockSnapshot {
                    epoch: pinned_epoch,
                }),
            });
        Ok(())
    }

    async fn get_new_table_id(&self) -> HummockResult<HummockSSTableId> {
        let table_id = self
            .mock_hummock_meta_service
            .get_new_table_id(GetNewTableIdRequest {})
            .table_id;
        Ok(table_id)
    }

    async fn add_tables(
        &self,
        epoch: HummockEpoch,
        sstables: Vec<SstableInfo>,
    ) -> HummockResult<()> {
        self.mock_hummock_meta_service.add_tables(AddTablesRequest {
            context_identifier: 0,
            tables: sstables.to_vec(),
            epoch,
        });
        Ok(())
    }
}
