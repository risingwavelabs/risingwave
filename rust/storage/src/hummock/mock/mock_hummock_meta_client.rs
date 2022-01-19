use std::sync::Arc;

use async_trait::async_trait;
use risingwave_pb::hummock::{
    AddTablesRequest, AddTablesResponse, GetNewTableIdRequest, GetNewTableIdResponse,
    PinSnapshotRequest, PinSnapshotResponse, PinVersionRequest, PinVersionResponse,
    UnpinSnapshotRequest, UnpinSnapshotResponse, UnpinVersionRequest, UnpinVersionResponse,
};

use crate::hummock::hummock_meta_client::HummockMetaClient;
use crate::hummock::mock::MockHummockMetaService;

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
    async fn pin_version(&self, request: PinVersionRequest) -> PinVersionResponse {
        self.mock_hummock_meta_service.pin_version(request)
    }

    async fn unpin_version(&self, request: UnpinVersionRequest) -> UnpinVersionResponse {
        self.mock_hummock_meta_service.unpin_version(request)
    }

    async fn pin_snapshot(&self, request: PinSnapshotRequest) -> PinSnapshotResponse {
        self.mock_hummock_meta_service.pin_snapshot(request)
    }

    async fn unpin_snapshot(&self, request: UnpinSnapshotRequest) -> UnpinSnapshotResponse {
        self.mock_hummock_meta_service.unpin_snapshot(request)
    }

    async fn get_new_table_id(&self, request: GetNewTableIdRequest) -> GetNewTableIdResponse {
        self.mock_hummock_meta_service.get_new_table_id(request)
    }

    async fn add_tables(&self, request: AddTablesRequest) -> AddTablesResponse {
        self.mock_hummock_meta_service.add_tables(request)
    }
}
