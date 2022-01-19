use async_trait::async_trait;
use risingwave_pb::hummock::{
    AddTablesRequest, AddTablesResponse, GetNewTableIdRequest, GetNewTableIdResponse,
    PinSnapshotRequest, PinSnapshotResponse, PinVersionRequest, PinVersionResponse,
    UnpinSnapshotRequest, UnpinSnapshotResponse, UnpinVersionRequest, UnpinVersionResponse,
};

/// Define the rpc client trait to ease unit test.
#[async_trait]
pub trait HummockMetaClient: Send + Sync + 'static {
    async fn pin_version(&self, request: PinVersionRequest) -> PinVersionResponse;
    async fn unpin_version(&self, request: UnpinVersionRequest) -> UnpinVersionResponse;
    async fn pin_snapshot(&self, request: PinSnapshotRequest) -> PinSnapshotResponse;
    async fn unpin_snapshot(&self, request: UnpinSnapshotRequest) -> UnpinSnapshotResponse;
    async fn get_new_table_id(&self, request: GetNewTableIdRequest) -> GetNewTableIdResponse;
    async fn add_tables(&self, request: AddTablesRequest) -> AddTablesResponse;
}
