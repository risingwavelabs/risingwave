

use risingwave_pb::hummock::hummock_manager_service_server::HummockManagerService;
use risingwave_pb::hummock::*;
use tonic::{Request, Response, Status};

use crate::hummock::HummockManager;

pub struct HummockServiceImpl {
    hummock_manager: HummockManager,
}

impl HummockServiceImpl {
    pub fn new(hummock_manager: HummockManager) -> Self {
        HummockServiceImpl { hummock_manager }
    }
}

#[async_trait::async_trait]
impl HummockManagerService for HummockServiceImpl {
    async fn pin_version(
        &self,
        request: Request<PinVersionRequest>,
    ) -> Result<Response<PinVersionResponse>, Status> {
        let req = request.into_inner();
        let result = self
            .hummock_manager
            .pin_version(req.context_identifier)
            .await;
        match result {
            Ok((pinned_version_id, pinned_version)) => Ok(Response::new(PinVersionResponse {
                status: None,
                pinned_version_id,
                pinned_version: Some(pinned_version),
            })),
            Err(e) => Err(e.to_grpc_status()),
        }
    }

    async fn unpin_version(
        &self,
        request: Request<UnpinVersionRequest>,
    ) -> Result<Response<UnpinVersionResponse>, Status> {
        let req = request.into_inner();
        let result = self
            .hummock_manager
            .unpin_version(req.context_identifier, req.pinned_version_id)
            .await;
        match result {
            Ok(_) => Ok(Response::new(UnpinVersionResponse { status: None })),
            Err(e) => Err(e.to_grpc_status()),
        }
    }

    async fn add_tables(
        &self,
        request: Request<AddTablesRequest>,
    ) -> Result<Response<AddTablesResponse>, Status> {
        let req = request.into_inner();
        let result = self
            .hummock_manager
            .add_tables(req.context_identifier, req.tables, req.epoch)
            .await;
        match result {
            Ok(_) => Ok(Response::new(AddTablesResponse { status: None })),
            Err(e) => Err(e.to_grpc_status()),
        }
    }

    async fn get_compaction_tasks(
        &self,
        _request: Request<GetCompactionTasksRequest>,
    ) -> Result<Response<GetCompactionTasksResponse>, Status> {
        todo!()
    }

    async fn report_compaction_tasks(
        &self,
        _request: Request<ReportCompactionTasksRequest>,
    ) -> Result<Response<ReportCompactionTasksResponse>, Status> {
        todo!()
    }

    async fn pin_snapshot(
        &self,
        request: Request<PinSnapshotRequest>,
    ) -> Result<Response<PinSnapshotResponse>, Status> {
        let req = request.into_inner();
        let result = self
            .hummock_manager
            .pin_snapshot(req.context_identifier)
            .await;
        match result {
            Ok(hummock_snapshot) => Ok(Response::new(PinSnapshotResponse {
                status: None,
                snapshot: Some(hummock_snapshot),
            })),
            Err(e) => Err(e.to_grpc_status()),
        }
    }

    async fn unpin_snapshot(
        &self,
        request: Request<UnpinSnapshotRequest>,
    ) -> Result<Response<UnpinSnapshotResponse>, Status> {
        let req = request.into_inner();
        let result = self
            .hummock_manager
            .unpin_snapshot(req.context_identifier, req.snapshot.unwrap())
            .await;
        match result {
            Ok(_) => Ok(Response::new(UnpinSnapshotResponse { status: None })),
            Err(e) => Err(e.to_grpc_status()),
        }
    }

    async fn commit_epoch(
        &self,
        request: Request<CommitEpochRequest>,
    ) -> Result<Response<CommitEpochResponse>, Status> {
        let req = request.into_inner();
        let result = self
            .hummock_manager
            .commit_epoch(req.context_identifier, req.epoch)
            .await;
        match result {
            Ok(_) => Ok(Response::new(CommitEpochResponse { status: None })),
            Err(e) => Err(e.to_grpc_status()),
        }
    }

    async fn abort_epoch(
        &self,
        request: Request<AbortEpochRequest>,
    ) -> Result<Response<AbortEpochResponse>, Status> {
        let req = request.into_inner();
        let result = self
            .hummock_manager
            .abort_epoch(req.context_identifier, req.epoch)
            .await;
        match result {
            Ok(_) => Ok(Response::new(AbortEpochResponse { status: None })),
            Err(e) => Err(e.to_grpc_status()),
        }
    }

    async fn get_new_table_id(
        &self,
        request: Request<GetNewTableIdRequest>,
    ) -> Result<Response<GetNewTableIdResponse>, Status> {
        let _req = request.into_inner();
        let result = self.hummock_manager.get_new_table_id().await;
        match result {
            Ok(table_id) => Ok(Response::new(GetNewTableIdResponse {
                status: None,
                table_id,
            })),
            Err(e) => Err(e.to_grpc_status()),
        }
    }
}
