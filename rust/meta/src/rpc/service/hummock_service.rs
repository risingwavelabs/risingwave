use std::sync::Arc;

use risingwave_pb::hummock::hummock_manager_service_server::HummockManagerService;
use risingwave_pb::hummock::*;
use tonic::{Request, Response, Status};

use crate::hummock::HummockManager;
use crate::storage::MetaStore;

pub struct HummockServiceImpl<S>
where
    S: MetaStore,
{
    hummock_manager: Arc<HummockManager<S>>,
}

impl<S> HummockServiceImpl<S>
where
    S: MetaStore,
{
    pub fn new(hummock_manager: Arc<HummockManager<S>>) -> Self {
        HummockServiceImpl { hummock_manager }
    }
}

#[async_trait::async_trait]
impl<S> HummockManagerService for HummockServiceImpl<S>
where
    S: MetaStore,
{
    async fn pin_version(
        &self,
        request: Request<PinVersionRequest>,
    ) -> Result<Response<PinVersionResponse>, Status> {
        let req = request.into_inner();
        let result = self.hummock_manager.pin_version(req.context_id).await;
        match result {
            Ok(pinned_version) => Ok(Response::new(PinVersionResponse {
                status: None,
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
            .unpin_version(req.context_id, req.pinned_version_id)
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
            .add_tables(req.context_id, req.tables, req.epoch)
            .await;
        match result {
            Ok(version) => Ok(Response::new(AddTablesResponse {
                status: None,
                version: Some(version),
            })),
            Err(e) => Err(e.to_grpc_status()),
        }
    }

    async fn get_compaction_tasks(
        &self,
        request: Request<GetCompactionTasksRequest>,
    ) -> Result<Response<GetCompactionTasksResponse>, Status> {
        let req = request.into_inner();
        let result = self.hummock_manager.get_compact_task(req.context_id).await;
        match result {
            Ok(compact_task) => Ok(Response::new(GetCompactionTasksResponse {
                status: None,
                compact_task,
            })),
            Err(e) => Err(e.to_grpc_status()),
        }
    }

    async fn report_compaction_tasks(
        &self,
        request: Request<ReportCompactionTasksRequest>,
    ) -> Result<Response<ReportCompactionTasksResponse>, Status> {
        let req = request.into_inner();
        match req.compact_task {
            None => Ok(Response::new(ReportCompactionTasksResponse {
                status: None,
            })),
            Some(compact_task) => {
                let result = self
                    .hummock_manager
                    .report_compact_task(req.context_id, compact_task, req.task_result)
                    .await;
                match result {
                    Ok(_) => Ok(Response::new(ReportCompactionTasksResponse {
                        status: None,
                    })),
                    Err(e) => Err(e.to_grpc_status()),
                }
            }
        }
    }

    async fn pin_snapshot(
        &self,
        request: Request<PinSnapshotRequest>,
    ) -> Result<Response<PinSnapshotResponse>, Status> {
        let req = request.into_inner();
        let result = self.hummock_manager.pin_snapshot(req.context_id).await;
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
            .unpin_snapshot(req.context_id, req.snapshot.unwrap())
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
        let result = self.hummock_manager.commit_epoch(req.epoch).await;
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
        let result = self.hummock_manager.abort_epoch(req.epoch).await;
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
