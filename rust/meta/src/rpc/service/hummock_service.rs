use std::sync::Arc;

use risingwave_pb::hummock::hummock_manager_service_server::HummockManagerService;
use risingwave_pb::hummock::*;
use tonic::Response;

use crate::hummock::HummockManager;

pub struct HummockServiceImpl {
    hummock_manager: Arc<dyn HummockManager>,
}

impl HummockServiceImpl {
    pub fn new(hummock_manager: Arc<dyn HummockManager>) -> Self {
        HummockServiceImpl { hummock_manager }
    }
}

#[async_trait::async_trait]
impl HummockManagerService for HummockServiceImpl {
    async fn create_hummock_context(
        &self,
        request: tonic::Request<CreateHummockContextRequest>,
    ) -> Result<tonic::Response<CreateHummockContextResponse>, tonic::Status> {
        let req = request.into_inner();
        let result = self
            .hummock_manager
            .create_hummock_context(req.get_group())
            .await;
        match result {
            Ok(hummock_context) => Ok(Response::new(CreateHummockContextResponse {
                status: None,
                hummock_context: Some(hummock_context),
            })),
            Err(e) => Err(e.to_grpc_status()),
        }
    }

    async fn invalidate_hummock_context(
        &self,
        request: tonic::Request<InvalidateHummockContextRequest>,
    ) -> Result<tonic::Response<InvalidateHummockContextResponse>, tonic::Status> {
        let req = request.into_inner();
        let result = self
            .hummock_manager
            .invalidate_hummock_context(req.context_identifier)
            .await;
        match result {
            Ok(()) => Ok(Response::new(InvalidateHummockContextResponse {
                status: None,
            })),
            Err(e) => Err(e.to_grpc_status()),
        }
    }

    async fn add_sst_watermark(
        &self,
        _request: tonic::Request<AddSstWatermarkRequest>,
    ) -> Result<tonic::Response<AddSstWatermarkResponse>, tonic::Status> {
        todo!()
    }

    async fn remove_sst_watermark(
        &self,
        _request: tonic::Request<RemoveSstWatermarkRequest>,
    ) -> Result<tonic::Response<RemoveSstWatermarkResponse>, tonic::Status> {
        todo!()
    }

    async fn add_ssts(
        &self,
        _request: tonic::Request<AddSstsRequest>,
    ) -> Result<tonic::Response<AddSstsResponse>, tonic::Status> {
        todo!()
    }

    async fn refresh_hummock_context(
        &self,
        request: tonic::Request<RefreshHummockContextRequest>,
    ) -> Result<tonic::Response<RefreshHummockContextResponse>, tonic::Status> {
        let req = request.into_inner();
        let result = self
            .hummock_manager
            .refresh_hummock_context(req.context_identifier)
            .await;
        match result {
            Ok(ttl) => Ok(Response::new(RefreshHummockContextResponse {
                status: None,
                ttl,
            })),
            Err(e) => Err(e.to_grpc_status()),
        }
    }
}
