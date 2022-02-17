use std::sync::Arc;

use risingwave_common::array::RwError;
use risingwave_common::error::ErrorCode::ProtocolError;
use risingwave_pb::common::WorkerType;
use risingwave_pb::meta::heartbeat_response::Body;
use risingwave_pb::meta::heartbeat_service_server::HeartbeatService;
use risingwave_pb::meta::{HeartbeatRequest, HeartbeatResponse};
use tonic::{Request, Response, Status};

use crate::model::Catalog;
use crate::storage::MetaStore;

#[derive(Clone)]
pub struct HeartbeatServiceImpl<S> {
    meta_store_ref: Arc<S>,
}

impl<S> HeartbeatServiceImpl<S> {
    pub fn new(meta_store_ref: Arc<S>) -> Self {
        HeartbeatServiceImpl { meta_store_ref }
    }
}

#[async_trait::async_trait]
impl<S> HeartbeatService for HeartbeatServiceImpl<S>
where
    S: MetaStore,
{
    #[cfg(not(tarpaulin_include))]
    async fn heartbeat(
        &self,
        request: Request<HeartbeatRequest>,
    ) -> Result<Response<HeartbeatResponse>, Status> {
        let req = request.into_inner();
        match WorkerType::from_i32(req.worker_type) {
            Some(WorkerType::Frontend) => Ok(Response::new(HeartbeatResponse {
                status: None,
                body: Some(Body::Catalog(
                    Catalog::get(&*self.meta_store_ref)
                        .await
                        .map_err(|e| e.to_grpc_status())?
                        .inner(),
                )),
            })),
            Some(WorkerType::ComputeNode) => Ok(Response::new(HeartbeatResponse {
                status: None,
                body: None,
            })),
            _ => {
                Err(RwError::from(ProtocolError("node type invalid".to_string())).to_grpc_status())
            }
        }
    }
}
