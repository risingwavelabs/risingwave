use risingwave_common::array::RwError;
use risingwave_common::error::ErrorCode::ProtocolError;
use risingwave_pb::meta::heartbeat_response::Body;
use risingwave_pb::meta::heartbeat_service_server::HeartbeatService;
use risingwave_pb::meta::{ClusterType, HeartbeatRequest, HeartbeatResponse};
use tonic::{Request, Response, Status};

use crate::catalog::CatalogManagerRef;

#[derive(Clone)]
pub struct HeartbeatServiceImpl {
    cmr: CatalogManagerRef,
}

impl HeartbeatServiceImpl {
    pub fn new(cmr: CatalogManagerRef) -> Self {
        HeartbeatServiceImpl { cmr }
    }
}

#[async_trait::async_trait]
impl HeartbeatService for HeartbeatServiceImpl {
    #[cfg(not(tarpaulin_include))]
    async fn heartbeat(
        &self,
        request: Request<HeartbeatRequest>,
    ) -> Result<Response<HeartbeatResponse>, Status> {
        let req = request.into_inner();
        match ClusterType::from_i32(req.cluster_type) {
            Some(ClusterType::Frontend) => Ok(Response::new(HeartbeatResponse {
                status: None,
                body: Some(Body::Catalog(
                    self.cmr
                        .get_catalog()
                        .await
                        .map_err(|e| e.to_grpc_status())?,
                )),
            })),
            Some(ClusterType::ComputeNode) => Ok(Response::new(HeartbeatResponse {
                status: None,
                body: None,
            })),
            _ => {
                Err(RwError::from(ProtocolError("node type invalid".to_string())).to_grpc_status())
            }
        }
    }
}
