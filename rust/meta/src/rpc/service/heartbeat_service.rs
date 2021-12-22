use risingwave_common::array::RwError;
use risingwave_common::error::ErrorCode::ProtocolError;
use risingwave_pb::meta::heartbeat_request::NodeType;
use risingwave_pb::meta::heartbeat_response::Body;
use risingwave_pb::meta::heartbeat_service_server::HeartbeatService;
use risingwave_pb::meta::{HeartbeatRequest, HeartbeatResponse};
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
        match NodeType::from_i32(req.node_type) {
            Some(NodeType::Frontend) => {
                return Ok(Response::new(HeartbeatResponse {
                    status: None,
                    body: Some(Body::Catalog(
                        self.cmr
                            .get_catalog()
                            .await
                            .map_err(|e| e.to_grpc_status())?,
                    )),
                }));
            }
            Some(NodeType::Backend) => {
                todo!()
            }
            None => {
                Err(RwError::from(ProtocolError("node type invalid".to_string())).to_grpc_status())
            }
        }
    }
}
