use std::sync::Arc;

use risingwave_common::array::InternalError;
use risingwave_common::error::ErrorCode::ProtocolError;
use risingwave_common::error::RwError;
use risingwave_pb::meta::cluster_service_server::ClusterService;
use risingwave_pb::meta::{
    AddWorkerNodeRequest, AddWorkerNodeResponse, ClusterType, DeleteWorkerNodeRequest,
    DeleteWorkerNodeResponse, ListAllNodesRequest, ListAllNodesResponse,
};
use tonic::{Request, Response, Status};

use crate::cluster::{StoredClusterManager, WorkerNodeMetaManager};

#[derive(Clone)]
pub struct ClusterServiceImpl {
    scm: Arc<StoredClusterManager>,
}

impl ClusterServiceImpl {
    pub fn new(scm: Arc<StoredClusterManager>) -> Self {
        ClusterServiceImpl { scm }
    }
}

#[async_trait::async_trait]
impl ClusterService for ClusterServiceImpl {
    async fn add_worker_node(
        &self,
        request: Request<AddWorkerNodeRequest>,
    ) -> Result<Response<AddWorkerNodeResponse>, Status> {
        let req = request.into_inner();
        let cluster_type = ClusterType::from_i32(req.cluster_type).unwrap();
        if let Some(host) = req.host {
            let worker_node_res = self.scm.add_worker_node(host, cluster_type).await;
            match worker_node_res {
                Ok(worker_node) => Ok(Response::new(AddWorkerNodeResponse {
                    status: None,
                    node: Some(worker_node.0),
                })),
                Err(_e) => Err(RwError::from(InternalError(
                    "worker node already exists".to_string(),
                ))
                .to_grpc_status()),
            }
        } else {
            Err(RwError::from(ProtocolError("host address invalid".to_string())).to_grpc_status())
        }
    }

    async fn delete_worker_node(
        &self,
        request: Request<DeleteWorkerNodeRequest>,
    ) -> Result<Response<DeleteWorkerNodeResponse>, Status> {
        let req = request.into_inner();
        let cluster_type = ClusterType::from_i32(req.cluster_type).unwrap();
        if let Some(node) = req.node {
            let delete_res = self.scm.delete_worker_node(node, cluster_type).await;
            match delete_res {
                Ok(()) => Ok(Response::new(DeleteWorkerNodeResponse { status: None })),
                Err(_e) => Err(
                    RwError::from(InternalError("worker node not exists".to_string()))
                        .to_grpc_status(),
                ),
            }
        } else {
            Err(RwError::from(ProtocolError("work node invalid".to_string())).to_grpc_status())
        }
    }

    async fn list_all_nodes(
        &self,
        request: Request<ListAllNodesRequest>,
    ) -> Result<Response<ListAllNodesResponse>, Status> {
        let req = request.into_inner();
        let cluster_type = ClusterType::from_i32(req.cluster_type).unwrap();
        let node_list = self.scm.list_worker_node(cluster_type).await.unwrap();
        Ok(Response::new(ListAllNodesResponse {
            status: None,
            nodes: node_list,
        }))
    }
}
