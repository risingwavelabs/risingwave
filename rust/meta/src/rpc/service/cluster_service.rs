use std::sync::Arc;

use risingwave_common::error::tonic_err;
use risingwave_common::try_match_expand;
use risingwave_pb::meta::cluster_service_server::ClusterService;
use risingwave_pb::meta::{
    AddWorkerNodeRequest, AddWorkerNodeResponse, DeleteWorkerNodeRequest, DeleteWorkerNodeResponse,
    ListAllNodesRequest, ListAllNodesResponse,
};
use tonic::{Request, Response, Status};

use crate::cluster::StoredClusterManager;

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
        let worker_type = req.get_worker_type().map_err(tonic_err)?;
        let host = try_match_expand!(req.host, Some, "AddWorkerNodeRequest::host is empty")
            .map_err(|e| e.to_grpc_status())?;
        let (worker_node, _added) = self
            .scm
            .add_worker_node(host, worker_type)
            .await
            .map_err(|e| e.to_grpc_status())?;
        Ok(Response::new(AddWorkerNodeResponse {
            status: None,
            node: Some(worker_node),
        }))
    }

    async fn delete_worker_node(
        &self,
        request: Request<DeleteWorkerNodeRequest>,
    ) -> Result<Response<DeleteWorkerNodeResponse>, Status> {
        let req = request.into_inner();
        let node = try_match_expand!(req.node, Some, "DeleteWorkerNodeRequest::node is empty")
            .map_err(|e| e.to_grpc_status())?;
        let host = try_match_expand!(
            node.host,
            Some,
            "DeleteWorkerNodeRequest::node::host is empty"
        )
        .map_err(|e| e.to_grpc_status())?;
        let _ = self
            .scm
            .delete_worker_node(host)
            .await
            .map_err(|e| e.to_grpc_status())?;
        Ok(Response::new(DeleteWorkerNodeResponse { status: None }))
    }

    async fn list_all_nodes(
        &self,
        request: Request<ListAllNodesRequest>,
    ) -> Result<Response<ListAllNodesResponse>, Status> {
        let req = request.into_inner();
        let worker_type = req.get_worker_type().map_err(tonic_err)?;
        let node_list = self
            .scm
            .list_worker_node(worker_type)
            .map_err(|e| e.to_grpc_status())?;
        Ok(Response::new(ListAllNodesResponse {
            status: None,
            nodes: node_list,
        }))
    }
}
