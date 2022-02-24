use std::sync::Arc;

use risingwave_common::error::tonic_err;
use risingwave_common::try_match_expand;
use risingwave_pb::meta::cluster_service_server::ClusterService;
use risingwave_pb::meta::{ActivateWorkerNodeRequest, ActivateWorkerNodeResponse, AddWorkerNodeRequest, AddWorkerNodeResponse, DeleteWorkerNodeRequest, DeleteWorkerNodeResponse, ListAllNodesRequest, ListAllNodesResponse};
use tonic::{Request, Response, Status};

use crate::cluster::StoredClusterManager;
use crate::storage::MetaStore;

#[derive(Clone)]
pub struct ClusterServiceImpl<S>
where
    S: MetaStore,
{
    scm: Arc<StoredClusterManager<S>>,
}

impl<S> ClusterServiceImpl<S>
where
    S: MetaStore,
{
    pub fn new(scm: Arc<StoredClusterManager<S>>) -> Self {
        ClusterServiceImpl { scm }
    }
}

#[async_trait::async_trait]
impl<S> ClusterService for ClusterServiceImpl<S>
where
    S: MetaStore,
{
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

    async fn activate_worker_node(&self, request: Request<ActivateWorkerNodeRequest>) -> Result<Response<ActivateWorkerNodeResponse>, Status> {
        let req = request.into_inner();
        let host = try_match_expand!(req.host, Some, "ActivateWorkerNodeRequest::host is empty")
            .map_err(|e| e.to_grpc_status())?;
        self
            .scm
            .activate_worker_node(host)
            .await
            .map_err(|e| e.to_grpc_status())?;
        Ok(Response::new(ActivateWorkerNodeResponse {
            status: None,
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
        let worker_state = if req.include_creating {None} else {Some(risingwave_pb::common::worker_node::State::Created)};
        let node_list = self.scm.list_worker_node(worker_type, worker_state);
        Ok(Response::new(ListAllNodesResponse {
            status: None,
            nodes: node_list,
        }))
    }
}
