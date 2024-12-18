// Copyright 2024 RisingWave Labs
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use risingwave_meta::barrier::BarrierManagerRef;
use risingwave_meta::manager::MetadataManager;
use risingwave_meta_model::WorkerId;
use risingwave_pb::common::worker_node::State;
use risingwave_pb::common::HostAddress;
use risingwave_pb::meta::cluster_service_server::ClusterService;
use risingwave_pb::meta::{
    ActivateWorkerNodeRequest, ActivateWorkerNodeResponse, AddWorkerNodeRequest,
    AddWorkerNodeResponse, DeleteWorkerNodeRequest, DeleteWorkerNodeResponse,
    GetClusterRecoveryStatusRequest, GetClusterRecoveryStatusResponse, GetMetaStoreInfoRequest,
    GetMetaStoreInfoResponse, ListAllNodesRequest, ListAllNodesResponse,
    UpdateWorkerNodeSchedulabilityRequest, UpdateWorkerNodeSchedulabilityResponse,
};
use tonic::{Request, Response, Status};

use crate::MetaError;

#[derive(Clone)]
pub struct ClusterServiceImpl {
    metadata_manager: MetadataManager,
    barrier_manager: BarrierManagerRef,
}

impl ClusterServiceImpl {
    pub fn new(metadata_manager: MetadataManager, barrier_manager: BarrierManagerRef) -> Self {
        ClusterServiceImpl {
            metadata_manager,
            barrier_manager,
        }
    }
}

#[async_trait::async_trait]
impl ClusterService for ClusterServiceImpl {
    async fn add_worker_node(
        &self,
        request: Request<AddWorkerNodeRequest>,
    ) -> Result<Response<AddWorkerNodeResponse>, Status> {
        let req = request.into_inner();
        let worker_type = req.get_worker_type()?;
        let host: HostAddress = req.get_host()?.clone();
        let property = req
            .property
            .ok_or_else(|| MetaError::invalid_parameter("worker node property is not provided"))?;
        let resource = req.resource.unwrap_or_default();
        let worker_id = self
            .metadata_manager
            .add_worker_node(worker_type, host, property, resource)
            .await?;
        let cluster_id = self.metadata_manager.cluster_id().to_string();

        Ok(Response::new(AddWorkerNodeResponse {
            node_id: Some(worker_id as _),
            cluster_id,
        }))
    }

    /// Update schedulability of a compute node. Will not affect actors which are already running on
    /// that node, if marked as unschedulable
    async fn update_worker_node_schedulability(
        &self,
        req: Request<UpdateWorkerNodeSchedulabilityRequest>,
    ) -> Result<Response<UpdateWorkerNodeSchedulabilityResponse>, Status> {
        let req = req.into_inner();
        let schedulability = req.get_schedulability()?;
        let worker_ids = req.worker_ids;

        self.metadata_manager
            .cluster_controller
            .update_schedulability(
                worker_ids.into_iter().map(|id| id as WorkerId).collect(),
                schedulability,
            )
            .await?;

        Ok(Response::new(UpdateWorkerNodeSchedulabilityResponse {
            status: None,
        }))
    }

    async fn activate_worker_node(
        &self,
        request: Request<ActivateWorkerNodeRequest>,
    ) -> Result<Response<ActivateWorkerNodeResponse>, Status> {
        let req = request.into_inner();
        let host = req.get_host()?.clone();
        #[cfg(not(madsim))]
        {
            use risingwave_common::util::addr::try_resolve_dns;
            use tracing::{error, info};
            let socket_addr = try_resolve_dns(&host.host, host.port).await.map_err(|e| {
                error!(e);
                Status::internal(e)
            })?;
            info!(?socket_addr, ?host, "resolve host addr");
        }
        self.metadata_manager
            .cluster_controller
            .activate_worker(req.node_id as _)
            .await?;
        Ok(Response::new(ActivateWorkerNodeResponse { status: None }))
    }

    async fn delete_worker_node(
        &self,
        request: Request<DeleteWorkerNodeRequest>,
    ) -> Result<Response<DeleteWorkerNodeResponse>, Status> {
        let req = request.into_inner();
        let host = req.get_host()?.clone();

        let worker_node = self
            .metadata_manager
            .cluster_controller
            .delete_worker(host)
            .await?;
        tracing::info!(
            host = ?worker_node.host,
            id = worker_node.id,
            r#type = ?worker_node.r#type(),
            "deleted worker node",
        );

        Ok(Response::new(DeleteWorkerNodeResponse { status: None }))
    }

    async fn list_all_nodes(
        &self,
        request: Request<ListAllNodesRequest>,
    ) -> Result<Response<ListAllNodesResponse>, Status> {
        let req = request.into_inner();
        let worker_type = req.worker_type.map(|wt| wt.try_into().unwrap());
        let worker_states = if req.include_starting_nodes {
            None
        } else {
            Some(State::Running)
        };

        let node_list = self
            .metadata_manager
            .list_worker_node(worker_type, worker_states)
            .await?;
        Ok(Response::new(ListAllNodesResponse {
            status: None,
            nodes: node_list,
        }))
    }

    async fn get_cluster_recovery_status(
        &self,
        _request: Request<GetClusterRecoveryStatusRequest>,
    ) -> Result<Response<GetClusterRecoveryStatusResponse>, Status> {
        Ok(Response::new(GetClusterRecoveryStatusResponse {
            status: self.barrier_manager.get_recovery_status() as _,
        }))
    }

    async fn get_meta_store_info(
        &self,
        _request: Request<GetMetaStoreInfoRequest>,
    ) -> Result<Response<GetMetaStoreInfoResponse>, Status> {
        Ok(Response::new(GetMetaStoreInfoResponse {
            meta_store_endpoint: self
                .metadata_manager
                .cluster_controller
                .meta_store_endpoint(),
        }))
    }
}
