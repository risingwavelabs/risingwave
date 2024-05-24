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

use risingwave_meta::manager::MetadataManager;
use risingwave_meta_model_v2::WorkerId;
use risingwave_pb::common::worker_node::State;
use risingwave_pb::common::HostAddress;
use risingwave_pb::meta::cluster_service_server::ClusterService;
use risingwave_pb::meta::{
    ActivateWorkerNodeRequest, ActivateWorkerNodeResponse, AddWorkerNodeRequest,
    AddWorkerNodeResponse, DeleteWorkerNodeRequest, DeleteWorkerNodeResponse, ListAllNodesRequest,
    ListAllNodesResponse, UpdateWorkerNodeSchedulabilityRequest,
    UpdateWorkerNodeSchedulabilityResponse,
};
use thiserror_ext::AsReport;
use tonic::{Request, Response, Status};

use crate::MetaError;

#[derive(Clone)]
pub struct ClusterServiceImpl {
    metadata_manager: MetadataManager,
}

impl ClusterServiceImpl {
    pub fn new(metadata_manager: MetadataManager) -> Self {
        ClusterServiceImpl { metadata_manager }
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
        let result = self
            .metadata_manager
            .add_worker_node(worker_type, host, property, resource)
            .await;
        match result {
            Ok(worker_id) => Ok(Response::new(AddWorkerNodeResponse {
                status: None,
                node_id: Some(worker_id),
            })),
            Err(e) => {
                if e.is_invalid_worker() {
                    return Ok(Response::new(AddWorkerNodeResponse {
                        status: Some(risingwave_pb::common::Status {
                            code: risingwave_pb::common::status::Code::UnknownWorker as i32,
                            message: e.to_report_string(),
                        }),
                        node_id: None,
                    }));
                }
                Err(e.into())
            }
        }
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

        match &self.metadata_manager {
            MetadataManager::V1(mgr) => {
                mgr.cluster_manager
                    .update_schedulability(worker_ids, schedulability)
                    .await?
            }
            MetadataManager::V2(mgr) => {
                mgr.cluster_controller
                    .update_schedulability(
                        worker_ids.into_iter().map(|id| id as WorkerId).collect(),
                        schedulability,
                    )
                    .await?
            }
        }

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
        match &self.metadata_manager {
            MetadataManager::V1(mgr) => mgr.cluster_manager.activate_worker_node(host).await?,
            MetadataManager::V2(mgr) => {
                mgr.cluster_controller
                    .activate_worker(req.node_id as _)
                    .await?
            }
        }
        Ok(Response::new(ActivateWorkerNodeResponse { status: None }))
    }

    async fn delete_worker_node(
        &self,
        request: Request<DeleteWorkerNodeRequest>,
    ) -> Result<Response<DeleteWorkerNodeResponse>, Status> {
        let req = request.into_inner();
        let host = req.get_host()?.clone();
        match &self.metadata_manager {
            MetadataManager::V1(mgr) => {
                let _ = mgr.cluster_manager.delete_worker_node(host).await?;
            }
            MetadataManager::V2(mgr) => {
                mgr.cluster_controller.delete_worker(host).await?;
            }
        }

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
}
