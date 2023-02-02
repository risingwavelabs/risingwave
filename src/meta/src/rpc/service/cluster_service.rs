// Copyright 2023 RisingWave Labs
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

use risingwave_pb::meta::cluster_service_server::ClusterService;
use risingwave_pb::meta::{
    ActivateWorkerNodeRequest, ActivateWorkerNodeResponse, AddWorkerNodeRequest,
    AddWorkerNodeResponse, DeleteWorkerNodeRequest, DeleteWorkerNodeResponse, ListAllNodesRequest,
    ListAllNodesResponse,
};
use tonic::{Request, Response, Status};

use crate::manager::{ClusterManagerRef, SystemParamManagerRef};
use crate::storage::MetaStore;

#[derive(Clone)]
pub struct ClusterServiceImpl<S: MetaStore> {
    cluster_manager: ClusterManagerRef<S>,
    system_param_manager: SystemParamManagerRef<S>,
}

impl<S> ClusterServiceImpl<S>
where
    S: MetaStore,
{
    pub fn new(
        cluster_manager: ClusterManagerRef<S>,
        system_param_manager: SystemParamManagerRef<S>,
    ) -> Self {
        ClusterServiceImpl {
            cluster_manager,
            system_param_manager,
        }
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
        let worker_type = req.get_worker_type()?;
        let host = req.get_host()?.clone();
        // Allow the worker to join only if it's system parameters are consistent with the cluster.
        // Note that the verification is just for backward compatibility. It may be removed after
        // we deprecate system param items from the config file.
        let system_params = self
            .system_param_manager
            .verify_params(worker_type, req.verify_params.unwrap())?;
        let worker_node_parallelism = req.worker_node_parallelism as usize;
        let worker_node = self
            .cluster_manager
            .add_worker_node(worker_type, host, worker_node_parallelism)
            .await?;
        Ok(Response::new(AddWorkerNodeResponse {
            status: None,
            node: Some(worker_node),
            system_params: Some(system_params),
        }))
    }

    async fn activate_worker_node(
        &self,
        request: Request<ActivateWorkerNodeRequest>,
    ) -> Result<Response<ActivateWorkerNodeResponse>, Status> {
        let req = request.into_inner();
        let host = req.get_host()?.clone();
        self.cluster_manager.activate_worker_node(host).await?;
        Ok(Response::new(ActivateWorkerNodeResponse { status: None }))
    }

    async fn delete_worker_node(
        &self,
        request: Request<DeleteWorkerNodeRequest>,
    ) -> Result<Response<DeleteWorkerNodeResponse>, Status> {
        let req = request.into_inner();
        let host = req.get_host()?.clone();
        self.cluster_manager.delete_worker_node(host).await?;
        Ok(Response::new(DeleteWorkerNodeResponse { status: None }))
    }

    async fn list_all_nodes(
        &self,
        request: Request<ListAllNodesRequest>,
    ) -> Result<Response<ListAllNodesResponse>, Status> {
        let req = request.into_inner();
        let worker_type = req.get_worker_type()?;
        let worker_state = if req.include_starting_nodes {
            None
        } else {
            Some(risingwave_pb::common::worker_node::State::Running)
        };
        let node_list = self
            .cluster_manager
            .list_worker_node(worker_type, worker_state)
            .await;
        Ok(Response::new(ListAllNodesResponse {
            status: None,
            nodes: node_list,
        }))
    }
}
