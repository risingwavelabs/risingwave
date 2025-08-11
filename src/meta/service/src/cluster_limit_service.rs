// Copyright 2025 RisingWave Labs
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

use std::collections::HashMap;

use risingwave_common::util::cluster_limit::{
    ActorCountPerParallelism, ClusterLimit, WorkerActorCount,
};
use risingwave_meta::MetaResult;
use risingwave_meta::manager::{MetaSrvEnv, MetadataManager};
use risingwave_meta_model::WorkerId;
use risingwave_pb::common::WorkerType;
use risingwave_pb::common::worker_node::State;
use risingwave_pb::meta::cluster_limit_service_server::ClusterLimitService;
use risingwave_pb::meta::{GetClusterLimitsRequest, GetClusterLimitsResponse};
use tonic::{Request, Response, Status};

#[derive(Clone)]
pub struct ClusterLimitServiceImpl {
    env: MetaSrvEnv,
    metadata_manager: MetadataManager,
}

impl ClusterLimitServiceImpl {
    pub fn new(env: MetaSrvEnv, metadata_manager: MetadataManager) -> Self {
        ClusterLimitServiceImpl {
            env,
            metadata_manager,
        }
    }

    async fn get_active_actor_limit(&self) -> MetaResult<Option<ClusterLimit>> {
        let (soft_limit, hard_limit) = (
            self.env.opts.actor_cnt_per_worker_parallelism_soft_limit,
            self.env.opts.actor_cnt_per_worker_parallelism_hard_limit,
        );

        let running_worker_parallelism: HashMap<WorkerId, usize> = self
            .metadata_manager
            .list_worker_node(Some(WorkerType::ComputeNode), Some(State::Running))
            .await?
            .into_iter()
            .map(|e| (e.id as _, e.compute_node_parallelism()))
            .collect();
        let worker_actor_count: HashMap<u32, WorkerActorCount> = self
            .metadata_manager
            .worker_actor_count()
            .await?
            .into_iter()
            .filter_map(|(worker_id, actor_count)| {
                running_worker_parallelism
                    .get(&worker_id)
                    .map(|parallelism| {
                        (
                            worker_id as _,
                            WorkerActorCount {
                                actor_count,
                                parallelism: *parallelism,
                            },
                        )
                    })
            })
            .collect();

        let limit = ActorCountPerParallelism {
            worker_id_to_actor_count: worker_actor_count,
            hard_limit,
            soft_limit,
        };

        if limit.exceed_limit() {
            Ok(Some(ClusterLimit::ActorCount(limit)))
        } else {
            Ok(None)
        }
    }
}

#[async_trait::async_trait]
impl ClusterLimitService for ClusterLimitServiceImpl {
    async fn get_cluster_limits(
        &self,
        _request: Request<GetClusterLimitsRequest>,
    ) -> Result<Response<GetClusterLimitsResponse>, Status> {
        // TODO: support more limits
        match self.get_active_actor_limit().await {
            Ok(Some(limit)) => Ok(Response::new(GetClusterLimitsResponse {
                active_limits: vec![limit.into()],
            })),
            Ok(None) => Ok(Response::new(GetClusterLimitsResponse {
                active_limits: vec![],
            })),
            Err(e) => Err(e.into()),
        }
    }
}
