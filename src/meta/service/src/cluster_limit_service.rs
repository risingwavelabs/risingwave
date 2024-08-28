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

use std::collections::HashSet;

use itertools::Itertools;
use risingwave_common::license::LicenseManager;
use risingwave_meta::manager::{MetaSrvEnv, MetadataManager, WorkerId};
use risingwave_pb::meta::cluster_limit_service_server::ClusterLimitService;
use risingwave_pb::meta::{
    GetClusterLimitsRequest, GetClusterLimitsResponse, PbActorCountPerParallelism,
    PbWorkerActorCount, WorkerActorCount,
};
use thiserror_ext::AsReport;
use tonic::{Request, Response, Status};

const FREE_TIER_ACTOR_CNT_SOFT_LIMIT: usize = 25;
const FREE_TIER_ACTOR_CNT_HARD_LIMIT: usize = 100;

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

    async fn get_active_actor_limit(&self) -> Option<PbActorCountPerParallelism> {
        let (soft, hard) = match LicenseManager::get().tier() {
            Ok(Tier::Paid) => (
                self.env.opts.actor_cnt_per_parallelism_soft_limit,
                self.env.opts.actor_cnt_per_parallelism_hard_limit,
            ),
            Ok(Tier::Free) => (
                FREE_TIER_ACTOR_CNT_SOFT_LIMIT,
                FREE_TIER_ACTOR_CNT_HARD_LIMIT,
            ),
            Err(e) => {
                tracing::warn!("Failed to get license tier: {}", e);
                // Default to use free tier limit if there is any license error
                (
                    FREE_TIER_ACTOR_CNT_SOFT_LIMIT,
                    FREE_TIER_ACTOR_CNT_HARD_LIMIT,
                )
            }
        };

        let running_worker_parallelism: HashMap<WorkerId, usize> = self
            .metadata_manager
            .list_worker_node(Some(WorkerType::ComputeNode), Some(State::Running))
            .await?
            .into_iter()
            .map(|e| (e.id, e.parallelism()))
            .collect();
        let worker_actor_count: HashSet<WorkerId, PbWorkerActorCount> = self
            .metadata_manager
            .worker_actor_count()
            .await?
            .into_iter()
            .filter_map(|(worker_id, actor_count)| {
                running_worker_parallelism
                    .get(&worker_id)
                    .map(|parallelism| {
                        (
                            worker_id,
                            PbWorkerActorCount {
                                actor_count,
                                parallelism,
                            },
                        )
                    })
            })
            .collect();

        
    }
}

#[async_trait::async_trait]
impl ClusterLimitService for ClusterLimitServiceImpl {
    #[cfg_attr(coverage, coverage(off))]
    async fn get_cluster_limits(
        &self,
        request: Request<GetClusterLimitsRequest>,
    ) -> Result<Response<GetClusterLimitsResponse>, Status> {
        // let req = request.into_inner();
        // let info = req
        //     .info
        //     .into_iter()
        //     .filter_map(|node_info| node_info.info)
        //     .collect_vec();
        // let result = match &self.metadata_manager {
        //     MetadataManager::V1(mgr) => mgr.cluster_manager.heartbeat(req.node_id, info).await,
        //     MetadataManager::V2(mgr) => {
        //         mgr.cluster_controller
        //             .heartbeat(req.node_id as _, info)
        //             .await
        //     }
        // };

        // match result {
        //     Ok(_) => Ok(Response::new(HeartbeatResponse { status: None })),
        //     Err(e) => {
        //         if e.is_invalid_worker() {
        //             return Ok(Response::new(HeartbeatResponse {
        //                 status: Some(risingwave_pb::common::Status {
        //                     code: risingwave_pb::common::status::Code::UnknownWorker as i32,
        //                     message: e.to_report_string(),
        //                 }),
        //             }));
        //         }
        //         Err(e.into())
        //     }
        // }
        Ok(Response::new(GetClusterLimitsResponse {
            active_limits: None,
        }))
    }
}
