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

use std::collections::{HashMap, HashSet};

use itertools::Itertools;
use risingwave_common::catalog::TableId;
use risingwave_meta::manager::{LocalNotification, MetadataManager};
use risingwave_meta::model;
use risingwave_meta::model::ActorId;
use risingwave_meta::stream::ThrottleConfig;
use risingwave_meta_model_v2::{SourceId, StreamingParallelism};
use risingwave_pb::meta::cancel_creating_jobs_request::Jobs;
use risingwave_pb::meta::list_table_fragments_response::{
    ActorInfo, FragmentInfo, TableFragmentInfo,
};
use risingwave_pb::meta::stream_manager_service_server::StreamManagerService;
use risingwave_pb::meta::table_fragments::actor_status::PbActorState;
use risingwave_pb::meta::table_fragments::fragment::PbFragmentDistributionType;
use risingwave_pb::meta::table_fragments::PbState;
use risingwave_pb::meta::*;
use tonic::{Request, Response, Status};

use crate::barrier::{BarrierScheduler, Command};
use crate::manager::MetaSrvEnv;
use crate::stream::GlobalStreamManagerRef;

pub type TonicResponse<T> = Result<Response<T>, Status>;

#[derive(Clone)]
pub struct StreamServiceImpl {
    env: MetaSrvEnv,
    barrier_scheduler: BarrierScheduler,
    stream_manager: GlobalStreamManagerRef,
    metadata_manager: MetadataManager,
}

impl StreamServiceImpl {
    pub fn new(
        env: MetaSrvEnv,
        barrier_scheduler: BarrierScheduler,
        stream_manager: GlobalStreamManagerRef,
        metadata_manager: MetadataManager,
    ) -> Self {
        StreamServiceImpl {
            env,
            barrier_scheduler,
            stream_manager,
            metadata_manager,
        }
    }
}

#[async_trait::async_trait]
impl StreamManagerService for StreamServiceImpl {
    #[cfg_attr(coverage, coverage(off))]
    async fn flush(&self, request: Request<FlushRequest>) -> TonicResponse<FlushResponse> {
        self.env.idle_manager().record_activity();
        let req = request.into_inner();

        let version_id = self.barrier_scheduler.flush(req.checkpoint).await?;
        Ok(Response::new(FlushResponse {
            status: None,
            hummock_version_id: version_id.to_u64(),
        }))
    }

    #[cfg_attr(coverage, coverage(off))]
    async fn pause(&self, _: Request<PauseRequest>) -> Result<Response<PauseResponse>, Status> {
        self.barrier_scheduler
            .run_command(Command::pause(PausedReason::Manual))
            .await?;
        Ok(Response::new(PauseResponse {}))
    }

    #[cfg_attr(coverage, coverage(off))]
    async fn resume(&self, _: Request<ResumeRequest>) -> Result<Response<ResumeResponse>, Status> {
        self.barrier_scheduler
            .run_command(Command::resume(PausedReason::Manual))
            .await?;
        Ok(Response::new(ResumeResponse {}))
    }

    #[cfg_attr(coverage, coverage(off))]
    async fn apply_throttle(
        &self,
        request: Request<ApplyThrottleRequest>,
    ) -> Result<Response<ApplyThrottleResponse>, Status> {
        let request = request.into_inner();

        let actor_to_apply = match request.kind() {
            ThrottleTarget::Source | ThrottleTarget::TableWithSource => {
                self.metadata_manager
                    .update_source_rate_limit_by_source_id(request.id as SourceId, request.rate)
                    .await?
            }
            ThrottleTarget::Mv => {
                self.metadata_manager
                    .update_mv_rate_limit_by_table_id(TableId::from(request.id), request.rate)
                    .await?
            }
            ThrottleTarget::CdcTable => {
                self.metadata_manager
                    .update_mv_rate_limit_by_table_id(TableId::from(request.id), request.rate)
                    .await?
            }
            ThrottleTarget::Unspecified => {
                return Err(Status::invalid_argument("unspecified throttle target"))
            }
        };

        let mutation: ThrottleConfig = actor_to_apply
            .iter()
            .map(|(fragment_id, actors)| {
                (
                    *fragment_id,
                    actors
                        .iter()
                        .map(|actor_id| (*actor_id, request.rate))
                        .collect::<HashMap<ActorId, Option<u32>>>(),
                )
            })
            .collect();
        let _i = self
            .barrier_scheduler
            .run_command(Command::Throttle(mutation))
            .await?;

        Ok(Response::new(ApplyThrottleResponse { status: None }))
    }

    async fn cancel_creating_jobs(
        &self,
        request: Request<CancelCreatingJobsRequest>,
    ) -> TonicResponse<CancelCreatingJobsResponse> {
        let req = request.into_inner();
        let table_ids = match req.jobs.unwrap() {
            Jobs::Infos(infos) => match &self.metadata_manager {
                MetadataManager::V1(mgr) => {
                    mgr.catalog_manager
                        .find_creating_streaming_job_ids(infos.infos)
                        .await
                }
                MetadataManager::V2(mgr) => mgr
                    .catalog_controller
                    .find_creating_streaming_job_ids(infos.infos)
                    .await?
                    .into_iter()
                    .map(|id| id as _)
                    .collect(),
            },
            Jobs::Ids(jobs) => jobs.job_ids,
        };

        let canceled_jobs = self
            .stream_manager
            .cancel_streaming_jobs(table_ids.into_iter().map(TableId::from).collect_vec())
            .await
            .into_iter()
            .map(|id| id.table_id)
            .collect_vec();
        Ok(Response::new(CancelCreatingJobsResponse {
            status: None,
            canceled_jobs,
        }))
    }

    #[cfg_attr(coverage, coverage(off))]
    async fn list_table_fragments(
        &self,
        request: Request<ListTableFragmentsRequest>,
    ) -> Result<Response<ListTableFragmentsResponse>, Status> {
        let req = request.into_inner();
        let table_ids = HashSet::<u32>::from_iter(req.table_ids);

        let info = match &self.metadata_manager {
            MetadataManager::V1(mgr) => {
                let core = mgr.fragment_manager.get_fragment_read_guard().await;
                core.table_fragments()
                    .values()
                    .filter(|tf| table_ids.contains(&tf.table_id().table_id))
                    .map(|tf| {
                        (
                            tf.table_id().table_id,
                            TableFragmentInfo {
                                fragments: tf
                                    .fragments
                                    .iter()
                                    .map(|(&id, fragment)| FragmentInfo {
                                        id,
                                        actors: fragment
                                            .actors
                                            .iter()
                                            .map(|actor| ActorInfo {
                                                id: actor.actor_id,
                                                node: actor.nodes.clone(),
                                                dispatcher: actor.dispatcher.clone(),
                                            })
                                            .collect_vec(),
                                    })
                                    .collect_vec(),
                                ctx: Some(tf.ctx.to_protobuf()),
                            },
                        )
                    })
                    .collect()
            }
            MetadataManager::V2(mgr) => {
                let mut info = HashMap::new();
                for job_id in table_ids {
                    let pb_table_fragments = mgr
                        .catalog_controller
                        .get_job_fragments_by_id(job_id as _)
                        .await?;
                    info.insert(
                        pb_table_fragments.table_id,
                        TableFragmentInfo {
                            fragments: pb_table_fragments
                                .fragments
                                .into_iter()
                                .map(|(id, fragment)| FragmentInfo {
                                    id,
                                    actors: fragment
                                        .actors
                                        .into_iter()
                                        .map(|actor| ActorInfo {
                                            id: actor.actor_id,
                                            node: actor.nodes,
                                            dispatcher: actor.dispatcher,
                                        })
                                        .collect_vec(),
                                })
                                .collect_vec(),
                            ctx: pb_table_fragments.ctx,
                        },
                    );
                }
                info
            }
        };

        Ok(Response::new(ListTableFragmentsResponse {
            table_fragments: info,
        }))
    }

    #[cfg_attr(coverage, coverage(off))]
    async fn list_table_fragment_states(
        &self,
        _request: Request<ListTableFragmentStatesRequest>,
    ) -> Result<Response<ListTableFragmentStatesResponse>, Status> {
        let states = match &self.metadata_manager {
            MetadataManager::V1(mgr) => {
                let core = mgr.fragment_manager.get_fragment_read_guard().await;
                core.table_fragments()
                    .values()
                    .map(
                        |tf| list_table_fragment_states_response::TableFragmentState {
                            table_id: tf.table_id().table_id,
                            state: tf.state() as i32,
                            parallelism: Some(tf.assigned_parallelism.into()),
                        },
                    )
                    .collect_vec()
            }
            MetadataManager::V2(mgr) => {
                let job_states = mgr.catalog_controller.list_streaming_job_states().await?;
                job_states
                    .into_iter()
                    .map(|(table_id, state, parallelism)| {
                        let parallelism = match parallelism {
                            StreamingParallelism::Adaptive => model::TableParallelism::Adaptive,
                            StreamingParallelism::Custom => model::TableParallelism::Custom,
                            StreamingParallelism::Fixed(n) => {
                                model::TableParallelism::Fixed(n as _)
                            }
                        };

                        list_table_fragment_states_response::TableFragmentState {
                            table_id: table_id as _,
                            state: PbState::from(state) as _,
                            parallelism: Some(parallelism.into()),
                        }
                    })
                    .collect_vec()
            }
        };

        Ok(Response::new(ListTableFragmentStatesResponse { states }))
    }

    #[cfg_attr(coverage, coverage(off))]
    async fn list_fragment_distribution(
        &self,
        _request: Request<ListFragmentDistributionRequest>,
    ) -> Result<Response<ListFragmentDistributionResponse>, Status> {
        let distributions = match &self.metadata_manager {
            MetadataManager::V1(mgr) => {
                let core = mgr.fragment_manager.get_fragment_read_guard().await;
                core.table_fragments()
                    .values()
                    .flat_map(|tf| {
                        let table_id = tf.table_id().table_id;
                        tf.fragments.iter().map(move |(&fragment_id, fragment)| {
                            list_fragment_distribution_response::FragmentDistribution {
                                fragment_id,
                                table_id,
                                distribution_type: fragment.distribution_type,
                                state_table_ids: fragment.state_table_ids.clone(),
                                upstream_fragment_ids: fragment.upstream_fragment_ids.clone(),
                                fragment_type_mask: fragment.fragment_type_mask,
                                parallelism: fragment.actors.len() as _,
                            }
                        })
                    })
                    .collect_vec()
            }
            MetadataManager::V2(mgr) => {
                let fragment_descs = mgr.catalog_controller.list_fragment_descs().await?;
                fragment_descs
                    .into_iter()
                    .map(|fragment_desc| {
                        list_fragment_distribution_response::FragmentDistribution {
                            fragment_id: fragment_desc.fragment_id as _,
                            table_id: fragment_desc.job_id as _,
                            distribution_type: PbFragmentDistributionType::from(
                                fragment_desc.distribution_type,
                            ) as _,
                            state_table_ids: fragment_desc.state_table_ids.into_u32_array(),
                            upstream_fragment_ids: fragment_desc
                                .upstream_fragment_id
                                .into_u32_array(),
                            fragment_type_mask: fragment_desc.fragment_type_mask as _,
                            parallelism: fragment_desc.parallelism as _,
                        }
                    })
                    .collect_vec()
            }
        };

        Ok(Response::new(ListFragmentDistributionResponse {
            distributions,
        }))
    }

    #[cfg_attr(coverage, coverage(off))]
    async fn list_actor_states(
        &self,
        _request: Request<ListActorStatesRequest>,
    ) -> Result<Response<ListActorStatesResponse>, Status> {
        let states = match &self.metadata_manager {
            MetadataManager::V1(mgr) => {
                let core = mgr.fragment_manager.get_fragment_read_guard().await;
                core.table_fragments()
                    .values()
                    .flat_map(|tf| {
                        let actor_to_fragment = tf.actor_fragment_mapping();
                        tf.actor_status.iter().map(move |(&actor_id, status)| {
                            list_actor_states_response::ActorState {
                                actor_id,
                                fragment_id: actor_to_fragment[&actor_id],
                                state: status.state,
                                worker_id: status.worker_id(),
                            }
                        })
                    })
                    .collect_vec()
            }
            MetadataManager::V2(mgr) => {
                let actor_locations = mgr.catalog_controller.list_actor_locations().await?;
                actor_locations
                    .into_iter()
                    .map(|actor_location| list_actor_states_response::ActorState {
                        actor_id: actor_location.actor_id as _,
                        fragment_id: actor_location.fragment_id as _,
                        state: PbActorState::from(actor_location.status) as _,
                        worker_id: actor_location.worker_id as _,
                    })
                    .collect_vec()
            }
        };

        Ok(Response::new(ListActorStatesResponse { states }))
    }

    #[cfg_attr(coverage, coverage(off))]
    async fn list_object_dependencies(
        &self,
        _request: Request<ListObjectDependenciesRequest>,
    ) -> Result<Response<ListObjectDependenciesResponse>, Status> {
        let dependencies = match &self.metadata_manager {
            MetadataManager::V1(mgr) => mgr.catalog_manager.list_object_dependencies().await,
            MetadataManager::V2(mgr) => mgr.catalog_controller.list_object_dependencies().await?,
        };

        Ok(Response::new(ListObjectDependenciesResponse {
            dependencies,
        }))
    }

    #[cfg_attr(coverage, coverage(off))]
    async fn recover(
        &self,
        _request: Request<RecoverRequest>,
    ) -> Result<Response<RecoverResponse>, Status> {
        self.env
            .notification_manager()
            .notify_local_subscribers(LocalNotification::AdhocRecovery)
            .await;
        Ok(Response::new(RecoverResponse {}))
    }
}
