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

use std::collections::{HashMap, HashSet};

use itertools::Itertools;
use risingwave_common::catalog::TableId;
use risingwave_pb::meta::list_table_fragments_response::{
    ActorInfo, FragmentInfo, TableFragmentInfo,
};
use risingwave_pb::meta::stream_manager_service_server::StreamManagerService;
use risingwave_pb::meta::*;
use tonic::{Request, Response, Status};

use crate::barrier::BarrierScheduler;
use crate::manager::{CatalogManagerRef, FragmentManagerRef, MetaSrvEnv};
use crate::storage::MetaStore;
use crate::stream::GlobalStreamManagerRef;

pub type TonicResponse<T> = Result<Response<T>, Status>;

#[derive(Clone)]
pub struct StreamServiceImpl<S>
where
    S: MetaStore,
{
    env: MetaSrvEnv<S>,
    barrier_scheduler: BarrierScheduler<S>,
    stream_manager: GlobalStreamManagerRef<S>,
    catalog_manager: CatalogManagerRef<S>,
    fragment_manager: FragmentManagerRef<S>,
}

impl<S> StreamServiceImpl<S>
where
    S: MetaStore,
{
    pub fn new(
        env: MetaSrvEnv<S>,
        barrier_scheduler: BarrierScheduler<S>,
        stream_manager: GlobalStreamManagerRef<S>,
        catalog_manager: CatalogManagerRef<S>,
        fragment_manager: FragmentManagerRef<S>,
    ) -> Self {
        StreamServiceImpl {
            env,
            barrier_scheduler,
            stream_manager,
            catalog_manager,
            fragment_manager,
        }
    }
}

#[async_trait::async_trait]
impl<S> StreamManagerService for StreamServiceImpl<S>
where
    S: MetaStore,
{
    #[cfg_attr(coverage, no_coverage)]
    async fn flush(&self, request: Request<FlushRequest>) -> TonicResponse<FlushResponse> {
        self.env.idle_manager().record_activity();
        let req = request.into_inner();

        let snapshot = self.barrier_scheduler.flush(req.checkpoint).await?;
        Ok(Response::new(FlushResponse {
            status: None,
            snapshot: Some(snapshot),
        }))
    }

    async fn cancel_creating_jobs(
        &self,
        request: Request<CancelCreatingJobsRequest>,
    ) -> TonicResponse<CancelCreatingJobsResponse> {
        let req = request.into_inner();
        let table_ids = self
            .catalog_manager
            .find_creating_streaming_job_ids(req.infos)
            .await;
        if !table_ids.is_empty() {
            self.stream_manager
                .cancel_streaming_jobs(table_ids.into_iter().map(TableId::from).collect_vec())
                .await;
        }
        Ok(Response::new(CancelCreatingJobsResponse { status: None }))
    }

    #[cfg_attr(coverage, no_coverage)]
    async fn list_table_fragments(
        &self,
        request: Request<ListTableFragmentsRequest>,
    ) -> Result<Response<ListTableFragmentsResponse>, Status> {
        let req = request.into_inner();
        let table_ids = HashSet::<u32>::from_iter(req.table_ids);
        let core = self.fragment_manager.get_fragment_read_guard().await;
        let info = core
            .table_fragments()
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
                        env: Some(tf.env.to_protobuf()),
                    },
                )
            })
            .collect::<HashMap<u32, TableFragmentInfo>>();

        Ok(Response::new(ListTableFragmentsResponse {
            table_fragments: info,
        }))
    }

    #[cfg_attr(coverage, no_coverage)]
    async fn list_table_fragment_states(
        &self,
        _request: Request<ListTableFragmentStatesRequest>,
    ) -> Result<Response<ListTableFragmentStatesResponse>, Status> {
        let core = self.fragment_manager.get_fragment_read_guard().await;

        Ok(Response::new(ListTableFragmentStatesResponse {
            states: core
                .table_fragments()
                .values()
                .map(
                    |tf| list_table_fragment_states_response::TableFragmentState {
                        table_id: tf.table_id().table_id,
                        state: tf.state() as i32,
                    },
                )
                .collect_vec(),
        }))
    }

    #[cfg_attr(coverage, no_coverage)]
    async fn list_fragment_distribution(
        &self,
        _request: Request<ListFragmentDistributionRequest>,
    ) -> Result<Response<ListFragmentDistributionResponse>, Status> {
        let core = self.fragment_manager.get_fragment_read_guard().await;

        Ok(Response::new(ListFragmentDistributionResponse {
            distributions: core
                .table_fragments()
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
                        }
                    })
                })
                .collect_vec(),
        }))
    }

    #[cfg_attr(coverage, no_coverage)]
    async fn list_actor_states(
        &self,
        _request: Request<ListActorStatesRequest>,
    ) -> Result<Response<ListActorStatesResponse>, Status> {
        let core = self.fragment_manager.get_fragment_read_guard().await;

        Ok(Response::new(ListActorStatesResponse {
            states: core
                .table_fragments()
                .values()
                .flat_map(|tf| {
                    let actor_to_fragment = tf.actor_fragment_mapping();
                    tf.actor_status.iter().map(move |(&actor_id, status)| {
                        list_actor_states_response::ActorState {
                            actor_id,
                            fragment_id: actor_to_fragment[&actor_id],
                            state: status.state,
                            parallel_unit_id: status.parallel_unit.as_ref().unwrap().id,
                        }
                    })
                })
                .collect_vec(),
        }))
    }
}
