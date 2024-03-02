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

use std::collections::HashMap;

use risingwave_common::catalog;
use risingwave_meta::manager::MetadataManager;
use risingwave_meta::model::TableParallelism;
use risingwave_meta::stream::{ScaleControllerRef, TableRevision};
use risingwave_meta_model_v2::FragmentId;
use risingwave_pb::common::WorkerType;
use risingwave_pb::meta::scale_service_server::ScaleService;
use risingwave_pb::meta::{
    GetClusterInfoRequest, GetClusterInfoResponse, GetReschedulePlanRequest,
    GetReschedulePlanResponse, Reschedule, RescheduleRequest, RescheduleResponse,
};
use risingwave_pb::source::{ConnectorSplit, ConnectorSplits};
use tonic::{Request, Response, Status};

use crate::barrier::BarrierManagerRef;
use crate::model::MetadataModel;
use crate::stream::{
    GlobalStreamManagerRef, ParallelUnitReschedule, RescheduleOptions, SourceManagerRef,
};

pub struct ScaleServiceImpl {
    metadata_manager: MetadataManager,
    source_manager: SourceManagerRef,
    stream_manager: GlobalStreamManagerRef,
    barrier_manager: BarrierManagerRef,
    scale_controller: ScaleControllerRef,
}

impl ScaleServiceImpl {
    pub fn new(
        metadata_manager: MetadataManager,
        source_manager: SourceManagerRef,
        stream_manager: GlobalStreamManagerRef,
        barrier_manager: BarrierManagerRef,
        scale_controller: ScaleControllerRef,
    ) -> Self {
        Self {
            metadata_manager,
            source_manager,
            stream_manager,
            barrier_manager,
            scale_controller,
        }
    }

    async fn get_revision(&self) -> TableRevision {
        match &self.metadata_manager {
            MetadataManager::V1(mgr) => mgr.fragment_manager.get_revision().await,
            // todo, support table revision in meta model v2
            MetadataManager::V2(_) => Default::default(),
        }
    }
}

#[async_trait::async_trait]
impl ScaleService for ScaleServiceImpl {
    #[cfg_attr(coverage, coverage(off))]
    async fn get_cluster_info(
        &self,
        _: Request<GetClusterInfoRequest>,
    ) -> Result<Response<GetClusterInfoResponse>, Status> {
        let _reschedule_job_lock = self.stream_manager.reschedule_lock_read_guard().await;

        let table_fragments = match &self.metadata_manager {
            MetadataManager::V1(mgr) => mgr
                .fragment_manager
                .get_fragment_read_guard()
                .await
                .table_fragments()
                .values()
                .map(|tf| tf.to_protobuf())
                .collect(),
            MetadataManager::V2(mgr) => mgr
                .catalog_controller
                .table_fragments()
                .await?
                .values()
                .cloned()
                .collect(),
        };

        let worker_nodes = self
            .metadata_manager
            .list_worker_node(Some(WorkerType::ComputeNode), None)
            .await?;

        let actor_splits = self
            .source_manager
            .list_assignments()
            .await
            .into_iter()
            .map(|(actor_id, splits)| {
                (
                    actor_id,
                    ConnectorSplits {
                        splits: splits.iter().map(ConnectorSplit::from).collect(),
                    },
                )
            })
            .collect();

        let sources = self.metadata_manager.list_sources().await?;
        let source_infos = sources.into_iter().map(|s| (s.id, s)).collect();

        let revision = self.get_revision().await.inner();

        Ok(Response::new(GetClusterInfoResponse {
            worker_nodes,
            table_fragments,
            actor_splits,
            source_infos,
            revision,
        }))
    }

    #[cfg_attr(coverage, coverage(off))]
    async fn reschedule(
        &self,
        request: Request<RescheduleRequest>,
    ) -> Result<Response<RescheduleResponse>, Status> {
        self.barrier_manager.check_status_running()?;

        let RescheduleRequest {
            reschedules,
            revision,
            resolve_no_shuffle_upstream,
        } = request.into_inner();

        let _reschedule_job_lock = self.stream_manager.reschedule_lock_write_guard().await;

        let current_revision = self.get_revision().await;

        if revision != current_revision.inner() {
            return Ok(Response::new(RescheduleResponse {
                success: false,
                revision: current_revision.inner(),
            }));
        }

        let table_parallelisms = {
            match &self.metadata_manager {
                MetadataManager::V1(mgr) => {
                    let guard = mgr.fragment_manager.get_fragment_read_guard().await;

                    let mut table_parallelisms = HashMap::new();
                    for (table_id, table) in guard.table_fragments() {
                        if table
                            .fragment_ids()
                            .any(|fragment_id| reschedules.contains_key(&fragment_id))
                        {
                            table_parallelisms.insert(*table_id, TableParallelism::Custom);
                        }
                    }

                    table_parallelisms
                }
                MetadataManager::V2(mgr) => {
                    let streaming_job_ids = mgr
                        .catalog_controller
                        .get_fragment_job_id(
                            reschedules.keys().map(|id| *id as FragmentId).collect(),
                        )
                        .await?;

                    streaming_job_ids
                        .into_iter()
                        .map(|id| (catalog::TableId::new(id as _), TableParallelism::Custom))
                        .collect()
                }
            }
        };

        self.stream_manager
            .reschedule_actors(
                reschedules
                    .into_iter()
                    .map(|(fragment_id, reschedule)| {
                        let Reschedule {
                            added_parallel_units,
                            removed_parallel_units,
                        } = reschedule;

                        let added_parallel_units = added_parallel_units.into_iter().collect();
                        let removed_parallel_units = removed_parallel_units.into_iter().collect();

                        (
                            fragment_id,
                            ParallelUnitReschedule {
                                added_parallel_units,
                                removed_parallel_units,
                            },
                        )
                    })
                    .collect(),
                RescheduleOptions {
                    resolve_no_shuffle_upstream,
                },
                Some(table_parallelisms),
            )
            .await?;

        let next_revision = self.get_revision().await;

        Ok(Response::new(RescheduleResponse {
            success: true,
            revision: next_revision.into(),
        }))
    }

    #[cfg_attr(coverage, coverage(off))]
    async fn get_reschedule_plan(
        &self,
        request: Request<GetReschedulePlanRequest>,
    ) -> Result<Response<GetReschedulePlanResponse>, Status> {
        self.barrier_manager.check_status_running()?;

        let req = request.into_inner();

        let _reschedule_job_lock = self.stream_manager.reschedule_lock_read_guard().await;

        let current_revision = self.get_revision().await;

        if req.revision != current_revision.inner() {
            return Ok(Response::new(GetReschedulePlanResponse {
                success: false,
                revision: current_revision.inner(),
                reschedules: Default::default(),
            }));
        }

        let policy = req
            .policy
            .ok_or_else(|| Status::invalid_argument("policy is required"))?;

        let scale_controller = &self.scale_controller;

        let plan = scale_controller.get_reschedule_plan(policy).await?;

        let next_revision = self.get_revision().await;

        // generate reschedule plan will not change the revision
        assert_eq!(current_revision, next_revision);

        Ok(Response::new(GetReschedulePlanResponse {
            success: true,
            revision: next_revision.into(),
            reschedules: plan
                .into_iter()
                .map(|(fragment_id, reschedule)| {
                    (
                        fragment_id,
                        Reschedule {
                            added_parallel_units: reschedule
                                .added_parallel_units
                                .into_iter()
                                .collect(),
                            removed_parallel_units: reschedule
                                .removed_parallel_units
                                .into_iter()
                                .collect(),
                        },
                    )
                })
                .collect(),
        }))
    }
}
