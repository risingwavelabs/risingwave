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

use risingwave_meta::manager::{MetaSrvEnv, MetadataManager};
use risingwave_pb::common::WorkerType;
use risingwave_pb::meta::scale_service_server::ScaleService;
use risingwave_pb::meta::{
    GetClusterInfoRequest, GetClusterInfoResponse, GetServerlessStreamingJobsStatusRequest,
    GetServerlessStreamingJobsStatusResponse, RescheduleRequest, RescheduleResponse,
    UpdateStreamingJobNodeLabelsRequest, UpdateStreamingJobNodeLabelsResponse,
};
use risingwave_pb::source::{ConnectorSplit, ConnectorSplits};
use tonic::{Request, Response, Status};

use crate::barrier::BarrierManagerRef;
use crate::stream::GlobalStreamManagerRef;

pub struct ScaleServiceImpl {
    metadata_manager: MetadataManager,
    stream_manager: GlobalStreamManagerRef,
    env: MetaSrvEnv,
}

impl ScaleServiceImpl {
    pub fn new(
        metadata_manager: MetadataManager,
        stream_manager: GlobalStreamManagerRef,
        _barrier_manager: BarrierManagerRef,
        env: MetaSrvEnv,
    ) -> Self {
        Self {
            metadata_manager,
            stream_manager,
            env,
        }
    }
}

#[async_trait::async_trait]
impl ScaleService for ScaleServiceImpl {
    async fn get_cluster_info(
        &self,
        _: Request<GetClusterInfoRequest>,
    ) -> Result<Response<GetClusterInfoResponse>, Status> {
        let _reschedule_job_lock = self.stream_manager.reschedule_lock_read_guard().await;

        let stream_job_fragments = self
            .metadata_manager
            .catalog_controller
            .table_fragments()
            .await?;

        let mut table_fragments = Vec::with_capacity(stream_job_fragments.len());
        for (_, stream_job_fragments) in stream_job_fragments {
            let upstreams = self
                .metadata_manager
                .catalog_controller
                .upstream_fragments(stream_job_fragments.fragment_ids())
                .await?;
            let dispatchers = self
                .metadata_manager
                .catalog_controller
                .get_fragment_actor_dispatchers(
                    stream_job_fragments
                        .fragment_ids()
                        .map(|id| id as _)
                        .collect(),
                )
                .await?;
            table_fragments.push(stream_job_fragments.to_protobuf(&upstreams, &dispatchers))
        }

        let worker_nodes = self
            .metadata_manager
            .list_worker_node(Some(WorkerType::ComputeNode), None)
            .await?;

        let actor_splits = self
            .env
            .shared_actor_infos()
            .list_assignments()
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

        Ok(Response::new(GetClusterInfoResponse {
            worker_nodes,
            table_fragments,
            actor_splits,
            source_infos,
            revision: 0,
        }))
    }

    async fn reschedule(
        &self,
        _request: Request<RescheduleRequest>,
    ) -> Result<Response<RescheduleResponse>, Status> {
        Ok(Response::new(RescheduleResponse {
            success: false,
            revision: 0,
        }))
    }

    async fn update_streaming_job_node_labels(
        &self,
        _request: Request<UpdateStreamingJobNodeLabelsRequest>,
    ) -> Result<Response<UpdateStreamingJobNodeLabelsResponse>, Status> {
        todo!()
    }

    async fn get_serverless_streaming_jobs_status(
        &self,
        _request: Request<GetServerlessStreamingJobsStatusRequest>,
    ) -> Result<Response<GetServerlessStreamingJobsStatusResponse>, Status> {
        todo!()
    }
}
