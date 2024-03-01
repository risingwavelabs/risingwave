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

use await_tree::InstrumentAwait;
use itertools::Itertools;
use risingwave_hummock_sdk::table_stats::to_prost_table_stats_map;
use risingwave_hummock_sdk::LocalSstableInfo;
use risingwave_pb::stream_service::barrier_complete_response::GroupedSstableInfo;
use risingwave_pb::stream_service::stream_service_server::StreamService;
use risingwave_pb::stream_service::*;
use risingwave_storage::dispatch_state_store;
use risingwave_stream::error::StreamError;
use risingwave_stream::executor::Barrier;
use risingwave_stream::task::{BarrierCompleteResult, LocalStreamManager, StreamEnvironment};
use thiserror_ext::AsReport;
use tonic::{Request, Response, Status};

#[derive(Clone)]
pub struct StreamServiceImpl {
    mgr: LocalStreamManager,
    env: StreamEnvironment,
}

impl StreamServiceImpl {
    pub fn new(mgr: LocalStreamManager, env: StreamEnvironment) -> Self {
        StreamServiceImpl { mgr, env }
    }
}

#[async_trait::async_trait]
impl StreamService for StreamServiceImpl {
    #[cfg_attr(coverage, coverage(off))]
    async fn update_actors(
        &self,
        request: Request<UpdateActorsRequest>,
    ) -> std::result::Result<Response<UpdateActorsResponse>, Status> {
        let req = request.into_inner();
        let res = self.mgr.update_actors(req.actors).await;
        match res {
            Err(e) => {
                error!(error = %e.as_report(), "failed to update stream actor");
                Err(e.into())
            }
            Ok(()) => Ok(Response::new(UpdateActorsResponse { status: None })),
        }
    }

    #[cfg_attr(coverage, coverage(off))]
    async fn build_actors(
        &self,
        request: Request<BuildActorsRequest>,
    ) -> std::result::Result<Response<BuildActorsResponse>, Status> {
        let req = request.into_inner();

        let actor_id = req.actor_id;
        let res = self.mgr.build_actors(actor_id).await;
        match res {
            Err(e) => {
                error!(error = %e.as_report(), "failed to build actors");
                Err(e.into())
            }
            Ok(()) => Ok(Response::new(BuildActorsResponse {
                request_id: req.request_id,
                status: None,
            })),
        }
    }

    #[cfg_attr(coverage, coverage(off))]
    async fn broadcast_actor_info_table(
        &self,
        request: Request<BroadcastActorInfoTableRequest>,
    ) -> std::result::Result<Response<BroadcastActorInfoTableResponse>, Status> {
        let req = request.into_inner();

        let res = self.mgr.update_actor_info(req.info).await;
        match res {
            Err(e) => {
                error!(error = %e.as_report(), "failed to update actor info table actor");
                Err(e.into())
            }
            Ok(()) => Ok(Response::new(BroadcastActorInfoTableResponse {
                status: None,
            })),
        }
    }

    #[cfg_attr(coverage, coverage(off))]
    async fn drop_actors(
        &self,
        request: Request<DropActorsRequest>,
    ) -> std::result::Result<Response<DropActorsResponse>, Status> {
        let req = request.into_inner();
        let actors = req.actor_ids;
        self.mgr.drop_actors(actors).await?;
        Ok(Response::new(DropActorsResponse {
            request_id: req.request_id,
            status: None,
        }))
    }

    #[cfg_attr(coverage, coverage(off))]
    async fn force_stop_actors(
        &self,
        request: Request<ForceStopActorsRequest>,
    ) -> std::result::Result<Response<ForceStopActorsResponse>, Status> {
        let req = request.into_inner();
        self.mgr.reset(req.prev_epoch).await;
        Ok(Response::new(ForceStopActorsResponse {
            request_id: req.request_id,
            status: None,
        }))
    }

    #[cfg_attr(coverage, coverage(off))]
    async fn inject_barrier(
        &self,
        request: Request<InjectBarrierRequest>,
    ) -> Result<Response<InjectBarrierResponse>, Status> {
        let req = request.into_inner();
        let barrier =
            Barrier::from_protobuf(req.get_barrier().unwrap()).map_err(StreamError::from)?;

        self.mgr
            .send_barrier(barrier, req.actor_ids_to_send, req.actor_ids_to_collect)
            .await?;

        Ok(Response::new(InjectBarrierResponse {
            request_id: req.request_id,
            status: None,
        }))
    }

    #[cfg_attr(coverage, coverage(off))]
    async fn barrier_complete(
        &self,
        request: Request<BarrierCompleteRequest>,
    ) -> Result<Response<BarrierCompleteResponse>, Status> {
        let req = request.into_inner();
        let BarrierCompleteResult {
            create_mview_progress,
            sync_result,
        } = self
            .mgr
            .collect_barrier(req.prev_epoch)
            .instrument_await(format!("collect_barrier (epoch {})", req.prev_epoch))
            .await
            .inspect_err(
                |err| tracing::error!(error = %err.as_report(), "failed to collect barrier"),
            )?;

        let (synced_sstables, table_watermarks) = sync_result
            .map(|sync_result| (sync_result.uncommitted_ssts, sync_result.table_watermarks))
            .unwrap_or_default();

        Ok(Response::new(BarrierCompleteResponse {
            request_id: req.request_id,
            status: None,
            create_mview_progress,
            synced_sstables: synced_sstables
                .into_iter()
                .map(
                    |LocalSstableInfo {
                         compaction_group_id,
                         sst_info,
                         table_stats,
                     }| GroupedSstableInfo {
                        compaction_group_id,
                        sst: Some(sst_info.to_protobuf()),
                        table_stats_map: to_prost_table_stats_map(table_stats),
                    },
                )
                .collect_vec(),
            worker_id: self.env.worker_id(),
            table_watermarks: table_watermarks
                .into_iter()
                .map(|(key, value)| (key.table_id, value.to_protobuf()))
                .collect(),
        }))
    }

    #[cfg_attr(coverage, coverage(off))]
    async fn wait_epoch_commit(
        &self,
        request: Request<WaitEpochCommitRequest>,
    ) -> Result<Response<WaitEpochCommitResponse>, Status> {
        let epoch = request.into_inner().epoch;

        dispatch_state_store!(self.env.state_store(), store, {
            use risingwave_hummock_sdk::HummockReadEpoch;
            use risingwave_storage::StateStore;

            store
                .try_wait_epoch(HummockReadEpoch::Committed(epoch))
                .instrument_await(format!("wait_epoch_commit (epoch {})", epoch))
                .await
                .map_err(StreamError::from)?;
        });

        Ok(Response::new(WaitEpochCommitResponse { status: None }))
    }
}
