// Copyright 2022 Singularity Data
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::sync::Arc;

use itertools::Itertools;
use risingwave_common::catalog::TableId;
use risingwave_common::error::{tonic_err, Result as RwResult};
use risingwave_pb::catalog::Source;
use risingwave_pb::stream_service::stream_service_server::StreamService;
use risingwave_pb::stream_service::*;
use risingwave_stream::executor::{Barrier, Epoch};
use risingwave_stream::task::{LocalStreamManager, StreamEnvironment};
use tonic::{Request, Response, Status};

#[derive(Clone)]
pub struct StreamServiceImpl {
    mgr: Arc<LocalStreamManager>,
    env: StreamEnvironment,
}

impl StreamServiceImpl {
    pub fn new(mgr: Arc<LocalStreamManager>, env: StreamEnvironment) -> Self {
        StreamServiceImpl { mgr, env }
    }
}

#[async_trait::async_trait]
impl StreamService for StreamServiceImpl {
    #[cfg_attr(coverage, no_coverage)]
    async fn update_actors(
        &self,
        request: Request<UpdateActorsRequest>,
    ) -> std::result::Result<Response<UpdateActorsResponse>, Status> {
        let req = request.into_inner();
        let res = self.mgr.update_actors(&req.actors, &req.hanging_channels);
        match res {
            Err(e) => {
                error!("failed to update stream actor {}", e);
                Err(e.to_grpc_status())
            }
            Ok(()) => Ok(Response::new(UpdateActorsResponse { status: None })),
        }
    }

    #[cfg_attr(coverage, no_coverage)]
    async fn build_actors(
        &self,
        request: Request<BuildActorsRequest>,
    ) -> std::result::Result<Response<BuildActorsResponse>, Status> {
        let req = request.into_inner();

        let actor_id = req.actor_id;
        let res = self.mgr.build_actors(actor_id.as_slice(), self.env.clone());
        match res {
            Err(e) => {
                error!("failed to build actors {}", e);
                Err(e.to_grpc_status())
            }
            Ok(()) => Ok(Response::new(BuildActorsResponse {
                request_id: req.request_id,
                status: None,
            })),
        }
    }

    #[cfg_attr(coverage, no_coverage)]
    async fn broadcast_actor_info_table(
        &self,
        request: Request<BroadcastActorInfoTableRequest>,
    ) -> std::result::Result<Response<BroadcastActorInfoTableResponse>, Status> {
        let table = request.into_inner();

        let res = self.mgr.update_actor_info(table);
        match res {
            Err(e) => {
                error!("failed to update actor info table actor {}", e);
                Err(e.to_grpc_status())
            }
            Ok(()) => Ok(Response::new(BroadcastActorInfoTableResponse {
                status: None,
            })),
        }
    }

    #[cfg_attr(coverage, no_coverage)]
    async fn drop_actors(
        &self,
        request: Request<DropActorsRequest>,
    ) -> std::result::Result<Response<DropActorsResponse>, Status> {
        let req = request.into_inner();
        let actors = req.actor_ids;
        self.mgr
            .drop_actor(&actors)
            .map_err(|e| e.to_grpc_status())?;
        Ok(Response::new(DropActorsResponse {
            request_id: req.request_id,
            status: None,
        }))
    }

    #[cfg_attr(coverage, no_coverage)]
    async fn force_stop_actors(
        &self,
        request: Request<ForceStopActorsRequest>,
    ) -> std::result::Result<Response<ForceStopActorsResponse>, Status> {
        let req = request.into_inner();
        let epoch = req.epoch.unwrap();
        self.mgr
            .stop_all_actors(Epoch {
                curr: epoch.curr,
                prev: epoch.prev,
            })
            .await
            .map_err(|e| e.to_grpc_status())?;
        Ok(Response::new(ForceStopActorsResponse {
            request_id: req.request_id,
            status: None,
        }))
    }

    #[cfg_attr(coverage, no_coverage)]
    async fn inject_barrier(
        &self,
        request: Request<InjectBarrierRequest>,
    ) -> Result<Response<InjectBarrierResponse>, Status> {
        let req = request.into_inner();
        let barrier =
            Barrier::from_protobuf(req.get_barrier().map_err(tonic_err)?).map_err(tonic_err)?;

        let collect_result = self
            .mgr
            .send_and_collect_barrier(
                &barrier,
                req.actor_ids_to_send,
                req.actor_ids_to_collect,
                true,
            )
            .await
            .map_err(|e| e.to_grpc_status())?;

        let finished_create_mviews = collect_result
            .finished_create_mviews
            .into_iter()
            .map(Into::into)
            .collect();

        Ok(Response::new(InjectBarrierResponse {
            request_id: req.request_id,
            finished_create_mviews,
            status: None,
        }))
    }

    #[cfg_attr(coverage, no_coverage)]
    async fn create_source(
        &self,
        request: Request<CreateSourceRequest>,
    ) -> Result<Response<CreateSourceResponse>, Status> {
        let source = request.into_inner().source.unwrap();
        self.create_source_inner(&source).map_err(tonic_err)?;
        tracing::debug!(id = %source.id, "create table source");

        Ok(Response::new(CreateSourceResponse { status: None }))
    }

    #[cfg_attr(coverage, no_coverage)]
    async fn sync_sources(
        &self,
        request: Request<SyncSourcesRequest>,
    ) -> Result<Response<SyncSourcesResponse>, Status> {
        let sources = request.into_inner().sources;
        self.env
            .source_manager()
            .clear_sources()
            .map_err(tonic_err)?;
        for source in sources {
            self.create_source_inner(&source).map_err(tonic_err)?;
        }

        Ok(Response::new(SyncSourcesResponse { status: None }))
    }

    #[cfg_attr(coverage, no_coverage)]
    async fn drop_source(
        &self,
        request: Request<DropSourceRequest>,
    ) -> Result<Response<DropSourceResponse>, Status> {
        let id = request.into_inner().source_id;
        let id = TableId::new(id); // TODO: use SourceId instead

        self.env
            .source_manager()
            .drop_source(&id)
            .map_err(tonic_err)?;

        tracing::debug!(id = %id, "drop source");

        Ok(Response::new(DropSourceResponse { status: None }))
    }
}

impl StreamServiceImpl {
    fn create_source_inner(&self, source: &Source) -> RwResult<()> {
        use risingwave_pb::catalog::source::Info;

        let id = TableId::new(source.id); // TODO: use SourceId instead

        match &source.get_info()? {
            Info::StreamSource(info) => {
                self.env
                    .source_manager()
                    .create_source(&id, info.to_owned())?;
            }
            Info::TableSource(info) => {
                let columns = info
                    .columns
                    .iter()
                    .cloned()
                    .map(|c| c.column_desc.unwrap().into())
                    .collect_vec();

                self.env
                    .source_manager()
                    .create_table_source(&id, columns)?;
            }
        };

        Ok(())
    }
}
