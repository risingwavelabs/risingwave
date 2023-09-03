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

use futures::{Stream, StreamExt};
use risingwave_pb::connector_service::sink_coordination_service_server::SinkCoordinationService;
use risingwave_pb::connector_service::{CoordinateRequest, CoordinateResponse};
use tonic::{Request, Response, Status, Streaming};

use crate::manager::sink_coordination::SinkCoordinatorManager;

#[derive(Clone)]
pub(crate) struct SinkCoordinationServiceImpl {
    sink_manager: SinkCoordinatorManager,
}

impl SinkCoordinationServiceImpl {
    pub(crate) fn new(sink_manager: SinkCoordinatorManager) -> Self {
        Self { sink_manager }
    }
}

#[async_trait::async_trait]
impl SinkCoordinationService for SinkCoordinationServiceImpl {
    type CoordinateStream = impl Stream<Item = Result<CoordinateResponse, Status>>;

    async fn coordinate(
        &self,
        request: Request<Streaming<CoordinateRequest>>,
    ) -> Result<Response<Self::CoordinateStream>, Status> {
        let stream = request.into_inner();
        Ok(Response::new(
            self.sink_manager.handle_new_request(stream.boxed()).await?,
        ))
    }
}
