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

use risingwave_meta::manager::event_log::EventLogManagerRef;
use risingwave_pb::meta::event_log_service_server::EventLogService;
use risingwave_pb::meta::{
    AddEventLogRequest, AddEventLogResponse, ListEventLogRequest, ListEventLogResponse,
};
use tonic::{Request, Response, Status};

pub struct EventLogServiceImpl {
    event_log_manager: EventLogManagerRef,
}

impl EventLogServiceImpl {
    pub fn new(event_log_manager: EventLogManagerRef) -> Self {
        Self { event_log_manager }
    }
}

#[async_trait::async_trait]
impl EventLogService for EventLogServiceImpl {
    async fn list_event_log(
        &self,
        _request: Request<ListEventLogRequest>,
    ) -> Result<Response<ListEventLogResponse>, Status> {
        let event_logs = self.event_log_manager.list_event_logs();
        Ok(Response::new(ListEventLogResponse { event_logs }))
    }

    async fn add_event_log(
        &self,
        request: Request<AddEventLogRequest>,
    ) -> Result<Response<AddEventLogResponse>, Status> {
        let Some(event) = request.into_inner().event else {
            return Ok(Response::new(AddEventLogResponse {}));
        };
        let e = match event {
            risingwave_pb::meta::add_event_log_request::Event::WorkerNodePanic(e) => {
                risingwave_pb::meta::event_log::Event::WorkerNodePanic(e)
            }
            risingwave_pb::meta::add_event_log_request::Event::SinkFail(e) => {
                risingwave_pb::meta::event_log::Event::SinkFail(e)
            }
            risingwave_pb::meta::add_event_log_request::Event::AutoSchemaChangeFail(e) => {
                risingwave_pb::meta::event_log::Event::AutoSchemaChangeFail(e)
            }
        };
        self.event_log_manager.add_event_logs(vec![e]);
        Ok(Response::new(AddEventLogResponse {}))
    }
}
