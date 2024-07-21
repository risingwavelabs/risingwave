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

use pgwire::pg_server::SessionManager;
use risingwave_pb::frontend_service::schema_change_request::Request;
use risingwave_pb::frontend_service::schema_change_service_server::SchemaChangeService;
use risingwave_pb::frontend_service::{SchemaChangeRequest, SchemaChangeResponse};
use tonic::{Request as RpcRequest, Response as RpcResponse, Status};

use crate::session::SESSION_MANAGER;

#[derive(Default)]
pub struct SchemaChangeServiceImpl {}

impl SchemaChangeServiceImpl {
    pub fn new() -> Self {
        Self {}
    }
}

#[async_trait::async_trait]
impl SchemaChangeService for SchemaChangeServiceImpl {
    async fn get_new_table_streaming_graph(
        &self,
        request: RpcRequest<SchemaChangeRequest>,
    ) -> Result<RpcResponse<SchemaChangeResponse>, Status> {
        let req = request.into_inner();

        match req.request.unwrap() {
            Request::ReplaceTablePlan(req) => {
                let change = req.schema_change.expect("schema_change");
                // get a session object
                let session_mgr = SESSION_MANAGER
                    .get()
                    .expect("session manager has been initialized");
                let _session = session_mgr
                    .get_session(req.database_id, &req.owner)
                    .map_err(|e| Status::internal(format!("Failed to get session: {}", e)))?;

                // call the handle alter method
            }
        };

        Ok(RpcResponse::new(SchemaChangeResponse { response: None }))
    }
}
