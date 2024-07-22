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

use pgwire::pg_server::{BoxedError, SessionManager};
use risingwave_pb::ddl_service::{ReplaceTablePlan, SchemaChangeEnvelope};
use risingwave_pb::frontend_service::schema_change_request::Request;
use risingwave_pb::frontend_service::schema_change_response::{GetNewTablePlanResponse, Response};
use risingwave_pb::frontend_service::schema_change_service_server::SchemaChangeService;
use risingwave_pb::frontend_service::{SchemaChangeRequest, SchemaChangeResponse};
use risingwave_rpc_client::error::ToTonicStatus;
use risingwave_sqlparser::ast::ObjectName;
use tonic::{Request as RpcRequest, Response as RpcResponse, Status};

use crate::error::RwError;
use crate::handler::{get_new_table_definition_for_cdc_table, get_replace_table_plan};
use crate::session::SESSION_MANAGER;

#[derive(thiserror::Error, Debug)]
pub enum AutoSchemaChangeError {
    #[error("frontend error")]
    FrontendError(
        #[from]
        #[backtrace]
        RwError,
    ),
}

impl From<BoxedError> for AutoSchemaChangeError {
    fn from(err: BoxedError) -> Self {
        AutoSchemaChangeError::FrontendError(RwError::from(err))
    }
}

impl From<AutoSchemaChangeError> for tonic::Status {
    fn from(err: AutoSchemaChangeError) -> Self {
        err.to_status(tonic::Code::Internal, "frontend")
    }
}

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

        if let Some(Request::GetNewTablePlan(req)) = req.request {
            let change = req
                .schema_change
                .expect("schema change message is required");
            let replace_plan =
                get_new_table_plan(change, req.table_name, req.database_id, req.owner).await?;
            Ok(RpcResponse::new(SchemaChangeResponse {
                response: Some(Response::ReplaceTablePlan(GetNewTablePlanResponse {
                    table_plan: Some(replace_plan),
                })),
            }))
        } else {
            Err(Status::invalid_argument("invalid schema change request"))
        }
    }
}

async fn get_new_table_plan(
    change: SchemaChangeEnvelope,
    table_name: String,
    database_id: u32,
    owner: String,
) -> Result<ReplaceTablePlan, AutoSchemaChangeError> {
    let session_mgr = SESSION_MANAGER
        .get()
        .expect("session manager has been initialized");

    // get a session object for the corresponding user and database
    let session = session_mgr.get_session(database_id, &owner)?;

    // call the handle alter method
    let new_columns = change.column_descs.into_iter().map(|c| c.into()).collect();
    let table_name = ObjectName::from(vec![table_name.as_str().into()]);
    let (new_table_definition, original_catalog) =
        get_new_table_definition_for_cdc_table(&session, table_name.clone(), new_columns).await?;
    let (_, table, graph, col_index_mapping, job_type) = get_replace_table_plan(
        &session,
        table_name,
        new_table_definition,
        &original_catalog,
        None,
    )
    .await?;

    Ok(ReplaceTablePlan {
        table: Some(table),
        fragment_graph: Some(graph),
        table_col_index_mapping: Some(col_index_mapping.to_protobuf()),
        source: None,
        job_type: job_type as _,
    })
}
