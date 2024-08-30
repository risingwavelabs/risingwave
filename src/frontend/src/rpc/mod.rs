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
use risingwave_pb::ddl_service::{ReplaceTablePlan, TableSchemaChange};
use risingwave_pb::frontend_service::frontend_service_server::FrontendService;
use risingwave_pb::frontend_service::{GetTableReplacePlanRequest, GetTableReplacePlanResponse};
use risingwave_pb::monitor_service::monitor_service_server::MonitorService;
use risingwave_pb::monitor_service::{
    AnalyzeHeapRequest, AnalyzeHeapResponse, GetBackPressureRequest, GetBackPressureResponse,
    HeapProfilingRequest, HeapProfilingResponse, ListHeapProfilingRequest,
    ListHeapProfilingResponse, ProfilingRequest, ProfilingResponse, StackTraceRequest,
    StackTraceResponse, TieredCacheTracingRequest, TieredCacheTracingResponse,
};
use risingwave_rpc_client::error::ToTonicStatus;
use risingwave_sqlparser::ast::ObjectName;
use tonic::{Request as RpcRequest, Request, Response as RpcResponse, Response, Status};

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
pub struct FrontendServiceImpl {}

impl FrontendServiceImpl {
    pub fn new() -> Self {
        Self {}
    }
}

#[async_trait::async_trait]
impl FrontendService for FrontendServiceImpl {
    async fn get_table_replace_plan(
        &self,
        request: RpcRequest<GetTableReplacePlanRequest>,
    ) -> Result<RpcResponse<GetTableReplacePlanResponse>, Status> {
        let req = request.into_inner();
        tracing::info!("get_table_replace_plan for table {}", req.table_name);

        let table_change = req.table_change.expect("schema change message is required");
        let replace_plan =
            get_new_table_plan(table_change, req.table_name, req.database_id, req.owner).await?;

        Ok(RpcResponse::new(GetTableReplacePlanResponse {
            replace_plan: Some(replace_plan),
        }))
    }
}

/// Get the new table plan for the given table schema change
async fn get_new_table_plan(
    table_change: TableSchemaChange,
    table_name: String,
    database_id: u32,
    owner: u32,
) -> Result<ReplaceTablePlan, AutoSchemaChangeError> {
    tracing::info!("get_new_table_plan for table {}", table_name);

    let session_mgr = SESSION_MANAGER
        .get()
        .expect("session manager has been initialized");

    // get a session object for the corresponding user and database
    let session = session_mgr.create_dummy_session(database_id, owner)?;

    let new_columns = table_change.columns.into_iter().map(|c| c.into()).collect();
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
        source: None, // none for cdc table
        job_type: job_type as _,
    })
}

pub struct MonitorServiceImpl {
    await_tree_reg: Option<await_tree::Registry>,
}

pub mod await_tree_key {
    #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
    pub enum Frontend {
        Query { task_id: u64 },
    }
}

impl MonitorServiceImpl {
    pub fn new(await_tree_reg: Option<await_tree::Registry>) -> Self {
        Self { await_tree_reg }
    }
}

#[async_trait::async_trait]
impl MonitorService for MonitorServiceImpl {
    async fn stack_trace(
        &self,
        _request: Request<StackTraceRequest>,
    ) -> Result<Response<StackTraceResponse>, Status> {
        let compaction_task_traces = match &self.await_tree_reg {
            None => Default::default(),
            Some(await_tree_reg) => await_tree_reg
                .collect::<await_tree_key::Frontend>()
                .into_iter()
                .map(|(k, v)| (format!("{k:?}"), v.to_string()))
                .collect(),
        };
        Ok(Response::new(StackTraceResponse {
            compaction_task_traces,
            ..Default::default()
        }))
    }

    async fn profiling(
        &self,
        _request: Request<ProfilingRequest>,
    ) -> Result<Response<ProfilingResponse>, Status> {
        Err(Status::unimplemented("profiling unimplemented in frontend"))
    }

    async fn heap_profiling(
        &self,
        _request: Request<HeapProfilingRequest>,
    ) -> Result<Response<HeapProfilingResponse>, Status> {
        Err(Status::unimplemented(
            "Heap profiling unimplemented in frontend",
        ))
    }

    async fn list_heap_profiling(
        &self,
        _request: Request<ListHeapProfilingRequest>,
    ) -> Result<Response<ListHeapProfilingResponse>, Status> {
        Err(Status::unimplemented(
            "Heap profiling unimplemented in frontend",
        ))
    }

    async fn analyze_heap(
        &self,
        _request: Request<AnalyzeHeapRequest>,
    ) -> Result<Response<AnalyzeHeapResponse>, Status> {
        Err(Status::unimplemented(
            "Heap profiling unimplemented in frontend",
        ))
    }

    async fn get_back_pressure(
        &self,
        _request: Request<GetBackPressureRequest>,
    ) -> Result<Response<GetBackPressureResponse>, Status> {
        Err(Status::unimplemented(
            "Get Back Pressure unimplemented in frontend",
        ))
    }

    async fn tiered_cache_tracing(
        &self,
        _: Request<TieredCacheTracingRequest>,
    ) -> Result<Response<TieredCacheTracingResponse>, Status> {
        Err(Status::unimplemented(
            "Tiered Cache Tracing unimplemented in frontend",
        ))
    }
}
