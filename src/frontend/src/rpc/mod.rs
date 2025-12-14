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

use itertools::Itertools;
use pgwire::pg_server::{Session, SessionManager};
use risingwave_common::id::{DatabaseId, TableId};
use risingwave_pb::ddl_service::{ReplaceJobPlan, TableSchemaChange, replace_job_plan};
use risingwave_pb::frontend_service::frontend_service_server::FrontendService;
use risingwave_pb::frontend_service::{
    AllCursors, CancelRunningSqlRequest, CancelRunningSqlResponse, GetAllCursorsRequest,
    GetAllCursorsResponse, GetAllSubCursorsRequest, GetAllSubCursorsResponse,
    GetRunningSqlsRequest, GetRunningSqlsResponse, GetTableReplacePlanRequest,
    GetTableReplacePlanResponse, RunningSql, SubscriptionCursor,
};
use risingwave_sqlparser::ast::ObjectName;
use tonic::{Request as RpcRequest, Response as RpcResponse, Status};

use crate::error::RwError;
use crate::handler::create_source::SqlColumnStrategy;
use crate::handler::kill_process::handle_kill_local;
use crate::handler::{get_new_table_definition_for_cdc_table, get_replace_table_plan};
use crate::session::{SESSION_MANAGER, SessionMapRef};

#[derive(Default)]
pub struct FrontendServiceImpl {
    session_map: SessionMapRef,
}

impl FrontendServiceImpl {
    pub fn new(session_map: SessionMapRef) -> Self {
        Self { session_map }
    }
}

#[async_trait::async_trait]
impl FrontendService for FrontendServiceImpl {
    async fn get_table_replace_plan(
        &self,
        request: RpcRequest<GetTableReplacePlanRequest>,
    ) -> Result<RpcResponse<GetTableReplacePlanResponse>, Status> {
        let req = request.into_inner();

        let replace_plan =
            get_new_table_plan(req.table_id, req.database_id, req.cdc_table_change).await?;

        Ok(RpcResponse::new(GetTableReplacePlanResponse {
            replace_plan: Some(replace_plan),
        }))
    }

    async fn get_all_cursors(
        &self,
        _request: RpcRequest<GetAllCursorsRequest>,
    ) -> Result<RpcResponse<GetAllCursorsResponse>, Status> {
        let sessions = self.session_map.read().values().cloned().collect_vec();
        let mut all_cursors = vec![];
        for s in sessions {
            let mut cursor_names = vec![];
            s.get_cursor_manager()
                .iter_query_cursors(|cursor_name: &String, _| {
                    cursor_names.push(cursor_name.clone());
                })
                .await;
            all_cursors.push(AllCursors {
                session_id: s.id().0,
                user_name: s.user_name(),
                peer_addr: format!("{}", s.peer_addr()),
                database: s.database(),
                cursor_names,
            });
        }
        Ok(RpcResponse::new(GetAllCursorsResponse { all_cursors }))
    }

    async fn get_all_sub_cursors(
        &self,
        _request: RpcRequest<GetAllSubCursorsRequest>,
    ) -> Result<RpcResponse<GetAllSubCursorsResponse>, Status> {
        let sessions = self.session_map.read().values().cloned().collect_vec();
        let mut subscription_cursors = vec![];
        for s in sessions {
            let mut sub_cursor_names = vec![];
            let mut cursor_names = vec![];
            let mut states = vec![];
            let mut idle_durations = vec![];
            s.get_cursor_manager()
                .iter_subscription_cursors(|cursor_name, sub_cursor| {
                    cursor_names.push(cursor_name.clone());
                    sub_cursor_names.push(sub_cursor.subscription_name().to_owned());
                    states.push(sub_cursor.state_info_string());
                    idle_durations.push(sub_cursor.idle_duration().as_secs());
                })
                .await;
            subscription_cursors.push(SubscriptionCursor {
                session_id: s.id().0,
                user_name: s.user_name(),
                peer_addr: format!("{}", s.peer_addr()),
                database: s.database(),
                states,
                idle_durations,
                cursor_names,
                subscription_names: sub_cursor_names,
            });
        }
        Ok(RpcResponse::new(GetAllSubCursorsResponse {
            subscription_cursors,
        }))
    }

    async fn get_running_sqls(
        &self,
        _request: RpcRequest<GetRunningSqlsRequest>,
    ) -> Result<RpcResponse<GetRunningSqlsResponse>, Status> {
        let running_sqls = self
            .session_map
            .read()
            .values()
            .map(|s| RunningSql {
                process_id: s.id().0,
                user_name: s.user_name(),
                peer_addr: format!("{}", s.peer_addr()),
                database: s.database(),
                elapsed_millis: s.elapse_since_running_sql().and_then(|e| e.try_into().ok()),
                sql: s.running_sql().map(|sql| format!("{}", sql)),
            })
            .collect();
        Ok(RpcResponse::new(GetRunningSqlsResponse { running_sqls }))
    }

    async fn cancel_running_sql(
        &self,
        request: RpcRequest<CancelRunningSqlRequest>,
    ) -> Result<RpcResponse<CancelRunningSqlResponse>, Status> {
        let process_id = request.into_inner().process_id;
        handle_kill_local(self.session_map.clone(), process_id).await?;
        Ok(RpcResponse::new(CancelRunningSqlResponse {}))
    }
}

/// Rebuild the table's streaming plan, possibly with cdc column changes.
async fn get_new_table_plan(
    table_id: TableId,
    database_id: DatabaseId,
    cdc_table_change: Option<TableSchemaChange>,
) -> Result<ReplaceJobPlan, RwError> {
    tracing::info!("get_new_table_plan for table {}", table_id);

    let session_mgr = SESSION_MANAGER
        .get()
        .expect("session manager has been initialized");

    // get a session object for the corresponding user and database
    let session = session_mgr.create_dummy_session(database_id)?;

    let _guard = scopeguard::guard((), |_| {
        session_mgr.end_session(&session);
    });

    let table_catalog = {
        let reader = session.env().catalog_reader().read_guard();
        reader.get_any_table_by_id(table_id)?.clone()
    };
    let original_owner = table_catalog.owner;

    let schema_name = {
        let reader = session.env().catalog_reader().read_guard();
        let schema = reader.get_schema_by_id(table_catalog.database_id, table_catalog.schema_id)?;
        schema.name.clone()
    };
    let table_name = ObjectName::from(vec![
        schema_name.as_str().into(),
        table_catalog.name.as_str().into(),
    ]);

    let definition = if let Some(cdc_table_change) = cdc_table_change {
        let new_version_columns = cdc_table_change
            .columns
            .into_iter()
            .map(|c| c.into())
            .collect_vec();
        get_new_table_definition_for_cdc_table(table_catalog.clone(), &new_version_columns).await?
    } else {
        table_catalog.create_sql_ast_purified()?
    };

    let (mut source, mut table, graph, job_type) = get_replace_table_plan(
        &session,
        table_name,
        definition,
        &table_catalog,
        SqlColumnStrategy::FollowUnchecked,
    )
    .await?;

    // The dummy session may be created with a fixed super user, which can cause the generated
    // plan to carry an incorrect table owner. Restore it to the original owner.
    table.owner = original_owner;
    if let Some(source) = &mut source {
        source.owner = original_owner;
    }

    Ok(ReplaceJobPlan {
        replace_job: Some(replace_job_plan::ReplaceJob::ReplaceTable(
            replace_job_plan::ReplaceTable {
                table: Some(table.to_prost()),
                source: source.map(|s| s.to_prost()),
                job_type: job_type as _,
            },
        )),
        fragment_graph: Some(graph),
    })
}
