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

use pgwire::pg_response::{PgResponse, StatementType};
use risingwave_common::error::tonic::TonicStatusWrapper;
use risingwave_pb::frontend_service::CancelRunningSqlRequest;

use crate::error::{ErrorCode, Result};
use crate::handler::{HandlerArgs, RwPgResponse};
use crate::session::{
    SessionMapRef, WorkerProcessId, cancel_creating_jobs_in_session, cancel_queries_in_session,
};

pub(super) async fn handle_kill(handler_args: HandlerArgs, s: String) -> Result<RwPgResponse> {
    let worker_process_id =
        WorkerProcessId::try_from(s).map_err(ErrorCode::InvalidParameterValue)?;
    let env = handler_args.session.env();
    let this_worker_id = env.meta_client_ref().worker_id();
    if this_worker_id == worker_process_id.worker_id {
        return handle_kill_local(
            handler_args.session.env().sessions_map().clone(),
            worker_process_id.process_id,
        )
        .await;
    }
    let Some(worker) = handler_args
        .session
        .env()
        .worker_node_manager_ref()
        .worker_node(worker_process_id.worker_id)
    else {
        return Err(ErrorCode::InvalidParameterValue(format!(
            "worker {} not found",
            worker_process_id.worker_id
        ))
        .into());
    };
    let frontend_client = env.frontend_client_pool().get(&worker).await?;
    frontend_client
        .cancel_running_sql(CancelRunningSqlRequest {
            process_id: worker_process_id.process_id,
        })
        .await
        .map_err(TonicStatusWrapper::from)?;
    Ok(PgResponse::empty_result(StatementType::KILL))
}

pub async fn handle_kill_local(
    sessions_map: SessionMapRef,
    process_id: i32,
) -> Result<RwPgResponse> {
    // Process id and secret key in session id are the same in RisingWave.
    let session_id = (process_id, process_id);
    tracing::trace!("kill query in session: {:?}", session_id);
    // TODO: cancel queries with await.
    let mut session_exists = cancel_queries_in_session(session_id, sessions_map.clone());
    session_exists |= cancel_creating_jobs_in_session(session_id, sessions_map);

    if session_exists {
        Ok(PgResponse::empty_result(StatementType::KILL))
    } else {
        Err(ErrorCode::SessionNotFound.into())
    }
}
