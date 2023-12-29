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

use pgwire::pg_response::{PgResponse, StatementType};
use risingwave_common::error::{ErrorCode, Result};

use crate::handler::{HandlerArgs, RwPgResponse};

pub(super) async fn handle_kill(
    handler_args: HandlerArgs,
    process_id: i32,
) -> Result<RwPgResponse> {
    // Process id and secret key in session id are the same in RisingWave.
    let session_id = (process_id, process_id);
    tracing::trace!("kill query in session: {:?}", session_id);
    let session = handler_args.session;
    // TODO: cancel queries with await.
    let mut session_exists = session.env().cancel_queries_in_session(session_id);
    session_exists |= session.env().cancel_creating_jobs_in_session(session_id);

    if session_exists {
        Ok(PgResponse::empty_result(StatementType::KILL))
    } else {
        Err(ErrorCode::SessionNotFound.into())
    }
}
