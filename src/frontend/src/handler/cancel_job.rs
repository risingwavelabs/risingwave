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

use itertools::Itertools;
use pgwire::pg_response::{PgResponse, StatementType};
use risingwave_common::types::Fields;
use risingwave_pb::id::JobId;
use risingwave_pb::meta::cancel_creating_jobs_request::{CreatingJobIds, PbJobs};
use risingwave_sqlparser::ast::JobIdents;

use super::RwPgResponseBuilderExt;
use super::util::execute_with_long_running_notification;
use crate::error::Result;
use crate::handler::{HandlerArgs, RwPgResponse};

pub(super) async fn handle_cancel(
    handler_args: HandlerArgs,
    jobs: JobIdents,
) -> Result<RwPgResponse> {
    let session = handler_args.session;
    let job_ids = jobs.0.into_iter().map(JobId::from).collect_vec();

    let canceled_jobs = if !job_ids.is_empty() {
        // Wrap in async block to convert RpcError to RwError
        execute_with_long_running_notification(
            async {
                session
                    .env()
                    .meta_client()
                    .cancel_creating_jobs(PbJobs::Ids(CreatingJobIds { job_ids }))
                    .await
                    .map_err(Into::into)
            },
            &session,
            "CANCEL JOBS",
        )
        .await?
    } else {
        vec![]
    };
    let rows = canceled_jobs
        .into_iter()
        .map(|id| CancelRow { id: id.to_string() });
    Ok(PgResponse::builder(StatementType::CANCEL_COMMAND)
        .rows(rows)
        .into())
}

#[derive(Fields)]
#[fields(style = "Title Case")]
struct CancelRow {
    id: String,
}
