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

use itertools::Itertools;
use pgwire::pg_field_descriptor::PgFieldDescriptor;
use pgwire::pg_response::{PgResponse, StatementType};
use pgwire::types::Row;
use risingwave_common::error::Result;
use risingwave_common::types::DataType;
use risingwave_pb::meta::cancel_creating_jobs_request::{CreatingJobIds, PbJobs};
use risingwave_sqlparser::ast::JobIdents;

use crate::handler::{HandlerArgs, RwPgResponse};

pub(super) async fn handle_cancel(
    handler_args: HandlerArgs,
    jobs: JobIdents,
) -> Result<RwPgResponse> {
    let session = handler_args.session;

    let canceled_jobs = session
        .env()
        .meta_client()
        .cancel_creating_jobs(PbJobs::Ids(CreatingJobIds { job_ids: jobs.0 }))
        .await?;
    let rows = canceled_jobs
        .into_iter()
        .map(|id| Row::new(vec![Some(id.to_string().into())]))
        .collect_vec();
    Ok(PgResponse::builder(StatementType::CANCEL_COMMAND)
        .values(
            rows.into(),
            vec![PgFieldDescriptor::new(
                "Id".to_string(),
                DataType::Varchar.to_oid(),
                DataType::Varchar.type_len(),
            )],
        )
        .into())
}
