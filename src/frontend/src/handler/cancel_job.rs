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

use pgwire::pg_response::StatementType;
use risingwave_common::types::Fields;
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
    let job_ids = jobs.0;
    let mut filtered_job_ids = vec![];
    let mut notices = vec![];
    {
        let catalog_reader = session.env().catalog_reader().read_guard();
        for job_id in job_ids {
            let database_catalog = catalog_reader.get_database_by_name(&session.database())?;
            let sink_catalog = database_catalog
                .iter_schemas()
                .find_map(|schema| schema.get_sink_by_id(&job_id));
            if let Some(sink_catalog) = sink_catalog {
                if sink_catalog.is_created() {
                    continue; // Skip already created sinks
                } else if sink_catalog.target_table.is_some() {
                    notices.push(format!(
                        "Please use `DROP SINK {}` to cancel sink into table job.",
                        sink_catalog.name
                    ));
                    continue;
                }
            }
            filtered_job_ids.push(job_id);
        }
    }

    let mut response_builder = RwPgResponse::builder(StatementType::CANCEL_COMMAND);
    for notice in notices {
        response_builder = response_builder.notice(notice);
    }

    let canceled_jobs = if !filtered_job_ids.is_empty() {
        let cancel_fut = async {
            session
                .env()
                .meta_client()
                .cancel_creating_jobs(PbJobs::Ids(CreatingJobIds {
                    job_ids: filtered_job_ids,
                }))
                .await
                .map_err(Into::into)
        };

        execute_with_long_running_notification(cancel_fut, &session, "CANCEL JOBS", 30).await?
    } else {
        vec![]
    };
    let rows = canceled_jobs
        .into_iter()
        .map(|id| CancelRow { id: id.to_string() });
    Ok(response_builder.rows(rows).into())
}

#[derive(Fields)]
#[fields(style = "Title Case")]
struct CancelRow {
    id: String,
}
