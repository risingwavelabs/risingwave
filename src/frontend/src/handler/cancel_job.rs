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
    return Ok(PgResponse::builder(StatementType::CANCEL_COMMAND)
        .values(
            rows.into(),
            vec![PgFieldDescriptor::new(
                "Id".to_string(),
                DataType::Varchar.to_oid(),
                DataType::Varchar.type_len(),
            )],
        )
        .into());
}
