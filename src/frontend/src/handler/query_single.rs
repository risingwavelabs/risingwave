// Copyright 2022 Singularity Data
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use futures_async_stream::for_await;
use pgwire::pg_response::{PgResponse, StatementType};
use risingwave_common::error::Result;
use risingwave_sqlparser::ast::Statement;

use super::query::IMPLICIT_FLUSH;
use crate::binder::Binder;
use crate::handler::util::{to_pg_field, to_pg_rows};
use crate::planner::Planner;
use crate::scheduler::{ExecutionContext, ExecutionContextRef};
use crate::session::{OptimizerContext, SessionImpl};

pub async fn handle_query_single(context: OptimizerContext, stmt: Statement) -> Result<PgResponse> {
    let stmt_type = to_statement_type(&stmt);
    let session = context.session_ctx.clone();

    let bound = {
        let mut binder = Binder::new(
            session.env().catalog_reader().read_guard(),
            session.database().to_string(),
        );
        binder.bind(stmt)?
    };

    let (plan, pg_descs) = {
        // Subblock to make sure PlanRef (an Rc) is dropped before `await` below.
        let plan = Planner::new(context.into())
            .plan(bound)?
            .gen_batch_query_plan();

        let pg_descs = plan.schema().fields().iter().map(to_pg_field).collect();

        (plan.to_batch_prost(), pg_descs)
    };

    let execution_context: ExecutionContextRef = ExecutionContext::new(session.clone()).into();
    let query_manager = execution_context.session().env().query_manager().clone();

    let mut rows = vec![];
    #[for_await]
    for chunk in query_manager
        .schedule_single(execution_context, plan)
        .await?
    {
        rows.extend(to_pg_rows(chunk?));
    }

    let rows_count = match stmt_type {
        StatementType::SELECT => rows.len() as i32,

        // TODO(renjie): We need a better solution for this.
        StatementType::INSERT | StatementType::DELETE | StatementType::UPDATE => {
            let first_row = rows[0].values();
            let affected_rows_str = first_row[0]
                .as_ref()
                .expect("compute node should return affected rows in output");
            affected_rows_str.parse().unwrap_or_default()
        }

        _ => unreachable!(),
    };

    // Implicitly flush the writes.
    if let Some(flag) = session.get(&IMPLICIT_FLUSH) {
        if flag.is_true() {
            flush_for_write(&session, stmt_type).await?;
        }
    }

    Ok(PgResponse::new(stmt_type, rows_count, rows, pg_descs))
}

async fn flush_for_write(session: &SessionImpl, stmt_type: StatementType) -> Result<()> {
    match stmt_type {
        StatementType::INSERT | StatementType::DELETE | StatementType::UPDATE => {
            let client = session.env().meta_client();
            client.flush().await
        }
        _ => Ok(()),
    }
}

fn to_statement_type(stmt: &Statement) -> StatementType {
    use StatementType::*;

    match stmt {
        Statement::Insert { .. } => INSERT,
        Statement::Delete { .. } => DELETE,
        Statement::Update { .. } => UPDATE,
        Statement::Query(_) => SELECT,
        _ => unreachable!(),
    }
}
