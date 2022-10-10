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

use std::sync::Arc;

use futures::StreamExt;
use pgwire::pg_field_descriptor::PgFieldDescriptor;
use pgwire::pg_response::{PgResponse, StatementType};
use risingwave_common::error::{ErrorCode, Result, RwError};
use risingwave_common::session_config::QueryMode;
use risingwave_sqlparser::ast::Statement;

use super::{PgResponseStream, RwPgResponse};
use crate::binder::Binder;
use crate::handler::privilege::{check_privileges, resolve_privileges};
use crate::handler::util::{force_local_mode, to_pg_field};
use crate::planner::Planner;
use crate::scheduler::plan_fragmenter::Query;
use crate::scheduler::{
    BatchPlanFragmenter, ExecutionContext, ExecutionContextRef, LocalQueryExecution,
};
use crate::session::{OptimizerContext, OptimizerContextRef, SessionImpl};
use crate::PlanRef;

pub type QueryResultSet = PgResponseStream;

pub fn gen_batch_query_plan(
    session: &SessionImpl,
    context: OptimizerContextRef,
    stmt: Statement,
) -> Result<(PlanRef, QueryMode)> {
    let stmt_type = to_statement_type(&stmt);

    let bound = {
        let mut binder = Binder::new(session);
        binder.bind(stmt)?
    };

    let check_items = resolve_privileges(&bound);
    check_privileges(session, &check_items)?;

    let mut planner = Planner::new(context);

    let must_local = force_local_mode(&bound);
    let must_dist = stmt_type.is_dml();
    let query_mode = match (must_dist, must_local) {
        (true, true) => {
            return Err(ErrorCode::InternalError(
                "the query is forced to both local and distributed mode by optimizer".to_owned(),
            )
            .into())
        }
        (true, false) => QueryMode::Distributed,
        (false, true) => QueryMode::Local,
        (false, false) => session.config().get_query_mode(),
    };

    let mut logical = planner.plan(bound)?;
    match query_mode {
        QueryMode::Local => Ok((logical.gen_batch_local_plan()?, query_mode)),
        QueryMode::Distributed => Ok((logical.gen_batch_distributed_plan()?, query_mode)),
    }
}

pub async fn handle_query(
    context: OptimizerContext,
    stmt: Statement,
    format: bool,
) -> Result<RwPgResponse> {
    let stmt_type = to_statement_type(&stmt);
    let session = context.session_ctx.clone();

    // Subblock to make sure PlanRef (an Rc) is dropped before `await` below.
    let (query, query_mode, pg_descs) = {
        let (plan, query_mode) = gen_batch_query_plan(&session, context.into(), stmt)?;
        let pg_descs = plan
            .schema()
            .fields()
            .iter()
            .map(to_pg_field)
            .collect::<Vec<PgFieldDescriptor>>();

        tracing::trace!(
            "Generated distributed plan: {:?}, query_mode:{:?}",
            plan.explain_to_string()?,
            query_mode
        );
        let plan_fragmenter = BatchPlanFragmenter::new(
            session.env().worker_node_manager_ref(),
            session.env().catalog_reader().clone(),
        );
        (plan_fragmenter.split(plan)?, query_mode, pg_descs)
    };
    tracing::trace!("Generated query after plan fragmenter: {:?}", &query);

    let mut row_stream = match query_mode {
        QueryMode::Local => local_execute(session.clone(), query, format).await?,
        // Local mode do not support cancel tasks.
        QueryMode::Distributed => distribute_execute(session.clone(), query, format).await?,
    };

    let rows_count = match stmt_type {
        StatementType::SELECT => None,
        StatementType::INSERT | StatementType::DELETE | StatementType::UPDATE => {
            // Get the row from the row_stream.
            let first_row_set = row_stream
                .next()
                .await
                .expect("compute node should return affected rows in output")
                .map_err(|err| RwError::from(ErrorCode::InternalError(format!("{}", err))))?;
            let affected_rows_str = first_row_set[0].values()[0]
                .as_ref()
                .expect("compute node should return affected rows in output");
            Some(
                String::from_utf8(affected_rows_str.to_vec())
                    .unwrap()
                    .parse()
                    .unwrap_or_default(),
            )
        }
        _ => unreachable!(),
    };

    // Implicitly flush the writes.
    if session.config().get_implicit_flush() {
        flush_for_write(&session, stmt_type).await?;
    }

    Ok(PgResponse::new_for_stream(
        stmt_type, rows_count, row_stream, pg_descs,
    ))
}

fn to_statement_type(stmt: &Statement) -> StatementType {
    use StatementType::*;

    match stmt {
        Statement::Query(_) => SELECT,
        Statement::Insert { .. } => INSERT,
        Statement::Delete { .. } => DELETE,
        Statement::Update { .. } => UPDATE,
        _ => unreachable!(),
    }
}

pub async fn distribute_execute(
    session: Arc<SessionImpl>,
    query: Query,
    format: bool,
) -> Result<QueryResultSet> {
    let execution_context: ExecutionContextRef = ExecutionContext::new(session.clone()).into();
    let query_manager = execution_context.session().env().query_manager().clone();
    query_manager
        .schedule(execution_context, query, format)
        .await
        .map_err(|err| err.into())
}

async fn local_execute(
    session: Arc<SessionImpl>,
    query: Query,
    format: bool,
) -> Result<QueryResultSet> {
    let timer = session
        .env()
        .frontend_metrics
        .latency_local_execution
        .start_timer();

    // Subblock to make sure PlanRef (an Rc) is dropped before `await` below.
    let front_env = session.env();

    let rsp = {
        // Acquire hummock snapshot for local execution.
        let hummock_snapshot_manager = front_env.hummock_snapshot_manager();
        let query_id = query.query_id().clone();
        let epoch = hummock_snapshot_manager
            .acquire(&query_id)
            .await?
            .committed_epoch;

        // TODO: Passing sql here
        let execution =
            LocalQueryExecution::new(query, front_env.clone(), "", epoch, session.auth_context());
        let rsp = Ok(execution.stream_rows(format));

        // Release hummock snapshot for local execution.
        hummock_snapshot_manager.release(epoch, &query_id).await;

        rsp
    };

    // Collect metrics
    timer.observe_duration();
    session
        .env()
        .frontend_metrics
        .query_counter_local_execution
        .inc();

    rsp
}

async fn flush_for_write(session: &SessionImpl, stmt_type: StatementType) -> Result<()> {
    match stmt_type {
        StatementType::INSERT | StatementType::DELETE | StatementType::UPDATE => {
            let client = session.env().meta_client();
            let snapshot = client.flush(true).await?;
            session
                .env()
                .hummock_snapshot_manager()
                .update_epoch(snapshot);
        }
        _ => {}
    }
    Ok(())
}
