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

use futures::StreamExt;
use futures_async_stream::for_await;
use pgwire::pg_field_descriptor::PgFieldDescriptor;
use pgwire::pg_response::{PgResponse, StatementType};
use risingwave_batch::executor::BoxedDataChunkStream;
use risingwave_common::error::Result;
use risingwave_common::session_config::QueryMode;
use risingwave_sqlparser::ast::Statement;
use tokio::sync::oneshot;
use tracing::debug;

use crate::binder::{Binder, BoundStatement};
use crate::handler::privilege::{check_privileges, resolve_privileges};
use crate::handler::util::{force_local_mode, to_pg_field, to_pg_rows};
use crate::planner::Planner;
use crate::scheduler::{
    BatchPlanFragmenter, ExecutionContext, ExecutionContextRef, LocalQueryExecution, SchedulerError,
};
use crate::session::{OptimizerContext, SessionImpl};

pub async fn handle_query(
    context: OptimizerContext,
    stmt: Statement,
    format: bool,
) -> Result<PgResponse> {
    let stmt_type = to_statement_type(&stmt);
    let session = context.session_ctx.clone();

    let bound = {
        let mut binder = Binder::new(&session);
        binder.bind(stmt)?
    };

    let check_items = resolve_privileges(&bound);
    check_privileges(&session, &check_items)?;

    let query_mode = if force_local_mode(&bound) {
        debug!("force query mode to local");
        QueryMode::Local
    } else {
        session.config().get_query_mode()
    };
    debug!("query_mode:{:?}", query_mode);

    let (shutdown_tx, shutdown_rx) = oneshot::channel();
    let (data_stream, pg_descs) = match query_mode {
        QueryMode::Local => {
            if stmt_type.is_dml() {
                // DML do not support local mode yet.
                distribute_execute(context, bound, shutdown_tx).await?
            } else {
                local_execute(context, bound)?
            }
        }
        // Local mode do not support cancel tasks.
        QueryMode::Distributed => distribute_execute(context, bound, shutdown_tx).await?,
    };

    let mut data_stream = data_stream.take_until(shutdown_rx);
    let mut rows = vec![];
    #[for_await]
    for chunk in &mut data_stream {
        rows.extend(to_pg_rows(chunk?, format));
    }

    // Check whether error happen, if yes, returned.
    let execution_ret = data_stream.take_result();
    if let Some(ret) = execution_ret {
        return Err(ret
            .expect("The shutdown message receiver should not fail")
            .into());
    }

    let rows_count = match stmt_type {
        StatementType::SELECT => rows.len() as i32,
        StatementType::INSERT | StatementType::DELETE | StatementType::UPDATE => {
            let first_row = rows[0].values();
            let affected_rows_str = first_row[0]
                .as_ref()
                .expect("compute node should return affected rows in output");
            String::from_utf8(affected_rows_str.to_vec())
                .unwrap()
                .parse()
                .unwrap_or_default()
        }
        _ => unreachable!(),
    };

    // Implicitly flush the writes.
    if session.config().get_implicit_flush() {
        flush_for_write(&session, stmt_type).await?;
    }
    Ok(PgResponse::new(stmt_type, rows_count, rows, pg_descs, true))
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
    context: OptimizerContext,
    stmt: BoundStatement,
    shutdown_tx: oneshot::Sender<SchedulerError>,
) -> Result<(BoxedDataChunkStream, Vec<PgFieldDescriptor>)> {
    let session = context.session_ctx.clone();
    // Subblock to make sure PlanRef (an Rc) is dropped before `await` below.
    let (query, pg_descs) = {
        let root = Planner::new(context.into()).plan(stmt)?;

        let pg_descs = root
            .schema()
            .fields()
            .iter()
            .map(to_pg_field)
            .collect::<Vec<PgFieldDescriptor>>();

        let plan = root.gen_batch_distributed_plan()?;

        tracing::trace!(
            "Generated distributed plan: {:?}",
            plan.explain_to_string()?
        );

        let plan_fragmenter = BatchPlanFragmenter::new(
            session.env().worker_node_manager_ref(),
            session.env().catalog_reader().clone(),
        );
        let query = plan_fragmenter.split(plan)?;
        tracing::trace!("Generated query after plan fragmenter: {:?}", &query);
        (query, pg_descs)
    };

    let execution_context: ExecutionContextRef = ExecutionContext::new(session.clone()).into();
    let query_manager = execution_context.session().env().query_manager().clone();
    Ok((
        Box::pin(
            query_manager
                .schedule(execution_context, query, shutdown_tx)
                .await?,
        ),
        pg_descs,
    ))
}

fn local_execute(
    context: OptimizerContext,
    stmt: BoundStatement,
) -> Result<(BoxedDataChunkStream, Vec<PgFieldDescriptor>)> {
    let session = context.session_ctx.clone();

    // Subblock to make sure PlanRef (an Rc) is dropped before `await` below.
    let (query, pg_descs) = {
        let root = Planner::new(context.into()).plan(stmt)?;

        let pg_descs = root
            .schema()
            .fields()
            .iter()
            .map(to_pg_field)
            .collect::<Vec<PgFieldDescriptor>>();

        let plan = root.gen_batch_local_plan()?;

        let plan_fragmenter = BatchPlanFragmenter::new(
            session.env().worker_node_manager_ref(),
            session.env().catalog_reader().clone(),
        );
        let query = plan_fragmenter.split(plan)?;
        tracing::trace!("Generated query after plan fragmenter: {:?}", &query);
        (query, pg_descs)
    };

    let front_env = session.env();

    // TODO: Passing sql here
    let execution = LocalQueryExecution::new(query, front_env.clone(), "", session.auth_context());
    Ok((Box::pin(execution.run()), pg_descs))
}

async fn flush_for_write(session: &SessionImpl, stmt_type: StatementType) -> Result<()> {
    match stmt_type {
        StatementType::INSERT | StatementType::DELETE | StatementType::UPDATE => {
            let client = session.env().meta_client();
            let snapshot = client.flush().await?;
            session
                .env()
                .hummock_snapshot_manager()
                .update_epoch(snapshot);
        }
        _ => {}
    }
    Ok(())
}
