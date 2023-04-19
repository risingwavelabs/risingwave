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

use std::collections::HashSet;
use std::sync::Arc;
use std::time::Instant;

use futures::StreamExt;
use itertools::Itertools;
use pgwire::pg_field_descriptor::PgFieldDescriptor;
use pgwire::pg_response::{PgResponse, StatementType};
use pgwire::types::Format;
use postgres_types::FromSql;
use risingwave_common::catalog::Schema;
use risingwave_common::error::{ErrorCode, Result, RwError};
use risingwave_common::session_config::QueryMode;
use risingwave_common::types::DataType;
use risingwave_sqlparser::ast::{SetExpr, Statement};

use super::extended_handle::{PortalResult, PrepareStatement, PreparedResult};
use super::{PgResponseStream, RwPgResponse};
use crate::binder::{Binder, BoundStatement};
use crate::catalog::TableId;
use crate::handler::flush::do_flush;
use crate::handler::privilege::resolve_privileges;
use crate::handler::util::{to_pg_field, DataChunkToRowSetAdapter};
use crate::handler::HandlerArgs;
use crate::optimizer::plan_node::Explain;
use crate::optimizer::{
    ExecutionModeDecider, OptimizerContext, OptimizerContextRef, RelationCollectorVisitor,
    SysTableVisitor,
};
use crate::planner::Planner;
use crate::scheduler::plan_fragmenter::Query;
use crate::scheduler::worker_node_manager::WorkerNodeSelector;
use crate::scheduler::{
    BatchPlanFragmenter, DistributedQueryStream, ExecutionContext, ExecutionContextRef,
    LocalQueryExecution, LocalQueryStream, PinnedHummockSnapshot,
};
use crate::session::SessionImpl;
use crate::PlanRef;

pub async fn handle_query(
    handler_args: HandlerArgs,
    stmt: Statement,
    formats: Vec<Format>,
) -> Result<RwPgResponse> {
    let session = handler_args.session.clone();

    let plan_fragmenter_result = {
        let context = OptimizerContext::from_handler_args(handler_args);
        let plan_result = gen_batch_plan_by_statement(&session, context.into(), stmt)?;
        gen_batch_plan_fragmenter(&session, plan_result)?
    };
    execute(session, plan_fragmenter_result, formats).await
}

pub fn handle_parse(
    handler_args: HandlerArgs,
    statement: Statement,
    specific_param_types: Vec<DataType>,
) -> Result<PrepareStatement> {
    let session = handler_args.session;
    let bound_result = gen_bound(&session, statement.clone(), specific_param_types)?;

    Ok(PrepareStatement::Prepared(PreparedResult {
        statement,
        bound_result,
    }))
}

pub async fn handle_execute(
    handler_args: HandlerArgs,
    portal: PortalResult,
) -> Result<RwPgResponse> {
    let PortalResult {
        bound_result,
        result_formats,
        ..
    } = portal;

    let session = handler_args.session.clone();
    let plan_fragmenter_result = {
        let context = OptimizerContext::from_handler_args(handler_args);
        let plan_result = gen_batch_query_plan(&session, context.into(), bound_result)?;

        gen_batch_plan_fragmenter(&session, plan_result)?
    };
    execute(session, plan_fragmenter_result, result_formats).await
}

pub fn gen_batch_plan_by_statement(
    session: &SessionImpl,
    context: OptimizerContextRef,
    stmt: Statement,
) -> Result<BatchQueryPlanResult> {
    let bound_result = gen_bound(session, stmt, vec![])?;
    gen_batch_query_plan(session, context, bound_result)
}

#[derive(Clone)]
pub struct BoundResult {
    pub(crate) stmt_type: StatementType,
    pub(crate) must_dist: bool,
    pub(crate) bound: BoundStatement,
    pub(crate) param_types: Vec<DataType>,
    pub(crate) dependent_relations: HashSet<TableId>,
}

fn gen_bound(
    session: &SessionImpl,
    stmt: Statement,
    specific_param_types: Vec<DataType>,
) -> Result<BoundResult> {
    let stmt_type = StatementType::infer_from_statement(&stmt)
        .map_err(|err| RwError::from(ErrorCode::InvalidInputSyntax(err)))?;
    let must_dist = must_run_in_distributed_mode(&stmt)?;

    let mut binder = Binder::new_with_param_types(session, specific_param_types);
    let bound = binder.bind(stmt)?;

    let check_items = resolve_privileges(&bound);
    session.check_privileges(&check_items)?;

    Ok(BoundResult {
        stmt_type,
        must_dist,
        bound,
        param_types: binder.export_param_types()?,
        dependent_relations: binder.included_relations(),
    })
}

pub struct BatchQueryPlanResult {
    pub(crate) plan: PlanRef,
    pub(crate) query_mode: QueryMode,
    pub(crate) schema: Schema,
    pub(crate) stmt_type: StatementType,
    // Note that these relations are only resolved in the binding phase, and it may only be a
    // subset of the final one. i.e. the final one may contain more implicit dependencies on
    // indices.
    pub(crate) dependent_relations: Vec<TableId>,
}

fn gen_batch_query_plan(
    session: &SessionImpl,
    context: OptimizerContextRef,
    bind_result: BoundResult,
) -> Result<BatchQueryPlanResult> {
    let BoundResult {
        stmt_type,
        must_dist,
        bound,
        dependent_relations,
        ..
    } = bind_result;

    let mut planner = Planner::new(context);

    let mut logical = planner.plan(bound)?;
    let schema = logical.schema();
    let batch_plan = logical.gen_batch_plan()?;

    let dependent_relations =
        RelationCollectorVisitor::collect_with(dependent_relations, batch_plan.clone());

    let must_local = must_run_in_local_mode(batch_plan.clone());

    let query_mode = match (must_dist, must_local) {
        (true, true) => {
            return Err(ErrorCode::InternalError(
                "the query is forced to both local and distributed mode by optimizer".to_owned(),
            )
            .into())
        }
        (true, false) => QueryMode::Distributed,
        (false, true) => QueryMode::Local,
        (false, false) => match session.config().get_query_mode() {
            QueryMode::Auto => determine_query_mode(batch_plan.clone()),
            QueryMode::Local => QueryMode::Local,
            QueryMode::Distributed => QueryMode::Distributed,
        },
    };

    let physical = match query_mode {
        QueryMode::Auto => unreachable!(),
        QueryMode::Local => logical.gen_batch_local_plan(batch_plan)?,
        QueryMode::Distributed => logical.gen_batch_distributed_plan(batch_plan)?,
    };

    Ok(BatchQueryPlanResult {
        plan: physical,
        query_mode,
        schema,
        stmt_type,
        dependent_relations: dependent_relations.into_iter().collect_vec(),
    })
}

fn must_run_in_distributed_mode(stmt: &Statement) -> Result<bool> {
    fn is_insert_using_select(stmt: &Statement) -> bool {
        fn has_select_query(set_expr: &SetExpr) -> bool {
            match set_expr {
                SetExpr::Select(_) => true,
                SetExpr::Query(query) => has_select_query(&query.body),
                SetExpr::SetOperation { left, right, .. } => {
                    has_select_query(left) || has_select_query(right)
                }
                SetExpr::Values(_) => false,
            }
        }

        matches!(
            stmt,
            Statement::Insert {source, ..} if has_select_query(&source.body)
        )
    }

    let stmt_type = StatementType::infer_from_statement(stmt)
        .map_err(|err| RwError::from(ErrorCode::InvalidInputSyntax(err)))?;

    Ok(matches!(
        stmt_type,
        StatementType::UPDATE
            | StatementType::DELETE
            | StatementType::UPDATE_RETURNING
            | StatementType::DELETE_RETURNING
    ) | is_insert_using_select(stmt))
}

fn must_run_in_local_mode(batch_plan: PlanRef) -> bool {
    SysTableVisitor::has_sys_table(batch_plan)
}

fn determine_query_mode(batch_plan: PlanRef) -> QueryMode {
    if ExecutionModeDecider::run_in_local_mode(batch_plan) {
        QueryMode::Local
    } else {
        QueryMode::Distributed
    }
}

struct BatchPlanFragmenterResult {
    pub(crate) plan_fragmenter: BatchPlanFragmenter,
    pub(crate) query_mode: QueryMode,
    pub(crate) schema: Schema,
    pub(crate) stmt_type: StatementType,
    pub(crate) _dependent_relations: Vec<TableId>,
    pub(crate) notice: String,
}

fn gen_batch_plan_fragmenter(
    session: &SessionImpl,
    plan_result: BatchQueryPlanResult,
) -> Result<BatchPlanFragmenterResult> {
    let BatchQueryPlanResult {
        plan,
        query_mode,
        schema,
        stmt_type,
        dependent_relations,
    } = plan_result;

    let context = plan.plan_base().ctx.clone();
    tracing::trace!(
        "Generated query plan: {:?}, query_mode:{:?}",
        plan.explain_to_string()?,
        query_mode
    );
    let worker_node_manager_reader = WorkerNodeSelector::new(
        session.env().worker_node_manager_ref(),
        !session.config().only_checkpoint_visible(),
    );
    let plan_fragmenter = BatchPlanFragmenter::new(
        worker_node_manager_reader,
        session.env().catalog_reader().clone(),
        session.config().get_batch_parallelism(),
        plan,
    )?;
    let mut notice = String::new();
    context.append_notice(&mut notice);

    Ok(BatchPlanFragmenterResult {
        plan_fragmenter,
        query_mode,
        schema,
        stmt_type,
        _dependent_relations: dependent_relations,
        notice,
    })
}

async fn execute(
    session: Arc<SessionImpl>,
    plan_fragmenter_result: BatchPlanFragmenterResult,
    formats: Vec<Format>,
) -> Result<RwPgResponse> {
    let BatchPlanFragmenterResult {
        plan_fragmenter,
        query_mode,
        schema,
        stmt_type,
        notice,
        ..
    } = plan_fragmenter_result;

    let only_checkpoint_visible = session.config().only_checkpoint_visible();
    let query_start_time = Instant::now();
    let query = plan_fragmenter.generate_complete_query().await?;
    tracing::trace!("Generated query after plan fragmenter: {:?}", &query);

    let pg_descs = schema
        .fields()
        .iter()
        .map(to_pg_field)
        .collect::<Vec<PgFieldDescriptor>>();
    let column_types = schema.fields().iter().map(|f| f.data_type()).collect_vec();

    // Used in counting row count.
    let first_field_format = formats.first().copied().unwrap_or(Format::Text);

    let mut row_stream = {
        let query_epoch = session.config().get_query_epoch();
        let query_snapshot = if let Some(query_epoch) = query_epoch {
            PinnedHummockSnapshot::Other(query_epoch)
        } else {
            // Acquire hummock snapshot for execution.
            // TODO: if there's no table scan, we don't need to acquire snapshot.
            let hummock_snapshot_manager = session.env().hummock_snapshot_manager();
            let query_id = query.query_id().clone();
            let pinned_snapshot = hummock_snapshot_manager.acquire(&query_id).await?;
            PinnedHummockSnapshot::FrontendPinned(pinned_snapshot, only_checkpoint_visible)
        };
        match query_mode {
            QueryMode::Auto => unreachable!(),
            QueryMode::Local => PgResponseStream::LocalQuery(DataChunkToRowSetAdapter::new(
                local_execute(session.clone(), query, query_snapshot).await?,
                column_types,
                formats,
                session.clone(),
            )),
            // Local mode do not support cancel tasks.
            QueryMode::Distributed => {
                PgResponseStream::DistributedQuery(DataChunkToRowSetAdapter::new(
                    distribute_execute(session.clone(), query, query_snapshot).await?,
                    column_types,
                    formats,
                    session.clone(),
                ))
            }
        }
    };

    let rows_count: Option<i32> = match stmt_type {
        StatementType::SELECT
        | StatementType::INSERT_RETURNING
        | StatementType::DELETE_RETURNING
        | StatementType::UPDATE_RETURNING => None,

        StatementType::INSERT | StatementType::DELETE | StatementType::UPDATE => {
            let first_row_set = row_stream.next().await;
            let first_row_set = match first_row_set {
                None => {
                    return Err(RwError::from(ErrorCode::InternalError(
                        "no affected rows in output".to_string(),
                    )))
                }
                Some(row) => {
                    row.map_err(|err| RwError::from(ErrorCode::InternalError(format!("{}", err))))?
                }
            };
            let affected_rows_str = first_row_set[0].values()[0]
                .as_ref()
                .expect("compute node should return affected rows in output");
            if let Format::Binary = first_field_format {
                Some(
                    i64::from_sql(&postgres_types::Type::INT8, affected_rows_str)
                        .unwrap()
                        .try_into()
                        .expect("affected rows count large than i32"),
                )
            } else {
                Some(
                    String::from_utf8(affected_rows_str.to_vec())
                        .unwrap()
                        .parse()
                        .unwrap_or_default(),
                )
            }
        }
        _ => unreachable!(),
    };

    // We need to do some post work after the query is finished and before the `Complete` response
    // it sent. This is achieved by the `callback` in `PgResponse`.
    let callback = async move {
        // Implicitly flush the writes.
        if session.config().get_implicit_flush() && stmt_type.is_dml() {
            do_flush(&session).await?;
        }

        // update some metrics
        match query_mode {
            QueryMode::Auto => unreachable!(),
            QueryMode::Local => {
                session
                    .env()
                    .frontend_metrics
                    .latency_local_execution
                    .observe(query_start_time.elapsed().as_secs_f64());

                session
                    .env()
                    .frontend_metrics
                    .query_counter_local_execution
                    .inc();
            }
            QueryMode::Distributed => {
                session
                    .env()
                    .query_manager()
                    .query_metrics
                    .query_latency
                    .observe(query_start_time.elapsed().as_secs_f64());

                session
                    .env()
                    .query_manager()
                    .query_metrics
                    .completed_query_counter
                    .inc();
            }
        }

        Ok(())
    };

    Ok(PgResponse::new_for_stream_extra(
        stmt_type, rows_count, row_stream, pg_descs, notice, callback,
    ))
}

async fn distribute_execute(
    session: Arc<SessionImpl>,
    query: Query,
    pinned_snapshot: PinnedHummockSnapshot,
) -> Result<DistributedQueryStream> {
    let execution_context: ExecutionContextRef = ExecutionContext::new(session.clone()).into();
    let query_manager = session.env().query_manager().clone();
    query_manager
        .schedule(execution_context, query, pinned_snapshot)
        .await
        .map_err(|err| err.into())
}

#[expect(clippy::unused_async)]
async fn local_execute(
    session: Arc<SessionImpl>,
    query: Query,
    pinned_snapshot: PinnedHummockSnapshot,
) -> Result<LocalQueryStream> {
    let front_env = session.env();

    // TODO: Passing sql here
    let execution = LocalQueryExecution::new(
        query,
        front_env.clone(),
        "",
        pinned_snapshot,
        session.auth_context(),
        session.reset_cancel_query_flag(),
    );

    Ok(execution.stream_rows())
}
