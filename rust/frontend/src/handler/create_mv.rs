use std::cell::RefCell;
use std::rc::Rc;

use pgwire::pg_response::PgResponse;
use risingwave_common::error::{ErrorCode, Result};
use risingwave_sqlparser::ast::{ObjectName, Query, Statement};

use crate::binder::Binder;
use crate::planner::Planner;
use crate::session::QueryContext;

pub async fn handle_create_mv(
    context: QueryContext,
    name: ObjectName,
    query: Box<Query>,
) -> Result<PgResponse> {
    let session = context.session_ctx.clone();
    let catalog_mgr = session.env().catalog_mgr();
    let catalog = catalog_mgr
        .get_database_snapshot(session.database())
        .ok_or_else(|| ErrorCode::InternalError(String::from("catalog not found")))?;

    let mut binder = Binder::new(catalog);
    let bound = binder.bind(Statement::Query(query))?;
    let plan = Planner::new(Rc::new(RefCell::new(context)))
        .plan(bound)?
        .gen_create_mv_plan()
        .to_stream_prost();

    let json_plan = serde_json::to_string(&plan).unwrap();
    tracing::info!(name= ?name, plan = ?json_plan);

    Ok(PgResponse::new(
        pgwire::pg_response::StatementType::CREATE_MATERIALIZED_VIEW,
        0,
        vec![],
        vec![],
    ))
}
