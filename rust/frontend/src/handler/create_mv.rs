use std::cell::RefCell;
use std::rc::Rc;

use pgwire::pg_response::PgResponse;
use risingwave_common::error::{Result};
use risingwave_sqlparser::ast::{ObjectName, Query, Statement};

use crate::binder::{Binder};

use crate::planner::Planner;
use crate::session::QueryContext;

pub async fn handle_create_mv(
    context: QueryContext,
    name: ObjectName,
    query: Box<Query>,
) -> Result<PgResponse> {
    let (schema_name, table_name) = Binder::resolve_table_name(name.clone())?;
    let session = context.session_ctx.clone();
    let (_db_id, _schema_id) = session
        .env()
        .catalog_reader()
        .read_guard()
        .check_relation_name(session.database(), &schema_name, &table_name)?;
    let bound = {
        let mut binder = Binder::new(
            session.env().catalog_reader().read_guard(),
            session.database().to_string(),
        );
        binder.bind(Statement::Query(query))?
    };
    let _catalog_writer = session.env().catalog_writer();
    let plan = Planner::new(Rc::new(RefCell::new(context)))
        .plan(bound)?
        .gen_create_mv_plan()
        .to_stream_prost();
    // TODO catalog writer to create mv

    tracing::info!(name= ?name, plan = ?plan);

    Ok(PgResponse::new(
        pgwire::pg_response::StatementType::CREATE_MATERIALIZED_VIEW,
        0,
        vec![],
        vec![],
    ))
}
