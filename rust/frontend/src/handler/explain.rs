use pgwire::pg_field_descriptor::{PgFieldDescriptor, TypeOid};
use pgwire::pg_response::{PgResponse, StatementType};
use pgwire::types::Row;
use risingwave_common::error::{ErrorCode, Result};
use risingwave_sqlparser::ast::Statement;

use crate::binder::Binder;
use crate::planner::Planner;
use crate::session::QueryContext;

pub(super) fn handle_explain(
    context: QueryContext<'_>,
    stmt: Statement,
    _verbose: bool,
) -> Result<PgResponse> {
    let session = context.session;
    let catalog_mgr = session.env().catalog_mgr();
    let catalog = catalog_mgr
        .get_database_snapshot(session.database())
        .unwrap();
    // bind, plan, optimize, and serialize here
    let mut binder = Binder::new(catalog);
    let bound = binder.bind(stmt)?;
    let mut planner = Planner::new();
    let plan = planner.plan(bound)?;
    let mut output = String::new();
    plan.explain(0, &mut output)
        .map_err(|e| ErrorCode::InternalError(e.to_string()))?;

    let rows = output
        .lines()
        .map(|s| Row::new(vec![Some(s.into())]))
        .collect::<Vec<_>>();
    let res = PgResponse::new(
        StatementType::EXPLAIN,
        rows.len() as i32,
        rows,
        vec![PgFieldDescriptor::new(
            "QUERY PLAN".to_owned(),
            TypeOid::Varchar,
        )],
    );
    Ok(res)
}
