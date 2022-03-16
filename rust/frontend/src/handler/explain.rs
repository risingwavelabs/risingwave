use std::cell::RefCell;
use std::rc::Rc;

use pgwire::pg_field_descriptor::{PgFieldDescriptor, TypeOid};
use pgwire::pg_response::{PgResponse, StatementType};
use pgwire::types::Row;
use risingwave_common::error::{ErrorCode, Result};
use risingwave_sqlparser::ast::Statement;

use crate::binder::{Binder, BoundStatement};
use crate::planner::Planner;
use crate::session::QueryContext;

pub(super) fn handle_explain(
    context: QueryContext,
    stmt: Statement,
    _verbose: bool,
) -> Result<PgResponse> {
    let context = Rc::new(RefCell::new(context));
    let session = context.borrow().session_ctx.clone();
    let catalog_mgr = session.env().catalog_mgr();
    let catalog = catalog_mgr
        .get_database_snapshot(session.database())
        .unwrap();
    // bind, plan, optimize, and serialize here
    let mut binder = Binder::new(catalog);
    let bound = binder.bind(stmt)?;

    let res = match &bound {
        BoundStatement::CreateView(_) => {
            let mut planner = Planner::new(context);
            let logical = planner.plan(bound)?;
            let stream = logical.gen_create_mv_plan();
            let mut output = String::new();
            stream
                .explain(0, &mut output)
                .map_err(|e| ErrorCode::InternalError(e.to_string()))?;

            let rows = output
                .lines()
                .map(|s| Row::new(vec![Some(s.into())]))
                .collect::<Vec<_>>();

            PgResponse::new(
                StatementType::EXPLAIN,
                rows.len() as i32,
                rows,
                vec![PgFieldDescriptor::new(
                    "QUERY PLAN".to_owned(),
                    TypeOid::Varchar,
                )],
            )
        }
        BoundStatement::Query(_) => {
            let mut planner = Planner::new(context);
            let logical = planner.plan(bound)?;
            let batch = logical.gen_batch_query_plan();
            let mut output = String::new();
            batch
                .explain(0, &mut output)
                .map_err(|e| ErrorCode::InternalError(e.to_string()))?;

            let rows = output
                .lines()
                .map(|s| Row::new(vec![Some(s.into())]))
                .collect::<Vec<_>>();

            PgResponse::new(
                StatementType::EXPLAIN,
                rows.len() as i32,
                rows,
                vec![PgFieldDescriptor::new(
                    "QUERY PLAN".to_owned(),
                    TypeOid::Varchar,
                )],
            )
        }
        _ => return Err(ErrorCode::NotImplementedError("cannot explain".into()).into()),
    };

    Ok(res)
}
