use std::cell::RefCell;
use std::rc::Rc;

use pgwire::pg_field_descriptor::{PgFieldDescriptor, TypeOid};
use pgwire::pg_response::{PgResponse, StatementType};
use pgwire::types::Row;
use risingwave_common::error::Result;
use risingwave_sqlparser::ast::Statement;

use crate::binder::Binder;
use crate::planner::Planner;
use crate::session::QueryContext;

pub(super) fn handle_explain(
    context: QueryContext,
    stmt: Statement,
    _verbose: bool,
) -> Result<PgResponse> {
    let context = Rc::new(RefCell::new(context));
    let session = context.borrow().session_ctx.clone();
    // bind, plan, optimize, and serialize here
    let bound = {
        let mut binder = Binder::new(
            session.env().catalog_reader().read_guard(),
            session.database().to_string(),
        );
        binder.bind(stmt)?
    };
    let mut planner = Planner::new(context);

    let plan = match stmt {
        Statement::CreateView {
            or_replace: false,
            materialized: true,
            query,
            ..
        } => {
            let bound = binder.bind_query(query.as_ref().clone())?;
            let logical = planner.plan_query(bound)?;
            logical.gen_create_mv_plan()
        }
        stmt => {
            let bound = binder.bind(stmt)?;
            let logical = planner.plan(bound)?;
            logical.gen_batch_query_plan()
        }
    };

    let output = plan.explain_to_string()?;

    let rows = output
        .lines()
        .map(|s| Row::new(vec![Some(s.into())]))
        .collect::<Vec<_>>();

    Ok(PgResponse::new(
        StatementType::EXPLAIN,
        rows.len() as i32,
        rows,
        vec![PgFieldDescriptor::new(
            "QUERY PLAN".to_owned(),
            TypeOid::Varchar,
        )],
    ))
}
