use std::cell::RefCell;
use std::rc::Rc;

use pgwire::pg_response::PgResponse;
use risingwave_common::error::{ErrorCode, Result};
use risingwave_sqlparser::ast::{ObjectName, Query, Statement};

use crate::binder::{self, Binder};
use crate::catalog::CatalogError;
use crate::planner::Planner;
use crate::session::QueryContext;

pub async fn handle_create_mv(
    context: QueryContext,
    name: ObjectName,
    query: Box<Query>,
) -> Result<PgResponse> {
    let (schema_name, table_name) = Binder::resolve_table_name(name.clone())?;
    let session = context.session_ctx.clone();
    let (db_id, schema_id) = {
        // use local catalog to check if the create mv is legal
        let catalog_reader = session.env().catalog_reader();
        let catalog_read_guard = catalog_reader.read_guard();
        let db = catalog_read_guard
            .get_database_by_name(session.database())
            .ok_or_else(|| {
                ErrorCode::InternalError(format!(
                    "database {} catalog not found",
                    session.database()
                ))
            })?;
        let schema = db.get_schema_by_name(&schema_name).ok_or_else(|| {
            ErrorCode::InternalError(format!("schema {} catalog not found", schema_name))
        })?;
        if let Some(table) = schema.get_table_by_name(&table_name) {
            // TODO: check if it is a materivalized source and improve the err msg
            return Err(
                CatalogError::Duplicated("materivalized view", schema_name.to_string()).into(),
            );
        }
        (db.id(), schema.id())
    };
    let bound = {
        let catalog_reader = session.env().catalog_reader();
        let catalog_read_guard = catalog_reader.read_guard();
        let mut binder = Binder::new(catalog_read_guard, session.database().to_string());
        binder.bind(Statement::Query(query))?
    };
    let catalog_writer = session.env().catalog_writer();
    let plan = Planner::new(Rc::new(RefCell::new(context)))
        .plan(bound)?
        .gen_create_mv_plan()
        .to_stream_prost();

    tracing::info!(name= ?name, plan = ?plan);

    Ok(PgResponse::new(
        pgwire::pg_response::StatementType::CREATE_MATERIALIZED_VIEW,
        0,
        vec![],
        vec![],
    ))
}
