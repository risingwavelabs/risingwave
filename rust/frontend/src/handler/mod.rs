use pgwire::pg_response::PgResponse;
use risingwave_common::error::{ErrorCode, Result};
use risingwave_sqlparser::ast::Statement;

use crate::session::RwSession;

mod create_source;
mod create_table;
mod explain;

pub(super) async fn handle(session: &RwSession, stmt: Statement) -> Result<PgResponse> {
    match stmt {
        Statement::Explain {
            statement, verbose, ..
        } => explain::handle_explain(*statement, verbose).await,
        Statement::CreateSource(stmt) => create_source::handle_create_source(session, stmt).await,
        Statement::CreateTable { name, columns, .. } => {
            create_table::handle_create_table(session, name, columns).await
        }
        _ => Err(ErrorCode::NotImplementedError(format!("Unhandled ast: {:?}", stmt)).into()),
    }
}
