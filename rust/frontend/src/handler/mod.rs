use pgwire::pg_response::PgResponse;
use risingwave_common::error::{ErrorCode, Result};
use risingwave_sqlparser::ast::Statement;

use crate::session::RwSession;

mod create_source;
mod explain;

pub(super) async fn handle(session: &RwSession, stmt: Statement) -> Result<PgResponse> {
    match stmt {
        Statement::Explain {
            statement, verbose, ..
        } => explain::handle_explain(*statement, verbose),
        Statement::CreateSource(stmt) => create_source::handle_create_source(session, stmt).await,
        _ => Err(ErrorCode::NotImplementedError(format!("Unhandled ast: {:?}", stmt)).into()),
    }
}
