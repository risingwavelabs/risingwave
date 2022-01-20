use risingwave_common::error::{ErrorCode, Result};
use risingwave_sqlparser::ast::Statement;

use crate::pgwire::pg_response::PgResponse;
use crate::session::FrontendEnv;

mod explain;

pub(super) async fn handle(_env: &FrontendEnv, stmt: Statement) -> Result<PgResponse> {
    match stmt {
        Statement::Explain {
            statement, verbose, ..
        } => explain::handle_explain(*statement, verbose),
        _ => Err(ErrorCode::NotImplementedError(format!("Unhandled ast: {:?}", stmt)).into()),
    }
}
