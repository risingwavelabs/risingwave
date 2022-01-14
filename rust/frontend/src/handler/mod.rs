use risingwave_sqlparser::ast::Statement;

use crate::pgwire::pg_result::PgResult;
use crate::session::FrontendEnv;

mod explain;

pub(super) async fn handle(_env: &FrontendEnv, stmt: Statement) -> PgResult {
    match stmt {
        Statement::Explain {
            statement, verbose, ..
        } => explain::handle_explain(*statement, verbose),
        _ => format!("Unhandled ast: {:?}", stmt).into(),
    }
}
