use risingwave_sqlparser::ast::Statement;

use crate::pgwire::pg_result::PgResult;

mod explain;

pub(super) fn handle(stmt: Statement) -> PgResult {
    match stmt {
        Statement::Explain {
            statement, verbose, ..
        } => explain::handle_explain(*statement, verbose),
        _ => format!("Unhandled ast: {:?}", stmt).into(),
    }
}
