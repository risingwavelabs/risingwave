use pgwire::pg_response::PgResponse;
use risingwave_common::error::{ErrorCode, Result};
use risingwave_sqlparser::ast::{ObjectName, Statement};

use crate::session::RwSession;

mod create_source;
mod create_table;
mod drop_table;
mod explain;

pub(super) async fn handle(session: &RwSession, stmt: Statement) -> Result<PgResponse> {
    match stmt {
        Statement::Explain {
            statement, verbose, ..
        } => explain::handle_explain(session, *statement, verbose).await,
        Statement::CreateSource(stmt) => create_source::handle_create_source(session, stmt).await,
        Statement::CreateTable { name, columns, .. } => {
            create_table::handle_create_table(session, name, columns).await
        }
        Statement::Drop(drop_statement) => {
            let table_object_name = ObjectName(vec![drop_statement.name]);
            drop_table::handle_drop_table(session, table_object_name).await
        }
        _ => Err(ErrorCode::NotImplementedError(format!("Unhandled ast: {:?}", stmt)).into()),
    }
}
