use pgwire::pg_response::PgResponse;
use risingwave_common::error::{ErrorCode, Result};
use risingwave_sqlparser::ast::Statement;
use risingwave_sqlparser::ast::{ColumnDef, DataType as AstDataType, ObjectName};
use crate::session::RwSession;

mod create_source;
mod create_table;
mod drop_table;
mod explain;

pub(super) async fn handle(session: &RwSession, stmt: Statement) -> Result<PgResponse> {
    match stmt {
        Statement::Explain {
            statement, verbose, ..
        } => explain::handle_explain(*statement, verbose),
        Statement::CreateSource(stmt) => create_source::handle_create_source(session, stmt).await,
        Statement::CreateTable { name, columns, .. } => {
            create_table::handle_create_table(session, name, columns).await
        }
        Statement::Drop(DropStatement) => {
            let table_object_name=ObjectName(vec![DropStatement.name]);
            drop_table::handle_drop_table(session, table_object_name).await
        }
        _ => Err(ErrorCode::NotImplementedError(format!("Unhandled ast: {:?}", stmt)).into()),
    }
}
