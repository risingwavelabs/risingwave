use pgwire::pg_response::PgResponse;
use risingwave_common::error::{ErrorCode, Result};
use risingwave_sqlparser::ast::{ObjectName, Statement};

use crate::session::{QueryContext, SessionImpl};

mod create_source;
pub mod create_table;
pub mod drop_table;
mod explain;
pub mod util;

pub(super) async fn handle(session: &SessionImpl, stmt: Statement) -> Result<PgResponse> {
    let context = QueryContext::new(session.ctx.clone());
    match stmt {
        Statement::Explain {
            statement, verbose, ..
        } => explain::handle_explain(context, *statement, verbose),
        Statement::CreateSource(stmt) => create_source::handle_create_source(context, stmt).await,
        Statement::CreateTable { name, columns, .. } => {
            create_table::handle_create_table(context, name, columns).await
        }
        Statement::Drop(drop_statement) => {
            let table_object_name = ObjectName(vec![drop_statement.name]);
            drop_table::handle_drop_table(context, table_object_name).await
        }
        _ => Err(ErrorCode::NotImplementedError(format!("Unhandled ast: {:?}", stmt)).into()),
    }
}
