use risingwave_common::error::{ErrorCode, Result};
use risingwave_sqlparser::ast::{ColumnDef, ObjectName};

use super::{HandlerArgs, RwPgResponse};
use crate::binder::Relation;
use crate::Binder;

pub async fn handle_add_column(
    handler_args: HandlerArgs,
    table_name: ObjectName,
    new_column: ColumnDef,
) -> Result<RwPgResponse> {
    let session = handler_args.session;

    let catalog = {
        let relation = Binder::new(&session).bind_relation_by_name(table_name.clone(), None)?;
        match relation {
            Relation::BaseTable(table) => table.table_catalog,
            _ => Err(ErrorCode::InvalidInputSyntax(format!(
                "\"{table_name}\" is not a table or cannot be altered"
            )))?,
        }
    };

    let new_column_name = new_column.name.real_value();
    if catalog
        .columns()
        .iter()
        .find(|c| c.name() == &new_column_name)
        .is_some()
    {
        Err(ErrorCode::InvalidInputSyntax(format!(
            "column \"\" of table \"\" already exists"
        )))?
    }

    todo!()
}
