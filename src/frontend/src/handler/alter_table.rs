// Copyright 2022 Singularity Data
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use risingwave_common::error::{ErrorCode, Result};
use risingwave_sqlparser::ast::{ColumnDef, ObjectName};

use super::{HandlerArgs, RwPgResponse};
use crate::binder::Relation;
use crate::Binder;

#[expect(clippy::unused_async)]
pub async fn handle_add_column(
    handler_args: HandlerArgs,
    table_name: ObjectName,
    new_column: ColumnDef,
) -> Result<RwPgResponse> {
    let session = handler_args.session;

    let catalog = {
        let relation = Binder::new(&session).bind_relation_by_name(table_name.clone(), None)?;
        match relation {
            Relation::BaseTable(table) if table.table_catalog.is_table() => table.table_catalog,
            _ => Err(ErrorCode::InvalidInputSyntax(format!(
                "\"{table_name}\" is not a table or cannot be altered"
            )))?,
        }
    };

    // Duplicated names can actually be checked by `StreamMaterialize`. We do here for better error
    // reporting.
    let new_column_name = new_column.name.real_value();
    if catalog
        .columns()
        .iter()
        .any(|c| c.name() == new_column_name)
    {
        Err(ErrorCode::InvalidInputSyntax(format!(
            "column \"{}\" of table \"{}\" already exists",
            new_column_name, table_name
        )))?
    }

    // let _new_column = {
    //     let column_id_offset = catalog.version.unwrap().next_column_id.get_id();
    //     let (columns, pk_id) = bind_sql_columns_with_offset(vec![new_column], column_id_offset)?;
    //     if pk_id.is_some() {
    //         Err(ErrorCode::NotImplemented(
    //             "add a primary key column".to_owned(),
    //             6903.into(),
    //         ))?
    //     }
    //     columns.into_iter().exactly_one().unwrap()
    // };

    Err(ErrorCode::NotImplemented(
        "ADD COLUMN".to_owned(),
        6903.into(),
    ))?
}
