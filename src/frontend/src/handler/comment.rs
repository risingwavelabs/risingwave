// Copyright 2023 RisingWave Labs
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

use pgwire::pg_response::{PgResponse, StatementType};
use risingwave_common::error::{ErrorCode, Result};
use risingwave_sqlparser::ast::{CommentObject, ObjectName};

use super::{HandlerArgs, RwPgResponse};
use crate::Binder;

pub async fn handle_comment(
    handler_args: HandlerArgs,
    object_type: CommentObject,
    object_name: ObjectName,
    comment: Option<String>,
) -> Result<RwPgResponse> {
    let session = handler_args.session;

    let (table_id, column_index) = {
        let mut binder = Binder::new_for_ddl(&session);
        match object_type {
            CommentObject::Column => {
                // TODO: wait to ask: How to bind `t.col`
                let [.., tab, col] = object_name.0.as_slice() else {
                    return Err(ErrorCode::BindError(format!(
                        "Invalid column: {}",
                        object_name.real_value()
                    ))
                    .into());
                };

                let table = binder.bind_table(None, &tab.real_value(), None)?;
                binder.bind_columns_to_context(col.real_value(), table.table_catalog.columns)?;

                let column = binder.bind_column(object_name.0.as_slice())?;

                (
                    table.table_id,
                    column
                        .as_input_ref()
                        .map(|input_ref| input_ref.index + 1) // +1 since `_row_id`
                        .unwrap_or_default(),
                )
            }
            CommentObject::Table => {
                let table = binder.bind_table(None, &object_name.real_value(), None)?;
                (table.table_id, 0)
            }
        }
    };

    let catalog_writer = session.catalog_writer()?;
    catalog_writer
        .create_comment(table_id, column_index as _, comment)
        .await?;

    Ok(PgResponse::empty_result(StatementType::COMMENT))
}
