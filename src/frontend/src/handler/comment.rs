// Copyright 2024 RisingWave Labs
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
use risingwave_pb::catalog::PbComment;
use risingwave_sqlparser::ast::{CommentObject, ObjectName};

use super::{HandlerArgs, RwPgResponse};
use crate::error::{ErrorCode, Result};
use crate::Binder;

pub async fn handle_comment(
    handler_args: HandlerArgs,
    object_type: CommentObject,
    object_name: ObjectName,
    comment: Option<String>,
) -> Result<RwPgResponse> {
    let session = handler_args.session;
    let comment = comment.filter(|s| !s.is_empty());

    let comment = {
        let mut binder = Binder::new_for_ddl(&session);
        // only `Column` and `Table` object are now supported
        match object_type {
            CommentObject::Column => {
                let [tab @ .., col] = object_name.0.as_slice() else {
                    return Err(ErrorCode::BindError(format!(
                        "Invalid column: {}",
                        object_name.real_value()
                    ))
                    .into());
                };

                let (schema, table) = Binder::resolve_schema_qualified_name(
                    session.database(),
                    ObjectName(tab.to_vec()),
                )?;

                let (database_id, schema_id) =
                    session.get_database_and_schema_id_for_create(schema.clone())?;
                let table = binder.bind_table(schema.as_deref(), &table, None)?;
                binder.bind_columns_to_context(col.real_value(), &table.table_catalog.columns)?;

                let column = binder.bind_column(object_name.0.as_slice())?;

                PbComment {
                    table_id: table.table_id.into(),
                    schema_id,
                    database_id,
                    column_index: column.as_input_ref().map(|input_ref| input_ref.index as _),
                    description: comment,
                }
            }
            CommentObject::Table => {
                let (schema, table) =
                    Binder::resolve_schema_qualified_name(session.database(), object_name)?;
                let (database_id, schema_id) =
                    session.get_database_and_schema_id_for_create(schema.clone())?;
                let table = binder.bind_table(schema.as_deref(), &table, None)?;

                PbComment {
                    table_id: table.table_id.into(),
                    schema_id,
                    database_id,
                    column_index: None,
                    description: comment,
                }
            }
        }
    };

    let catalog_writer = session.catalog_writer()?;
    catalog_writer.comment_on(comment).await?;

    Ok(PgResponse::empty_result(StatementType::COMMENT))
}
