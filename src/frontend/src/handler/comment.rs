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
use risingwave_pb::catalog::PbComment;
use risingwave_pb::common::ObjectType;
use risingwave_sqlparser::ast::{CommentObject, ObjectName};

use super::{HandlerArgs, RwPgResponse};
use crate::Binder;
use crate::catalog::root_catalog::SchemaPath;
use crate::catalog::table_catalog::TableType;
use crate::error::{ErrorCode, Result};

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
                    &session.database(),
                    &ObjectName(tab.to_vec()),
                )?;

                let table = binder.bind_table(schema.as_deref(), &table)?;
                let comment_object_type = match table.table_catalog.table_type() {
                    TableType::Table => ObjectType::Table,
                    TableType::MaterializedView => ObjectType::Mview,
                    _ => {
                        return Err(ErrorCode::WrongObjectType(format!(
                            "{} is not a table or materialized view",
                            table.table_catalog.name
                        ))
                        .into());
                    }
                };
                if table.table_catalog.owner != session.user_id() && !session.is_super_user() {
                    return Err(ErrorCode::PermissionDenied(format!(
                        "must be owner of relation {}",
                        table.table_catalog.name
                    ))
                    .into());
                }
                binder.bind_columns_to_context(col.real_value(), &table.table_catalog.columns)?;

                let column = binder.bind_column(object_name.0.as_slice())?;

                PbComment {
                    object_id: table.table_id.as_raw_id(),
                    object_type: comment_object_type as i32,
                    column_index: column.as_input_ref().map(|input_ref| input_ref.index as _),
                    description: comment,
                }
            }
            CommentObject::Table | CommentObject::MaterializedView => {
                let (schema, table) =
                    Binder::resolve_schema_qualified_name(&session.database(), &object_name)?;
                let table = binder.bind_table(schema.as_deref(), &table)?;

                let (expected_table_type, expected_object_name, comment_object_type) =
                    match object_type {
                        CommentObject::Table => (TableType::Table, "table", ObjectType::Table),
                        CommentObject::MaterializedView => (
                            TableType::MaterializedView,
                            "materialized view",
                            ObjectType::Mview,
                        ),
                        CommentObject::Column | CommentObject::View => unreachable!(),
                    };

                if table.table_catalog.table_type() != expected_table_type {
                    return Err(ErrorCode::WrongObjectType(format!(
                        "{} is not a {}",
                        table.table_catalog.name, expected_object_name
                    ))
                    .into());
                }
                if table.table_catalog.owner != session.user_id() && !session.is_super_user() {
                    return Err(ErrorCode::PermissionDenied(format!(
                        "must be owner of relation {}",
                        table.table_catalog.name
                    ))
                    .into());
                }

                PbComment {
                    object_id: table.table_id.as_raw_id(),
                    object_type: comment_object_type as i32,
                    column_index: None,
                    description: comment,
                }
            }
            CommentObject::View => {
                let (schema, view) =
                    Binder::resolve_schema_qualified_name(&session.database(), &object_name)?;
                let db_name = session.database();
                let search_path = session.config().search_path();
                let user_name = session.user_name();
                let schema_path = SchemaPath::new(schema.as_deref(), &search_path, &user_name);
                let reader = session.env().catalog_reader().read_guard();
                if let Ok((table_catalog, _)) =
                    reader.get_any_table_by_name(&db_name, schema_path, &view)
                {
                    return Err(ErrorCode::WrongObjectType(format!(
                        "{} is not a view",
                        table_catalog.name
                    ))
                    .into());
                }
                let (view_catalog, _) = reader.get_view_by_name(&db_name, schema_path, &view)?;

                if view_catalog.owner != session.user_id() && !session.is_super_user() {
                    return Err(ErrorCode::PermissionDenied(format!(
                        "must be owner of relation {}",
                        view_catalog.name
                    ))
                    .into());
                }

                PbComment {
                    object_id: view_catalog.id.as_raw_id(),
                    object_type: ObjectType::View as i32,
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
