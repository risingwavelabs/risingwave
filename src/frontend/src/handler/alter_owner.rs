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

use pgwire::pg_response::StatementType;
use risingwave_common::error::Result;
use risingwave_pb::ddl_service::alter_owner_request::Object;
use risingwave_sqlparser::ast::{Ident, ObjectName};

use super::{HandlerArgs, RwPgResponse};
use crate::catalog::root_catalog::SchemaPath;
use crate::catalog::CatalogError;
use crate::Binder;

pub async fn handle_alter_owner(
    handler_args: HandlerArgs,
    obj_name: ObjectName,
    new_owner_name: Ident,
    stmt_type: StatementType,
) -> Result<RwPgResponse> {
    let session = handler_args.session;
    let db_name = session.database();
    let (schema_name, real_obj_name) =
        Binder::resolve_schema_qualified_name(db_name, obj_name.clone())?;
    let search_path = session.config().get_search_path();
    let user_name = &session.auth_context().user_name;
    let schema_path = SchemaPath::new(schema_name.as_deref(), &search_path, user_name);

    let object = {
        let catalog_reader = session.env().catalog_reader().read_guard();
        match stmt_type {
            StatementType::ALTER_TABLE | StatementType::ALTER_MATERIALIZED_VIEW => {
                let (table, schema_name) =
                    catalog_reader.get_table_by_name(db_name, schema_path, &real_obj_name)?;
                session.check_privilege_for_drop_alter(schema_name, &**table)?;
                Object::TableId(table.id.table_id)
            }
            StatementType::ALTER_VIEW => {
                let (view, schema_name) =
                    catalog_reader.get_view_by_name(db_name, schema_path, &real_obj_name)?;
                session.check_privilege_for_drop_alter(schema_name, &**view)?;
                Object::ViewId(view.id)
            }
            StatementType::ALTER_SOURCE => {
                let (source, schema_name) =
                    catalog_reader.get_source_by_name(db_name, schema_path, &real_obj_name)?;
                session.check_privilege_for_drop_alter(schema_name, &**source)?;
                Object::SourceId(source.id)
            }
            StatementType::ALTER_SINK => {
                let (sink, schema_name) =
                    catalog_reader.get_sink_by_name(db_name, schema_path, &real_obj_name)?;
                session.check_privilege_for_drop_alter(schema_name, &**sink)?;
                Object::SinkId(sink.id.sink_id)
            }
            StatementType::ALTER_DATABASE => {
                let database = catalog_reader.get_database_by_name(&obj_name.real_value())?;
                session.check_privilege_for_drop_alter_db_schema(database)?;
                Object::DatabaseId(database.id())
            }
            StatementType::ALTER_SCHEMA => {
                let schema = catalog_reader.get_schema_by_name(db_name, &obj_name.real_value())?;
                session.check_privilege_for_drop_alter_db_schema(schema)?;
                Object::SchemaId(schema.id())
            }
            _ => unreachable!(),
        }
    };

    let new_owner_name = Binder::resolve_user_name(vec![new_owner_name].into())?;
    let owner_id = session
        .env()
        .user_info_reader()
        .read_guard()
        .get_user_by_name(&new_owner_name)
        .map(|u| u.id)
        .ok_or(CatalogError::NotFound("user", new_owner_name))?;

    let catalog_writer = session.catalog_writer()?;
    catalog_writer.alter_owner(object, owner_id).await?;

    Ok(RwPgResponse::empty_result(stmt_type))
}
