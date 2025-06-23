// Copyright 2025 RisingWave Labs
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

use std::sync::Arc;

use pgwire::pg_response::StatementType;
use risingwave_common::acl::AclMode;
use risingwave_common::catalog::DEFAULT_SUPER_USER_FOR_ADMIN;
use risingwave_pb::ddl_service::alter_owner_request::Object;
use risingwave_pb::user::grant_privilege;
use risingwave_sqlparser::ast::{Ident, ObjectName};

use super::{HandlerArgs, RwPgResponse};
use crate::Binder;
use crate::catalog::root_catalog::SchemaPath;
use crate::catalog::{CatalogError, OwnedByUserCatalog};
use crate::error::ErrorCode::PermissionDenied;
use crate::error::Result;
use crate::session::SessionImpl;
use crate::user::user_catalog::UserCatalog;

pub fn check_schema_create_privilege(
    session: &Arc<SessionImpl>,
    new_owner: &UserCatalog,
    schema_id: u32,
) -> Result<()> {
    if session.is_super_user() {
        return Ok(());
    }
    if !new_owner.is_super
        && !new_owner.has_privilege(
            &grant_privilege::Object::SchemaId(schema_id),
            AclMode::Create,
        )
    {
        return Err(PermissionDenied(
            "Require new owner to have create privilege on the object.".to_owned(),
        )
        .into());
    }
    Ok(())
}

pub async fn handle_alter_owner(
    handler_args: HandlerArgs,
    obj_name: ObjectName,
    new_owner_name: Ident,
    stmt_type: StatementType,
) -> Result<RwPgResponse> {
    let session = handler_args.session;
    let db_name = &session.database();
    let (schema_name, real_obj_name) =
        Binder::resolve_schema_qualified_name(db_name, obj_name.clone())?;
    let search_path = session.config().search_path();
    let user_name = &session.user_name();
    let schema_path = SchemaPath::new(schema_name.as_deref(), &search_path, user_name);

    let new_owner_name = Binder::resolve_user_name(vec![new_owner_name].into())?;
    let (object, owner_id) = {
        let catalog_reader = session.env().catalog_reader().read_guard();
        let user_reader = session.env().user_info_reader().read_guard();
        let new_owner = user_reader
            .get_user_by_name(&new_owner_name)
            .ok_or(CatalogError::NotFound("user", new_owner_name))?;
        if new_owner.name == DEFAULT_SUPER_USER_FOR_ADMIN {
            return Err(PermissionDenied(
                format!("{} is reserved for admin", DEFAULT_SUPER_USER_FOR_ADMIN).to_owned(),
            )
            .into());
        }
        let owner_id = new_owner.id;
        (
            match stmt_type {
                StatementType::ALTER_TABLE | StatementType::ALTER_MATERIALIZED_VIEW => {
                    let (table, schema_name) = catalog_reader.get_created_table_by_name(
                        db_name,
                        schema_path,
                        &real_obj_name,
                    )?;
                    session.check_privilege_for_drop_alter(schema_name, &**table)?;
                    let schema_id = catalog_reader
                        .get_schema_by_name(db_name, schema_name)?
                        .id();
                    check_schema_create_privilege(&session, new_owner, schema_id)?;
                    if table.owner() == owner_id {
                        return Ok(RwPgResponse::empty_result(stmt_type));
                    }
                    Object::TableId(table.id.table_id)
                }
                StatementType::ALTER_VIEW => {
                    let (view, schema_name) =
                        catalog_reader.get_view_by_name(db_name, schema_path, &real_obj_name)?;
                    session.check_privilege_for_drop_alter(schema_name, &**view)?;
                    let schema_id = catalog_reader
                        .get_schema_by_name(db_name, schema_name)?
                        .id();
                    check_schema_create_privilege(&session, new_owner, schema_id)?;
                    if view.owner() == owner_id {
                        return Ok(RwPgResponse::empty_result(stmt_type));
                    }
                    Object::ViewId(view.id)
                }
                StatementType::ALTER_SOURCE => {
                    let (source, schema_name) =
                        catalog_reader.get_source_by_name(db_name, schema_path, &real_obj_name)?;
                    session.check_privilege_for_drop_alter(schema_name, &**source)?;
                    let schema_id = catalog_reader
                        .get_schema_by_name(db_name, schema_name)?
                        .id();
                    check_schema_create_privilege(&session, new_owner, schema_id)?;
                    if source.owner() == owner_id {
                        return Ok(RwPgResponse::empty_result(stmt_type));
                    }
                    Object::SourceId(source.id)
                }
                StatementType::ALTER_SINK => {
                    let (sink, schema_name) =
                        catalog_reader.get_sink_by_name(db_name, schema_path, &real_obj_name)?;
                    session.check_privilege_for_drop_alter(schema_name, &**sink)?;
                    let schema_id = catalog_reader
                        .get_schema_by_name(db_name, schema_name)?
                        .id();
                    check_schema_create_privilege(&session, new_owner, schema_id)?;
                    if sink.owner() == owner_id {
                        return Ok(RwPgResponse::empty_result(stmt_type));
                    }
                    Object::SinkId(sink.id.sink_id)
                }
                StatementType::ALTER_SUBSCRIPTION => {
                    let (subscription, schema_name) = catalog_reader.get_subscription_by_name(
                        db_name,
                        schema_path,
                        &real_obj_name,
                    )?;
                    session.check_privilege_for_drop_alter(schema_name, &**subscription)?;
                    let schema_id = catalog_reader
                        .get_schema_by_name(db_name, schema_name)?
                        .id();
                    check_schema_create_privilege(&session, new_owner, schema_id)?;
                    if subscription.owner() == owner_id {
                        return Ok(RwPgResponse::empty_result(stmt_type));
                    }
                    Object::SubscriptionId(subscription.id.subscription_id)
                }
                StatementType::ALTER_DATABASE => {
                    let database = catalog_reader.get_database_by_name(&obj_name.real_value())?;
                    session.check_privilege_for_drop_alter_db_schema(database)?;
                    if database.owner() == owner_id {
                        return Ok(RwPgResponse::empty_result(stmt_type));
                    }
                    Object::DatabaseId(database.id())
                }
                StatementType::ALTER_SCHEMA => {
                    let schema =
                        catalog_reader.get_schema_by_name(db_name, &obj_name.real_value())?;
                    session.check_privilege_for_drop_alter_db_schema(schema)?;
                    if schema.owner() == owner_id {
                        return Ok(RwPgResponse::empty_result(stmt_type));
                    }
                    Object::SchemaId(schema.id())
                }
                StatementType::ALTER_CONNECTION => {
                    let (connection, schema_name) = catalog_reader.get_connection_by_name(
                        db_name,
                        schema_path,
                        &real_obj_name,
                    )?;
                    session.check_privilege_for_drop_alter(schema_name, &**connection)?;
                    if connection.owner() == owner_id {
                        return Ok(RwPgResponse::empty_result(stmt_type));
                    }
                    Object::ConnectionId(connection.id)
                }
                _ => unreachable!(),
            },
            owner_id,
        )
    };

    let catalog_writer = session.catalog_writer()?;
    catalog_writer.alter_owner(object, owner_id).await?;

    Ok(RwPgResponse::empty_result(stmt_type))
}
