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

use std::collections::HashSet;

use risingwave_common::session_config::SearchPath;
use risingwave_expr::{ExprError, Result, capture_context, function};
use risingwave_pb::user::Action;
use risingwave_pb::user::grant_privilege::Object;
use risingwave_sqlparser::parser::Parser;
use thiserror_ext::AsReport;

use super::context::{AUTH_CONTEXT, CATALOG_READER, DB_NAME, SEARCH_PATH, USER_INFO_READER};
use crate::catalog::root_catalog::SchemaPath;
use crate::catalog::system_catalog::is_system_catalog;
use crate::catalog::{CatalogReader, DatabaseId, OwnedGrantObject, SchemaId};
use crate::session::AuthContext;
use crate::user::user_service::UserInfoReader;
use crate::{Binder, bind_data_type};

#[inline(always)]
pub fn user_not_found_err(inner_err: &str) -> ExprError {
    ExprError::InvalidParam {
        name: "user",
        reason: inner_err.into(),
    }
}

#[function("has_table_privilege(int4, int4, varchar) -> boolean")]
fn has_table_privilege(user_id: i32, table_oid: i32, privileges: &str) -> Result<bool> {
    // does user have privilege for table
    let user_name = get_user_name_by_id_captured(user_id)?;
    has_table_privilege_1(user_name.as_str(), table_oid, privileges)
}

#[function("has_table_privilege(varchar, int4, varchar) -> boolean")]
fn has_table_privilege_1(user_name: &str, table_oid: i32, privileges: &str) -> Result<bool> {
    let allowed_actions = HashSet::new();
    let actions = parse_privilege(privileges, &allowed_actions)?;
    if is_system_catalog(table_oid as _) {
        return Ok(true);
    }
    has_privilege_impl_captured(
        user_name,
        &get_grant_object_by_oid_captured(table_oid)?,
        &actions,
    )
}

#[function("has_any_column_privilege(int4, int4, varchar) -> boolean")]
fn has_any_column_privilege(user_id: i32, table_oid: i32, privileges: &str) -> Result<bool> {
    // does user have privilege for any column of table
    let user_name = get_user_name_by_id_captured(user_id)?;
    has_any_column_privilege_1(user_name.as_str(), table_oid, privileges)
}

#[function("has_any_column_privilege(varchar, int4, varchar) -> boolean")]
fn has_any_column_privilege_1(user_name: &str, table_oid: i32, privileges: &str) -> Result<bool> {
    let allowed_actions = HashSet::from_iter([Action::Select, Action::Insert, Action::Update]);
    let actions = parse_privilege(privileges, &allowed_actions)?;
    has_privilege_impl_captured(
        user_name,
        &get_grant_object_by_oid_captured(table_oid)?,
        &actions,
    )
}

#[function("has_database_privilege(varchar, int4, varchar) -> boolean")]
fn has_database_privilege(user_name: &str, database_oid: i32, privileges: &str) -> Result<bool> {
    // does user have privilege for database
    let database_owner = get_database_owner_by_id_captured(database_oid as DatabaseId)?;
    let allowed_actions = HashSet::from_iter([Action::Create, Action::Connect]);
    let actions = parse_privilege(privileges, &allowed_actions)?;
    has_privilege_impl_captured(
        user_name,
        &OwnedGrantObject {
            object: Object::DatabaseId(database_oid as u32),
            owner: database_owner as u32,
        },
        &actions,
    )
}

#[function("has_database_privilege(int4, varchar, varchar) -> boolean")]
fn has_database_privilege_1(user_id: i32, database_name: &str, privileges: &str) -> Result<bool> {
    let user_name = get_user_name_by_id_captured(user_id)?;
    let database_oid = get_database_id_by_name_captured(database_name)?;
    has_database_privilege(user_name.as_str(), database_oid, privileges)
}

#[function("has_database_privilege(int4, int4, varchar) -> boolean")]
fn has_database_privilege_2(user_id: i32, database_oid: i32, privileges: &str) -> Result<bool> {
    let user_name = get_user_name_by_id_captured(user_id)?;
    has_database_privilege(user_name.as_str(), database_oid, privileges)
}

#[function("has_database_privilege(varchar, varchar, varchar) -> boolean")]
fn has_database_privilege_3(
    user_name: &str,
    database_name: &str,
    privileges: &str,
) -> Result<bool> {
    let database_oid = get_database_id_by_name_captured(database_name)?;
    has_database_privilege(user_name, database_oid, privileges)
}

#[function("has_schema_privilege(varchar, int4, varchar) -> boolean")]
fn has_schema_privilege(user_name: &str, schema_oid: i32, privileges: &str) -> Result<bool> {
    // does user have privilege for schema
    let schema_owner = get_schema_owner_by_id_captured(schema_oid as SchemaId)?;
    let allowed_actions = HashSet::from_iter([Action::Create, Action::Usage]);
    let actions = parse_privilege(privileges, &allowed_actions)?;
    has_privilege_impl_captured(
        user_name,
        &OwnedGrantObject {
            object: Object::SchemaId(schema_oid as u32),
            owner: schema_owner as u32,
        },
        &actions,
    )
}

#[function("has_schema_privilege(int4, varchar, varchar) -> boolean")]
fn has_schema_privilege_1(user_id: i32, schema_name: &str, privileges: &str) -> Result<bool> {
    let user_name = get_user_name_by_id_captured(user_id)?;
    let schema_oid = get_schema_id_by_name_captured(schema_name)?;
    has_schema_privilege(user_name.as_str(), schema_oid, privileges)
}

#[function("has_schema_privilege(int4, int4, varchar) -> boolean")]
fn has_schema_privilege_2(user_id: i32, schema_oid: i32, privileges: &str) -> Result<bool> {
    let user_name = get_user_name_by_id_captured(user_id)?;
    has_schema_privilege(user_name.as_str(), schema_oid, privileges)
}

#[function("has_schema_privilege(varchar, varchar, varchar) -> boolean")]
fn has_schema_privilege_3(user_name: &str, schema_name: &str, privileges: &str) -> Result<bool> {
    let schema_oid = get_schema_id_by_name_captured(schema_name)?;
    has_schema_privilege(user_name, schema_oid, privileges)
}

#[function("has_function_privilege(varchar, int4, varchar) -> boolean")]
fn has_function_privilege(user_name: &str, function_oid: i32, privileges: &str) -> Result<bool> {
    // does user have privilege for function
    let func_obj = get_grant_object_by_oid_captured(function_oid)?;
    let allowed_actions = HashSet::from_iter([Action::Execute]);
    let actions = parse_privilege(privileges, &allowed_actions)?;
    has_privilege_impl_captured(user_name, &func_obj, &actions)
}

#[function("has_function_privilege(int4, int4, varchar) -> boolean")]
fn has_function_privilege_1(user_id: i32, function_oid: i32, privileges: &str) -> Result<bool> {
    let user_name = get_user_name_by_id_captured(user_id)?;
    has_function_privilege(user_name.as_str(), function_oid, privileges)
}

#[function("has_function_privilege(varchar, varchar, varchar) -> boolean")]
fn has_function_privilege_2(
    user_name: &str,
    function_name: &str,
    privileges: &str,
) -> Result<bool> {
    let function_oid = get_function_id_by_name_captured(function_name)?;
    has_function_privilege(user_name, function_oid, privileges)
}

#[function("has_function_privilege(int4, varchar, varchar) -> boolean")]
fn has_function_privilege_3(user_id: i32, function_name: &str, privileges: &str) -> Result<bool> {
    let user_name = get_user_name_by_id_captured(user_id)?;
    let function_oid = get_function_id_by_name_captured(function_name)?;
    has_function_privilege(user_name.as_str(), function_oid, privileges)
}

#[capture_context(USER_INFO_READER)]
fn has_privilege_impl(
    user_info_reader: &UserInfoReader,
    user_name: &str,
    object: &OwnedGrantObject,
    actions: &Vec<(Action, bool)>,
) -> Result<bool> {
    let user_info = &user_info_reader.read_guard();
    let user_catalog = user_info
        .get_user_by_name(user_name)
        .ok_or(user_not_found_err(
            format!("User {} not found", user_name).as_str(),
        ))?;
    if user_catalog.id == object.owner {
        // if the user is the owner of the object, they have all privileges
        return Ok(true);
    }
    Ok(user_catalog.check_privilege_with_grant_option(&object.object, actions))
}

#[capture_context(USER_INFO_READER)]
fn get_user_name_by_id(user_info_reader: &UserInfoReader, user_id: i32) -> Result<String> {
    let user_info = &user_info_reader.read_guard();
    user_info
        .get_user_name_by_id(user_id as u32)
        .ok_or(user_not_found_err(
            format!("User {} not found", user_id).as_str(),
        ))
}

#[capture_context(CATALOG_READER, DB_NAME)]
fn get_grant_object_by_oid(
    catalog_reader: &CatalogReader,
    db_name: &str,
    oid: i32,
) -> Result<OwnedGrantObject> {
    catalog_reader
        .read_guard()
        .get_database_by_name(db_name)
        .map_err(|e| ExprError::InvalidParam {
            name: "oid",
            reason: e.to_report_string().into(),
        })?
        .get_grant_object_by_oid(oid as u32)
        .ok_or(ExprError::InvalidParam {
            name: "oid",
            reason: format!("Table {} not found", oid).as_str().into(),
        })
}

#[capture_context(CATALOG_READER)]
fn get_database_id_by_name(catalog_reader: &CatalogReader, db_name: &str) -> Result<i32> {
    let reader = &catalog_reader.read_guard();
    Ok(reader
        .get_database_by_name(db_name)
        .map_err(|e| ExprError::InvalidParam {
            name: "database",
            reason: e.to_report_string().into(),
        })?
        .id() as i32)
}

#[capture_context(CATALOG_READER)]
fn get_database_owner_by_id(
    catalog_reader: &CatalogReader,
    database_id: DatabaseId,
) -> Result<i32> {
    let reader = &catalog_reader.read_guard();
    let database =
        reader
            .get_database_by_id(&database_id)
            .map_err(|e| ExprError::InvalidParam {
                name: "database",
                reason: e.to_report_string().into(),
            })?;
    Ok(database.owner as i32)
}

#[capture_context(CATALOG_READER, DB_NAME)]
fn get_schema_owner_by_id(
    catalog_reader: &CatalogReader,
    db_name: &str,
    schema_id: SchemaId,
) -> Result<i32> {
    let reader = &catalog_reader.read_guard();
    let db_id = reader
        .get_database_by_name(db_name)
        .map_err(|e| ExprError::InvalidParam {
            name: "database",
            reason: e.to_report_string().into(),
        })?
        .id();
    Ok(reader
        .get_schema_by_id(&db_id, &schema_id)
        .map_err(|e| ExprError::InvalidParam {
            name: "schema",
            reason: e.to_report_string().into(),
        })?
        .owner as i32)
}

#[capture_context(CATALOG_READER, DB_NAME)]
fn get_schema_id_by_name(
    catalog_reader: &CatalogReader,
    db_name: &str,
    schema_name: &str,
) -> Result<i32> {
    let reader = &catalog_reader.read_guard();
    Ok(reader
        .get_schema_by_name(db_name, schema_name)
        .map_err(|e| ExprError::InvalidParam {
            name: "schema",
            reason: e.to_report_string().into(),
        })?
        .id() as i32)
}

#[capture_context(CATALOG_READER, DB_NAME, AUTH_CONTEXT, SEARCH_PATH)]
fn get_function_id_by_name(
    catalog_reader: &CatalogReader,
    db_name: &str,
    auth_context: &AuthContext,
    search_path: &SearchPath,
    function_name: &str,
) -> Result<i32> {
    let desc =
        Parser::parse_function_desc_str(function_name).map_err(|e| ExprError::InvalidParam {
            name: "function",
            reason: e.to_report_string().into(),
        })?;
    let mut arg_types = vec![];
    if let Some(args) = desc.args {
        for arg in &args {
            arg_types.push(bind_data_type(&arg.data_type).map_err(|e| {
                ExprError::InvalidParam {
                    name: "function",
                    reason: e.to_report_string().into(),
                }
            })?);
        }
    }

    let (schema, name) =
        Binder::resolve_schema_qualified_name(db_name, desc.name).map_err(|e| {
            ExprError::InvalidParam {
                name: "function",
                reason: e.to_report_string().into(),
            }
        })?;
    let schema_path = SchemaPath::new(schema.as_deref(), search_path, &auth_context.user_name);

    let reader = &catalog_reader.read_guard();
    Ok(reader
        .get_function_by_name_args(db_name, schema_path, &name, &arg_types)
        .map_err(|e| ExprError::InvalidParam {
            name: "function",
            reason: e.to_report_string().into(),
        })?
        .0
        .id
        .function_id() as i32)
}

fn parse_privilege(
    privilege_string: &str,
    allowed_actions: &HashSet<Action>,
) -> Result<Vec<(Action, bool)>> {
    let mut privileges = Vec::new();
    for part in privilege_string.split(',').map(str::trim) {
        let (privilege_type, grant_option) = match part.rsplit_once(" WITH GRANT OPTION") {
            Some((p, _)) => (p, true),
            None => (part, false),
        };
        match Action::from_str_name(privilege_type.to_uppercase().as_str()) {
            Some(Action::Unspecified) | None => {
                return Err(ExprError::InvalidParam {
                    name: "privilege",
                    reason: format!("unrecognized privilege type: \"{}\"", part).into(),
                });
            }
            Some(action) => {
                if allowed_actions.is_empty() || allowed_actions.contains(&action) {
                    privileges.push((action, grant_option))
                } else {
                    return Err(ExprError::InvalidParam {
                        name: "privilege",
                        reason: format!("unrecognized privilege type: \"{}\"", part).into(),
                    });
                }
            }
        }
    }
    Ok(privileges)
}
