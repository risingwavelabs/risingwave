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

use risingwave_expr::{capture_context, function, ExprError, Result};
use risingwave_pb::user::grant_privilege::{Action, Object};
use thiserror_ext::AsReport;

use super::context::{CATALOG_READER, DB_NAME, USER_INFO_READER};
use crate::catalog::CatalogReader;
use crate::user::user_service::UserInfoReader;

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
    // currently, we haven't support grant for view.
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

#[function("has_schema_privilege(varchar, int4, varchar) -> boolean")]
fn has_schema_privilege(user_name: &str, schema_oid: i32, privileges: &str) -> Result<bool> {
    // does user have privilege for schema
    let allowed_actions = HashSet::from_iter([Action::Create, Action::Usage]);
    let actions = parse_privilege(privileges, &allowed_actions)?;
    has_privilege_impl_captured(user_name, &Object::SchemaId(schema_oid as u32), &actions)
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

#[capture_context(USER_INFO_READER)]
fn has_privilege_impl(
    user_info_reader: &UserInfoReader,
    user_name: &str,
    object: &Object,
    actions: &Vec<(Action, bool)>,
) -> Result<bool> {
    let user_info = &user_info_reader.read_guard();
    let user_catalog = user_info
        .get_user_by_name(user_name)
        .ok_or(user_not_found_err(
            format!("User {} not found", user_name).as_str(),
        ))?;
    Ok(user_catalog.check_privilege_with_grant_option(object, actions))
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
) -> Result<Object> {
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
                })
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
