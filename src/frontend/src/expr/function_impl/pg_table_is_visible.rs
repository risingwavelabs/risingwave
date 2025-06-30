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

use risingwave_common::acl::AclMode;
use risingwave_common::session_config::SearchPath;
use risingwave_expr::{Result, capture_context, function};
use risingwave_pb::user::grant_privilege::Object as GrantObject;

use super::context::{AUTH_CONTEXT, CATALOG_READER, DB_NAME, SEARCH_PATH, USER_INFO_READER};
use crate::catalog::CatalogReader;
use crate::catalog::system_catalog::is_system_catalog;
use crate::expr::function_impl::has_privilege::user_not_found_err;
use crate::session::AuthContext;
use crate::user::user_service::UserInfoReader;

#[function("pg_table_is_visible(int4) -> boolean")]
fn pg_table_is_visible(oid: i32) -> Result<Option<bool>> {
    pg_table_is_visible_impl_captured(oid)
}

#[capture_context(CATALOG_READER, USER_INFO_READER, AUTH_CONTEXT, SEARCH_PATH, DB_NAME)]
fn pg_table_is_visible_impl(
    catalog: &CatalogReader,
    user_info: &UserInfoReader,
    auth_context: &AuthContext,
    search_path: &SearchPath,
    db_name: &str,
    oid: i32,
) -> Result<Option<bool>> {
    // To maintain consistency with PostgreSQL, we ensure that system catalogs are always visible.
    if is_system_catalog(oid as u32) {
        return Ok(Some(true));
    }

    let catalog_reader = catalog.read_guard();
    let user_reader = user_info.read_guard();
    let user_info = user_reader
        .get_user_by_name(&auth_context.user_name)
        .ok_or(user_not_found_err(
            format!("User {} not found", auth_context.user_name).as_str(),
        ))?;
    // Return true only if:
    // 1. The schema of the object exists in the search path.
    // 2. User have `USAGE` privilege on the schema.
    for schema in search_path.path() {
        if let Ok(schema) = catalog_reader.get_schema_by_name(db_name, schema)
            && schema.contains_object(oid as u32) {
                return if user_info.is_super
                    || user_info.has_privilege(&GrantObject::SchemaId(schema.id()), AclMode::Usage)
                {
                    Ok(Some(true))
                } else {
                    Ok(Some(false))
                };
            }
    }

    Ok(None)
}
