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
use risingwave_common::id::ObjectId;
use risingwave_common::session_config::{SearchPath, USER_NAME_WILD_CARD};
use risingwave_expr::{Result, capture_context, function};

use super::context::{
    AUTH_CONTEXT, CATALOG_READER, DB_NAME, ROLE_MEMBERSHIP_INFO_READER, SEARCH_PATH,
    USER_INFO_READER,
};
use crate::catalog::schema_catalog::SchemaCatalog;
use crate::catalog::system_catalog::is_system_catalog;
use crate::catalog::{CatalogReader, OwnedByUserCatalog};
use crate::session::AuthContext;
use crate::user::effective_privilege::{role_memberships_snapshot, session_has_privilege};
use crate::user::user_service::{RoleMembershipInfoReader, UserInfoReader};

#[function("pg_table_is_visible(int4) -> boolean")]
fn pg_table_is_visible(oid: i32) -> Result<Option<bool>> {
    pg_table_is_visible_impl_captured(ObjectId::new(oid as _))
}

#[capture_context(
    CATALOG_READER,
    SEARCH_PATH,
    DB_NAME,
    AUTH_CONTEXT,
    USER_INFO_READER,
    ROLE_MEMBERSHIP_INFO_READER
)]
fn pg_table_is_visible_impl(
    catalog: &CatalogReader,
    search_path: &SearchPath,
    db_name: &str,
    auth_context: &AuthContext,
    user_info_reader: &UserInfoReader,
    role_membership_reader: &RoleMembershipInfoReader,
    oid: ObjectId,
) -> Result<Option<bool>> {
    // To maintain consistency with PostgreSQL, we ensure that system catalogs are always visible.
    if is_system_catalog(oid) {
        return Ok(Some(true));
    }

    let catalog_reader = catalog.read_guard();
    let Some(target_name) = catalog_reader
        .iter_schemas(db_name)
        .ok()
        .and_then(|mut schemas| schemas.find_map(|schema| relation_name_by_oid(schema, oid)))
    else {
        return Ok(None);
    };

    let current_user_name = user_info_reader
        .read_guard()
        .get_user_by_id(&auth_context.current_user_id())
        .map(|user| user.name.clone());
    let memberships = role_memberships_snapshot(role_membership_reader);
    for schema_name in search_path.path() {
        let schema_name = if schema_name == USER_NAME_WILD_CARD {
            let Some(user_name) = current_user_name.as_deref() else {
                continue;
            };
            user_name
        } else {
            schema_name
        };
        if let Ok(schema) = catalog_reader.get_schema_by_name(db_name, schema_name)
            && session_has_privilege(
                user_info_reader,
                auth_context,
                &memberships,
                schema.owner(),
                schema.id(),
                AclMode::Usage,
            )
            && let Some(found_oid) = relation_oid_by_name(schema, &target_name)
        {
            return Ok(Some(found_oid == oid));
        }
    }

    Ok(Some(false))
}

fn relation_name_by_oid(schema: &SchemaCatalog, oid: ObjectId) -> Option<String> {
    if let Some(table) = schema.get_created_table_by_id(oid.as_table_id()) {
        Some(table.name.clone())
    } else if let Some(view) = schema.get_view_by_id(oid.as_view_id()) {
        Some(view.name.clone())
    } else if let Some(source) = schema.get_source_by_id(oid.as_source_id()) {
        Some(source.name.clone())
    } else if let Some(sink) = schema.get_sink_by_id(oid.as_sink_id()) {
        Some(sink.name.clone())
    } else if let Some(index) = schema.get_index_by_id(oid.as_index_id()) {
        Some(index.name.clone())
    } else {
        schema
            .get_subscription_by_id(oid.as_subscription_id())
            .map(|subscription| subscription.name.clone())
    }
}

fn relation_oid_by_name(schema: &SchemaCatalog, name: &str) -> Option<ObjectId> {
    if let Some(table) = schema.get_created_table_by_name(name) {
        Some(table.id.as_object_id())
    } else if let Some(view) = schema.get_view_by_name(name) {
        Some(view.id.into())
    } else if let Some(source) = schema.get_source_by_name(name) {
        Some(source.id.into())
    } else if let Some(sink) = schema.get_created_sink_by_name(name) {
        Some(sink.id.into())
    } else if let Some(index) = schema.get_created_index_by_name(name) {
        Some(index.id.as_object_id())
    } else {
        schema
            .get_subscription_by_name(name)
            .map(|subscription| subscription.id.into())
    }
}
