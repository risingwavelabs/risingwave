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

use risingwave_common::catalog::CreateType;
use risingwave_common::types::{Fields, Timestamptz};
use risingwave_frontend_macro::system_catalog;
use risingwave_pb::user::grant_privilege::Object;

use crate::catalog::system_catalog::{get_acl_items, SysCatalogReaderImpl};
use crate::error::Result;
use crate::user::has_access_to_object;

#[derive(Fields)]
struct RwMaterializedView {
    #[primary_key]
    id: i32,
    name: String,
    schema_id: i32,
    owner: i32,
    definition: String,
    append_only: bool,
    acl: Vec<String>,
    initialized_at: Option<Timestamptz>,
    created_at: Option<Timestamptz>,
    initialized_at_cluster_version: Option<String>,
    created_at_cluster_version: Option<String>,
    background_ddl: bool,
}

#[system_catalog(table, "rw_catalog.rw_materialized_views")]
fn read_rw_materialized_views(reader: &SysCatalogReaderImpl) -> Result<Vec<RwMaterializedView>> {
    let user_reader = reader.user_info_reader.read_guard();
    let current_user = user_reader
        .get_user_by_name(&reader.auth_context.user_name)
        .expect("user not found");
    let users = user_reader.get_all_users();
    let username_map = user_reader.get_user_name_map();

    reader.read_all_mviews::<RwMaterializedView, _>(
        |schema, table| {
            has_access_to_object(current_user, &schema.name, table.id().table_id, table.owner)
        },
        |_, table| RwMaterializedView {
            id: table.id.table_id as i32,
            name: table.name().to_owned(),
            schema_id: table.schema_id as i32,
            owner: table.owner as i32,
            definition: table.create_sql(),
            append_only: table.append_only,
            acl: get_acl_items(
                &Object::TableId(table.id.table_id),
                false,
                &users,
                username_map,
            ),
            initialized_at: table.initialized_at_epoch.map(|e| e.as_timestamptz()),
            created_at: table.created_at_epoch.map(|e| e.as_timestamptz()),
            initialized_at_cluster_version: table.initialized_at_cluster_version.clone(),
            created_at_cluster_version: table.created_at_cluster_version.clone(),
            background_ddl: table.create_type == CreateType::Background,
        },
    )
}
