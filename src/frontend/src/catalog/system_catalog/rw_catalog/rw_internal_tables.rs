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

use risingwave_common::types::{Fields, Timestamptz};
use risingwave_frontend_macro::system_catalog;
use risingwave_pb::user::grant_privilege::Object;

use crate::catalog::system_catalog::{get_acl_items, SysCatalogReaderImpl};
use crate::error::Result;

#[derive(Fields)]
struct RwInternalTable {
    #[primary_key]
    id: i32,
    name: String,
    schema_id: i32,
    job_id: i32,
    owner: i32,
    definition: String,
    acl: Vec<String>,
    initialized_at: Option<Timestamptz>,
    created_at: Option<Timestamptz>,
    initialized_at_cluster_version: Option<String>,
    created_at_cluster_version: Option<String>,
}

#[system_catalog(table, "rw_catalog.rw_internal_tables")]
fn read_rw_internal_tables(reader: &SysCatalogReaderImpl) -> Result<Vec<RwInternalTable>> {
    let catalog_reader = reader.catalog_reader.read_guard();
    let schemas = catalog_reader.iter_schemas(&reader.auth_context.database)?;
    let user_reader = reader.user_info_reader.read_guard();
    let users = user_reader.get_all_users();
    let username_map = user_reader.get_user_name_map();

    Ok(schemas
        .flat_map(|schema| {
            schema.iter_internal_table().map(|table| RwInternalTable {
                id: table.id.table_id as i32,
                name: table.name().into(),
                schema_id: schema.id() as i32,
                job_id: table.job_id.unwrap().table_id as i32,
                owner: table.owner as i32,
                definition: table.create_sql(),
                acl: get_acl_items(
                    &Object::TableId(table.id.table_id),
                    true,
                    &users,
                    username_map,
                ),
                initialized_at: table.initialized_at_epoch.map(|e| e.as_timestamptz()),
                created_at: table.created_at_epoch.map(|e| e.as_timestamptz()),
                initialized_at_cluster_version: table.initialized_at_cluster_version.clone(),
                created_at_cluster_version: table.created_at_cluster_version.clone(),
            })
        })
        .collect())
}
