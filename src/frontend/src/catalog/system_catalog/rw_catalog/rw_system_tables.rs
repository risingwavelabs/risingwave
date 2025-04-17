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

use risingwave_common::types::Fields;
use risingwave_frontend_macro::system_catalog;
use risingwave_pb::user::grant_privilege::Object;

use crate::catalog::system_catalog::{SysCatalogReaderImpl, get_acl_items};
use crate::error::Result;

/// `rw_system_tables` stores all system tables in the database.
#[derive(Fields)]
struct SystemTable {
    #[primary_key]
    id: i32,
    name: String,
    schema_id: i32,
    owner: i32,
    definition: Option<String>,
    acl: Vec<String>,
}

#[system_catalog(table, "rw_catalog.rw_system_tables")]
fn read_system_table_info(reader: &SysCatalogReaderImpl) -> Result<Vec<SystemTable>> {
    let catalog_reader = reader.catalog_reader.read_guard();
    let schemas = catalog_reader.iter_schemas(&reader.auth_context.database)?;
    let user_reader = reader.user_info_reader.read_guard();
    let users = user_reader.get_all_users();
    let username_map = user_reader.get_user_name_map();

    Ok(schemas
        .flat_map(|schema| {
            schema.iter_system_tables().map(|table| SystemTable {
                id: table.id.table_id as i32,
                name: table.name().to_owned(),
                schema_id: schema.id() as i32,
                owner: table.owner as i32,
                definition: None,
                acl: get_acl_items(
                    &Object::TableId(table.id.table_id),
                    false,
                    &users,
                    username_map,
                ),
            })
        })
        .collect())
}
