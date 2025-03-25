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

use crate::catalog::OwnedByUserCatalog;
use crate::catalog::system_catalog::{SysCatalogReaderImpl, get_acl_items};
use crate::error::Result;

#[derive(Fields)]
struct RwDatabases {
    #[primary_key]
    id: i32,
    name: String,
    owner: i32,
    acl: Vec<String>,
    resource_group: String,
}

#[system_catalog(table, "rw_catalog.rw_databases")]
fn read(reader: &SysCatalogReaderImpl) -> Result<Vec<RwDatabases>> {
    let catalog_reader = reader.catalog_reader.read_guard();
    let user_reader = reader.user_info_reader.read_guard();
    let users = user_reader.get_all_users();
    let username_map = user_reader.get_user_name_map();

    Ok(catalog_reader
        .iter_databases()
        .map(|db| RwDatabases {
            id: db.id() as i32,
            name: db.name().into(),
            owner: db.owner() as i32,
            acl: get_acl_items(&Object::DatabaseId(db.id()), false, &users, username_map),
            resource_group: db.resource_group.clone(),
        })
        .collect())
}
