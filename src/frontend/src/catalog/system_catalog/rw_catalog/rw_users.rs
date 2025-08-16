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

use crate::catalog::system_catalog::SysCatalogReaderImpl;
use crate::error::Result;

#[derive(Fields)]
struct RwUser {
    #[primary_key]
    id: i32,
    name: String,
    is_super: bool,
    create_db: bool,
    create_user: bool,
    can_login: bool,
    is_admin: bool,
}

#[system_catalog(table, "rw_catalog.rw_users")]
fn read_rw_user_info(reader: &SysCatalogReaderImpl) -> Result<Vec<RwUser>> {
    let reader = reader.user_info_reader.read_guard();
    let users = reader.get_all_users();

    Ok(users
        .into_iter()
        .map(|user| RwUser {
            id: user.id as i32,
            name: user.name,
            is_super: user.is_super,
            create_db: user.can_create_db,
            create_user: user.can_create_user,
            can_login: user.can_login,
            is_admin: user.is_admin,
        })
        .collect())
}
