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
use crate::error::{ErrorCode, Result};
use crate::user::user_authentication::encrypted_raw_password;

/// `rw_user_secret` stores all user encrypted passwords in the database, which is only readable by
/// super users.
#[derive(Fields)]
struct RwUserSecret {
    #[primary_key]
    id: i32,
    password: Option<String>,
}

#[system_catalog(table, "rw_catalog.rw_user_secrets")]
fn read_rw_user_secrets_info(reader: &SysCatalogReaderImpl) -> Result<Vec<RwUserSecret>> {
    let user_info_reader = reader.user_info_reader.read_guard();
    // Since this catalog contains passwords, it must not be publicly readable.
    match user_info_reader.get_user_by_name(&reader.auth_context.user_name) {
        None => {
            return Err(ErrorCode::CatalogError(
                format!("user {} not found", reader.auth_context.user_name).into(),
            )
            .into());
        }
        Some(user) => {
            if !user.is_super {
                return Err(ErrorCode::PermissionDenied(
                    "permission denied for table \"rw_catalog.rw_user_secrets\"".to_owned(),
                )
                .into());
            }
        }
    }
    let users = user_info_reader.get_all_users();

    Ok(users
        .into_iter()
        .map(|user| RwUserSecret {
            id: user.id as i32,
            password: user.auth_info.as_ref().map(encrypted_raw_password),
        })
        .collect())
}
