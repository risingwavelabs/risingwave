// Copyright 2023 RisingWave Labs
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

use std::sync::LazyLock;

use itertools::Itertools;
use risingwave_common::catalog::RW_CATALOG_SCHEMA_NAME;
use risingwave_common::error::{ErrorCode, Result};
use risingwave_common::row::OwnedRow;
use risingwave_common::types::{DataType, ScalarImpl};

use crate::catalog::system_catalog::{BuiltinTable, SysCatalogReaderImpl};
use crate::user::user_authentication::encrypted_raw_password;

/// `rw_user_secret` stores all user encrypted passwords in the database, which is only readable by
/// super users.
pub static RW_USER_SECRETS: LazyLock<BuiltinTable> = LazyLock::new(|| BuiltinTable {
    name: "rw_user_secrets",
    schema: RW_CATALOG_SCHEMA_NAME,
    columns: &[(DataType::Int32, "id"), (DataType::Varchar, "password")],
    pk: &[0],
});

impl SysCatalogReaderImpl {
    pub fn read_rw_user_secrets_info(&self) -> Result<Vec<OwnedRow>> {
        let reader = self.user_info_reader.read_guard();
        // Since this catalog contains passwords, it must not be publicly readable.
        match reader.get_user_by_name(&self.auth_context.user_name) {
            None => {
                return Err(ErrorCode::CatalogError(
                    format!("user {} not found", self.auth_context.user_name).into(),
                )
                .into());
            }
            Some(user) => {
                if !user.is_super {
                    return Err(ErrorCode::PermissionDenied(
                        "permission denied for table rw_user_secrets".to_string(),
                    )
                    .into());
                }
            }
        }
        let users = reader.get_all_users();

        Ok(users
            .into_iter()
            .map(|user| {
                OwnedRow::new(vec![
                    Some(ScalarImpl::Int32(user.id as i32)),
                    user.auth_info
                        .as_ref()
                        .map(|info| ScalarImpl::Utf8(encrypted_raw_password(info).into())),
                ])
            })
            .collect_vec())
    }
}
