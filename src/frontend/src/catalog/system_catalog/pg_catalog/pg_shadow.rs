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
use risingwave_common::catalog::PG_CATALOG_SCHEMA_NAME;
use risingwave_common::error::{ErrorCode, Result};
use risingwave_common::row::OwnedRow;
use risingwave_common::types::{DataType, ScalarImpl};

use crate::catalog::system_catalog::{BuiltinTable, SysCatalogReaderImpl, SystemCatalogColumnsDef};
use crate::user::user_authentication::encrypted_raw_password;

pub static PG_SHADOW_COLUMNS: LazyLock<Vec<SystemCatalogColumnsDef<'_>>> = LazyLock::new(|| {
    vec![
        (DataType::Varchar, "usename"),
        (DataType::Int32, "usesysid"),
        (DataType::Boolean, "usecreatedb"),
        (DataType::Boolean, "usesuper"),
        // User can initiate streaming replication and put the system in and out of backup mode.
        (DataType::Boolean, "userepl"),
        // User can bypass row level security.
        (DataType::Boolean, "usebypassrls"),
        (DataType::Varchar, "passwd"),
        // Password expiry time (only used for password authentication)
        (DataType::Timestamptz, "valuntil"),
        // Session defaults for run-time configuration variables
        (DataType::List(Box::new(DataType::Varchar)), "useconfig"),
    ]
});

/// The view `pg_shadow` exists for backwards compatibility: it emulates a catalog that existed in
/// PostgreSQL before version 8.1. It shows properties of all roles that are marked as rolcanlogin
/// in `pg_authid`. Ref: [`https://www.postgresql.org/docs/current/view-pg-shadow.html`]
pub static PG_SHADOW: LazyLock<BuiltinTable> = LazyLock::new(|| BuiltinTable {
    name: "pg_shadow",
    schema: PG_CATALOG_SCHEMA_NAME,
    columns: &PG_SHADOW_COLUMNS,
    pk: &[1],
});

impl SysCatalogReaderImpl {
    pub fn read_user_info_shadow(&self) -> Result<Vec<OwnedRow>> {
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
                        "permission denied for table pg_shadow".to_string(),
                    )
                    .into());
                }
            }
        }

        let users = reader.get_all_users();
        Ok(users
            .iter()
            .map(|user| {
                OwnedRow::new(vec![
                    Some(ScalarImpl::Utf8(user.name.clone().into())),
                    Some(ScalarImpl::Int32(user.id as i32)),
                    Some(ScalarImpl::Bool(user.can_create_db)),
                    Some(ScalarImpl::Bool(user.is_super)),
                    Some(ScalarImpl::Bool(false)),
                    Some(ScalarImpl::Bool(false)),
                    user.auth_info
                        .as_ref()
                        .map(|info| ScalarImpl::Utf8(encrypted_raw_password(info).into())),
                    None,
                    None,
                ])
            })
            .collect_vec())
    }
}
