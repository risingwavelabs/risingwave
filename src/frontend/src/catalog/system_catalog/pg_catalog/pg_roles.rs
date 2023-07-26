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

use itertools::Itertools;
use risingwave_common::catalog::PG_CATALOG_SCHEMA_NAME;
use risingwave_common::error::Result;
use risingwave_common::row::OwnedRow;
use risingwave_common::types::{DataType, ScalarImpl};

use crate::catalog::system_catalog::{BuiltinTable, SysCatalogReaderImpl};

/// The catalog `pg_roles` provides access to information about database roles. This is simply a
/// publicly readable view of `pg_authid` that blanks out the password field.
/// Ref: [`https://www.postgresql.org/docs/current/view-pg-roles.html`]
pub const PG_ROLES: BuiltinTable = BuiltinTable {
    name: "pg_roles",
    schema: PG_CATALOG_SCHEMA_NAME,
    columns: &[
        (DataType::Int32, "oid"),
        (DataType::Varchar, "rolname"),
        (DataType::Boolean, "rolsuper"),
        (DataType::Boolean, "rolinherit"),
        (DataType::Boolean, "rolcreaterole"),
        (DataType::Boolean, "rolcreatedb"),
        (DataType::Boolean, "rolcanlogin"),
        (DataType::Varchar, "rolpassword"),
    ],
    pk: &[0],
};

impl SysCatalogReaderImpl {
    pub fn read_roles_info(&self) -> Result<Vec<OwnedRow>> {
        let reader = self.user_info_reader.read_guard();
        let users = reader.get_all_users();
        Ok(users
            .iter()
            .map(|user| {
                OwnedRow::new(vec![
                    Some(ScalarImpl::Int32(user.id as i32)),
                    Some(ScalarImpl::Utf8(user.name.clone().into())),
                    Some(ScalarImpl::Bool(user.is_super)),
                    Some(ScalarImpl::Bool(true)),
                    Some(ScalarImpl::Bool(user.can_create_user)),
                    Some(ScalarImpl::Bool(user.can_create_db)),
                    Some(ScalarImpl::Bool(user.can_login)),
                    Some(ScalarImpl::Utf8("********".into())),
                ])
            })
            .collect_vec())
    }
}
