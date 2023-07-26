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
use risingwave_common::error::Result;
use risingwave_common::row::OwnedRow;
use risingwave_common::types::{DataType, ScalarImpl};

use crate::catalog::system_catalog::{SysCatalogReaderImpl, SystemCatalogColumnsDef};

/// The catalog `pg_user` provides access to information about database users.
/// Ref: [`https://www.postgresql.org/docs/current/view-pg-user.html`]
pub const PG_USER_TABLE_NAME: &str = "pg_user";
pub const PG_USER_ID_INDEX: usize = 0;
pub const PG_USER_NAME_INDEX: usize = 1;

pub const PG_USER_COLUMNS: &[SystemCatalogColumnsDef<'_>] = &[
    (DataType::Int32, "usesysid"),
    (DataType::Varchar, "name"),
    (DataType::Boolean, "usecreatedb"),
    (DataType::Boolean, "usesuper"),
    (DataType::Varchar, "passwd"),
];

impl SysCatalogReaderImpl {
    pub fn read_user_info(&self) -> Result<Vec<OwnedRow>> {
        let reader = self.user_info_reader.read_guard();
        let users = reader.get_all_users();
        Ok(users
            .iter()
            .map(|user| {
                OwnedRow::new(vec![
                    Some(ScalarImpl::Int32(user.id as i32)),
                    Some(ScalarImpl::Utf8(user.name.clone().into())),
                    Some(ScalarImpl::Bool(user.can_create_db)),
                    Some(ScalarImpl::Bool(user.is_super)),
                    // compatible with PG.
                    Some(ScalarImpl::Utf8("********".into())),
                ])
            })
            .collect_vec())
    }
}
