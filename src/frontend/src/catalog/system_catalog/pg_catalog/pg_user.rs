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

use risingwave_common::catalog::PG_CATALOG_SCHEMA_NAME;
use risingwave_common::types::DataType;

use crate::catalog::system_catalog::BuiltinView;

/// The catalog `pg_user` provides access to information about database users.
/// Ref: [`https://www.postgresql.org/docs/current/view-pg-user.html`]
pub const PG_USER_TABLE_NAME: &str = "pg_user";
pub const PG_USER_ID_INDEX: usize = 0;
pub const PG_USER_NAME_INDEX: usize = 1;

pub static PG_USER: LazyLock<BuiltinView> = LazyLock::new(|| BuiltinView {
    name: PG_USER_TABLE_NAME,
    schema: PG_CATALOG_SCHEMA_NAME,
    columns: &[
        (DataType::Int32, "usesysid"),
        (DataType::Varchar, "name"),
        (DataType::Boolean, "usecreatedb"),
        (DataType::Boolean, "usesuper"),
        (DataType::Varchar, "passwd"),
    ],
    sql: "SELECT id AS usesysid, \
                name, \
                create_db AS usecreatedb, \
                is_super AS usesuper, \
                '********' AS passwd \
            FROM rw_catalog.rw_users"
        .into(),
});
