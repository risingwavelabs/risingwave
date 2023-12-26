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

/// The catalog `pg_roles` provides access to information about database roles. This is simply a
/// publicly readable view of `pg_authid` that blanks out the password field.
/// Ref: [`https://www.postgresql.org/docs/current/view-pg-roles.html`]
pub static PG_ROLES: LazyLock<BuiltinView> = LazyLock::new(|| BuiltinView {
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
        (DataType::Boolean, "rolreplication"),
        (DataType::Int32, "rolconnlimit"),
        (DataType::Timestamptz, "rolvaliduntil"),
        (DataType::Boolean, "rolbypassrls"),
        (DataType::Varchar, "rolpassword"),
    ],
    sql: "SELECT id AS oid, \
                name AS rolname, \
                is_super AS rolsuper, \
                true AS rolinherit, \
                create_user AS rolcreaterole, \
                create_db AS rolcreatedb, \
                can_login AS rolcanlogin, \
                true AS rolreplication, \
                -1 AS rolconnlimit, \
                NULL::timestamptz AS rolvaliduntil, \
                true AS rolbypassrls, \
                '********' AS rolpassword \
            FROM rw_catalog.rw_users"
        .into(),
});
