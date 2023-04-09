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

use risingwave_common::types::DataType;

use crate::catalog::system_catalog::SystemCatalogColumnsDef;

/// The catalog `pg_roles` provides access to information about database roles. This is simply a
/// publicly readable view of `pg_authid` that blanks out the password field.
/// Ref: [`https://www.postgresql.org/docs/current/view-pg-roles.html`]
pub const PG_ROLES_TABLE_NAME: &str = "pg_roles";
pub const PG_ROLES_COLUMNS: &[SystemCatalogColumnsDef<'_>] = &[
    (DataType::Int32, "oid"),
    (DataType::Varchar, "rolname"),
    (DataType::Boolean, "rolsuper"),
    (DataType::Boolean, "rolinherit"),
    (DataType::Boolean, "rolcreaterole"),
    (DataType::Boolean, "rolcreatedb"),
    (DataType::Boolean, "rolcanlogin"),
    (DataType::Varchar, "rolpassword"),
];
