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

use risingwave_common::types::{Fields, Timestamptz};
use risingwave_frontend_macro::system_catalog;

/// The catalog `pg_roles` provides access to information about database roles. This is simply a
/// publicly readable view of `pg_authid` that blanks out the password field.
/// Ref: `https://www.postgresql.org/docs/current/view-pg-roles.html`
#[system_catalog(
    view,
    "pg_catalog.pg_roles",
    "SELECT id AS oid,
        name AS rolname,
        is_super AS rolsuper,
        true AS rolinherit,
        create_user AS rolcreaterole,
        create_db AS rolcreatedb,
        can_login AS rolcanlogin,
        true AS rolreplication,
        -1 AS rolconnlimit,
        NULL::timestamptz AS rolvaliduntil,
        true AS rolbypassrls,
        '********' AS rolpassword
    FROM rw_catalog.rw_users"
)]
#[derive(Fields)]
struct PgRule {
    oid: i32,
    rolname: String,
    rolsuper: bool,
    rolinherit: bool,
    rolcreaterole: bool,
    rolcreatedb: bool,
    rolcanlogin: bool,
    rolreplication: bool,
    rolconnlimit: i32,
    rolvaliduntil: Timestamptz,
    rolbypassrls: bool,
    rolpassword: String,
}
