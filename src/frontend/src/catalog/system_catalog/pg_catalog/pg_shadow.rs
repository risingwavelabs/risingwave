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
/// The view `pg_shadow` exists for backwards compatibility: it emulates a catalog that existed in
/// PostgreSQL before version 8.1. It shows properties of all roles that are marked as rolcanlogin
/// in `pg_authid`. Ref: `https://www.postgresql.org/docs/current/view-pg-shadow.html`
#[system_catalog(
    view,
    "pg_catalog.pg_shadow",
    "SELECT u.name AS usename,
            u.id AS usesysid,
            u.create_db AS usecreatedb,
            u.is_super AS usesuper,
            false AS userepl,
            false AS usebypassrls,
            s.password AS passwd,
            NULL::timestamptz AS valuntil,
            NULL::text[] AS useconfig
        FROM rw_catalog.rw_users u
        JOIN rw_catalog.rw_user_secrets s
            ON u.id = s.id"
)]
#[derive(Fields)]
struct PgShadow {
    usename: String,
    usesysid: i32,
    usecreatedb: bool,
    usesuper: bool,
    // User can initiate streaming replication and put the system in and out of backup mode.
    userepl: bool,
    // User can bypass row level security.
    usebypassrls: bool,
    passwd: String,
    // Password expiry time (only used for password authentication)
    valuntil: Timestamptz,
    // Session defaults for run-time configuration variables
    useconfig: Vec<String>,
}
