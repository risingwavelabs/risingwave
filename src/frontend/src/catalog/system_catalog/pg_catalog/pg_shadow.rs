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

use crate::catalog::system_catalog::{BuiltinView, SystemCatalogColumnsDef};

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
pub static PG_SHADOW: LazyLock<BuiltinView> = LazyLock::new(|| BuiltinView {
    name: "pg_shadow",
    schema: PG_CATALOG_SCHEMA_NAME,
    columns: &PG_SHADOW_COLUMNS,
    sql: "SELECT u.name AS usename,\
                 u.id AS usesysid, \
                 u.create_db AS usecreatedb, \
                 u.is_super AS usesuper, \
                 false AS userepl, \
                 false AS usebypassrls, \
                 s.password AS passwd, \
                 NULL::timestamptz AS valuntil, \
                 NULL::text[] AS useconfig \
            FROM rw_catalog.rw_users u \
            JOIN rw_catalog.rw_user_secrets s \
              ON u.id = s.id"
        .to_string(),
});
