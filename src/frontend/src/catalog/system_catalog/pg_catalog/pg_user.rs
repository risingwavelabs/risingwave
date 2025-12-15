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

use risingwave_common::types::Fields;
use risingwave_frontend_macro::system_catalog;

/// The catalog `pg_user` provides access to information about database users.
/// Ref: `https://www.postgresql.org/docs/current/view-pg-user.html`
#[system_catalog(
    view,
    "pg_catalog.pg_user",
    "SELECT id AS usesysid,
        name,
        create_db AS usecreatedb,
        is_super AS usesuper,
        '********' AS passwd
    FROM rw_catalog.rw_users"
)]
#[derive(Fields)]
struct PgUser {
    usesysid: i32,
    usename: String,
    usecreatedb: bool,
    usesuper: bool,
    passwd: String,
}
