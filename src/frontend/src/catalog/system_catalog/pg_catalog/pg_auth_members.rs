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

/// The catalog `pg_auth_members` shows the membership relations between roles. Any non-circular set of relationships is allowed.
/// Ref: [`https://www.postgresql.org/docs/current/catalog-pg-auth-members.html`]
#[system_catalog(view, "pg_catalog.pg_auth_members")]
#[derive(Fields)]
struct PgAuthMember {
    oid: i32,
    roleid: i32,
    member: i32,
    grantor: i32,
    admin_option: bool,
    inherit_option: bool,
    set_option: bool,
}
