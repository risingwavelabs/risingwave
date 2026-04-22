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

use risingwave_common::types::Fields;
use risingwave_frontend_macro::system_catalog;

use crate::catalog::system_catalog::SysCatalogReaderImpl;
use crate::error::Result;

/// The catalog `pg_auth_members` shows the membership relations between roles. Any non-circular
/// set of relationships is allowed.
/// Ref: `https://www.postgresql.org/docs/current/catalog-pg-auth-members.html`
#[derive(Fields)]
struct PgAuthMember {
    #[primary_key]
    oid: i32,
    roleid: i32,
    member: i32,
    grantor: i32,
    admin_option: bool,
    inherit_option: bool,
    set_option: bool,
}

#[system_catalog(table, "pg_catalog.pg_auth_members")]
async fn read_pg_auth_members(reader: &SysCatalogReaderImpl) -> Result<Vec<PgAuthMember>> {
    let mut memberships = reader.meta_client.list_role_memberships(vec![]).await?;
    memberships.sort_by_key(|membership| {
        (
            membership.role_id,
            membership.member_id,
            membership.granted_by,
        )
    });

    Ok(memberships
        .into_iter()
        .enumerate()
        .map(|(idx, membership)| PgAuthMember {
            oid: (idx + 1) as i32,
            roleid: membership.role_id as i32,
            member: membership.member_id as i32,
            grantor: membership.granted_by as i32,
            admin_option: membership.admin_option,
            inherit_option: membership.inherit_option,
            set_option: membership.set_option,
        })
        .collect())
}
