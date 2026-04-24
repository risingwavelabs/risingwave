// Copyright 2026 RisingWave Labs
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

use std::collections::{HashSet, VecDeque};

use risingwave_common::acl::AclMode;
use risingwave_pb::user::RoleMembership;

use crate::session::AuthContext;
use crate::user::UserId;
use crate::user::user_catalog::UserCatalog;
use crate::user::user_service::{RoleMembershipInfoReader, UserInfoReader};

fn reachable_role_ids(
    start_ids: impl IntoIterator<Item = UserId>,
    memberships: &[RoleMembership],
    edge_allowed: impl Fn(&RoleMembership) -> bool,
) -> HashSet<UserId> {
    let mut visited = HashSet::new();
    let mut queue = VecDeque::new();
    for start_id in start_ids {
        queue.push_back(start_id);
    }

    while let Some(member_id) = queue.pop_front() {
        for membership in memberships.iter().filter(|membership| {
            membership.member_id == member_id.as_raw_id() && edge_allowed(membership)
        }) {
            let role_id = UserId::from(membership.role_id);
            if visited.insert(role_id) {
                queue.push_back(role_id);
            }
        }
    }

    visited
}

pub fn can_set_role(
    session_user_id: UserId,
    target_role_id: UserId,
    memberships: &[RoleMembership],
) -> bool {
    session_user_id == target_role_id
        || reachable_role_ids([session_user_id], memberships, |membership| {
            membership.set_option
        })
        .contains(&target_role_id)
}

pub fn effective_role_ids(
    user_info_reader: &UserInfoReader,
    auth_context: &AuthContext,
    memberships: &[RoleMembership],
) -> HashSet<UserId> {
    let current_user_id = auth_context.current_user_id();
    let mut ids = HashSet::from([current_user_id]);
    let reader = user_info_reader.read_guard();
    let Some(current_user) = reader.get_user_by_id(&current_user_id) else {
        return ids;
    };
    if !current_user.can_inherit {
        return ids;
    }
    ids.extend(reachable_role_ids(
        [current_user_id],
        memberships,
        |membership| membership.inherit_option,
    ));
    ids
}

pub fn session_has_privilege(
    user_info_reader: &UserInfoReader,
    auth_context: &AuthContext,
    memberships: &[RoleMembership],
    owner: UserId,
    object: impl Copy + Into<risingwave_pb::user::grant_privilege::Object>,
    mode: AclMode,
) -> bool {
    let reader = user_info_reader.read_guard();
    drop(reader);
    for role_id in effective_role_ids(user_info_reader, auth_context, memberships) {
        let reader = user_info_reader.read_guard();
        let Some(user) = reader.get_user_by_id(&role_id) else {
            continue;
        };
        if has_privilege_for_catalog_user(user, owner, object, mode) {
            return true;
        }
    }
    false
}

pub fn principal_has_privilege(
    user_info_reader: &UserInfoReader,
    memberships: &[RoleMembership],
    principal: &UserCatalog,
    owner: UserId,
    object: impl Copy + Into<risingwave_pb::user::grant_privilege::Object>,
    mode: AclMode,
) -> bool {
    let reader = user_info_reader.read_guard();
    if has_privilege_for_catalog_user(principal, owner, object, mode) {
        return true;
    }
    if !principal.can_inherit {
        return false;
    }

    for role_id in reachable_role_ids([principal.id], memberships, |membership| {
        membership.inherit_option
    }) {
        let Some(role) = reader.get_user_by_id(&role_id) else {
            continue;
        };
        if has_privilege_for_catalog_user(role, owner, object, mode) {
            return true;
        }
    }

    false
}

fn has_privilege_for_catalog_user(
    user: &UserCatalog,
    owner: UserId,
    object: impl Copy + Into<risingwave_pb::user::grant_privilege::Object>,
    mode: AclMode,
) -> bool {
    user.is_super || user.id == owner || user.has_privilege(object, mode)
}

pub fn role_memberships_snapshot(
    role_membership_reader: &RoleMembershipInfoReader,
) -> Vec<RoleMembership> {
    role_membership_reader.read_guard().clone()
}
