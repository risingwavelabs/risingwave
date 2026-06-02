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

use std::collections::{HashMap, HashSet, VecDeque};

use itertools::Itertools;
use risingwave_common::catalog::{
    DEFAULT_SUPER_USER, DEFAULT_SUPER_USER_FOR_ADMIN, DEFAULT_SUPER_USER_FOR_PG,
    DEFAULT_SUPER_USER_ID,
};
use risingwave_meta_model::object::ObjectType;
use risingwave_meta_model::prelude::{
    Object, User, UserDefaultPrivilege, UserPrivilege, UserRoleMembership,
};
use risingwave_meta_model::user_privilege::Action;
use risingwave_meta_model::{
    AuthInfo, DatabaseId, DefaultPrivilegeId, PrivilegeId, SchemaId, UserId, object, user,
    user_default_privilege, user_privilege, user_role_membership,
};
use risingwave_pb::common::PbObjectType;
use risingwave_pb::meta::subscribe_response::{
    Info as NotificationInfo, Operation as NotificationOperation,
};
use risingwave_pb::user::update_user_request::PbUpdateField;
use risingwave_pb::user::{PbAction, PbGrantPrivilege, PbRoleMembership, PbUserInfo};
use sea_orm::ActiveValue::Set;
use sea_orm::sea_query::{OnConflict, SimpleExpr, Value};
use sea_orm::{
    ActiveModelTrait, ColumnTrait, Condition, DatabaseConnection, EntityTrait, IntoActiveModel,
    PaginatorTrait, QueryFilter, QuerySelect, TransactionTrait,
};

use crate::controller::catalog::CatalogController;
use crate::controller::utils::{
    PartialUserPrivilege, check_user_name_duplicate, ensure_object_id,
    ensure_privileges_not_referred, ensure_user_id, extract_grant_obj_id,
    get_iceberg_related_object_ids, get_index_state_tables_by_table_id, get_internal_tables_by_id,
    get_object_owner, get_referring_privileges_cascade, get_user_privilege, list_user_info_by_ids,
};
use crate::manager::{IGNORED_NOTIFICATION_VERSION, NotificationVersion};
use crate::{MetaError, MetaResult};

#[derive(Clone, Copy)]
enum RoleMembershipGraphKind {
    All,
    Admin,
    Inherit,
}

type RoleMembershipTarget = (UserId, UserId, UserId);

fn build_role_membership_graph(
    memberships: &[user_role_membership::Model],
    kind: RoleMembershipGraphKind,
) -> HashMap<UserId, Vec<UserId>> {
    let mut graph: HashMap<UserId, Vec<UserId>> = HashMap::new();
    for membership in memberships {
        let include = match kind {
            RoleMembershipGraphKind::All => true,
            RoleMembershipGraphKind::Admin => membership.admin_option,
            RoleMembershipGraphKind::Inherit => membership.inherit_option,
        };
        if !include {
            continue;
        }
        graph
            .entry(membership.member_id)
            .or_default()
            .push(membership.role_id);
    }
    for next in graph.values_mut() {
        next.sort_by_key(|id| id.as_raw_id());
    }
    graph
}

fn has_role_membership_path(
    graph: &HashMap<UserId, Vec<UserId>>,
    start: UserId,
    target: UserId,
) -> bool {
    let mut stack = vec![start];
    let mut visited = HashSet::new();
    while let Some(node) = stack.pop() {
        if !visited.insert(node) {
            continue;
        }
        if node == target {
            return true;
        }
        if let Some(next) = graph.get(&node) {
            stack.extend(next.iter().copied());
        }
    }
    false
}

fn has_role_membership_path_strict(
    graph: &HashMap<UserId, Vec<UserId>>,
    start: UserId,
    target: UserId,
) -> bool {
    start != target && has_role_membership_path(graph, start, target)
}

fn role_membership_dependency_is_valid(
    admin_graph: &HashMap<UserId, Vec<UserId>>,
    superuser_ids: &HashSet<UserId>,
    grantor: UserId,
    role_id: UserId,
) -> bool {
    grantor == DEFAULT_SUPER_USER_ID
        || superuser_ids.contains(&grantor)
        || has_role_membership_path_strict(admin_graph, grantor, role_id)
}

fn grantor_has_admin_option_for_grant(
    admin_graph: &HashMap<UserId, Vec<UserId>>,
    grantor: UserId,
    role_id: UserId,
    executor_is_super: bool,
    grantor_is_super: bool,
) -> bool {
    (executor_is_super && grantor_is_super)
        || has_role_membership_path_strict(admin_graph, grantor, role_id)
}

fn role_possesses_grantor(
    inherit_graph: &HashMap<UserId, Vec<UserId>>,
    executor: UserId,
    grantor: UserId,
) -> bool {
    executor == grantor || has_role_membership_path(inherit_graph, executor, grantor)
}

fn closest_reachable_role_ids(graph: &HashMap<UserId, Vec<UserId>>, start: UserId) -> Vec<UserId> {
    let mut queue = VecDeque::new();
    let mut visited = HashSet::from([start]);
    let mut reachable = vec![];

    if let Some(next) = graph.get(&start) {
        queue.extend(next.iter().copied());
    }

    while let Some(node) = queue.pop_front() {
        if !visited.insert(node) {
            continue;
        }
        reachable.push(node);
        if let Some(next) = graph.get(&node) {
            queue.extend(next.iter().copied());
        }
    }

    reachable
}

fn matching_role_membership_exists(
    memberships: &[user_role_membership::Model],
    role_id: UserId,
    member_id: UserId,
    granted_by: UserId,
) -> bool {
    memberships.iter().any(|membership| {
        membership.granted_by == granted_by
            && membership.role_id == role_id
            && membership.member_id == member_id
    })
}

fn role_membership_target(membership: &user_role_membership::Model) -> RoleMembershipTarget {
    (
        membership.role_id,
        membership.member_id,
        membership.granted_by,
    )
}

fn role_membership_targets_for_grantor(
    role_ids: &[UserId],
    member_ids: &[UserId],
    granted_by: UserId,
) -> HashSet<RoleMembershipTarget> {
    role_ids
        .iter()
        .copied()
        .cartesian_product(member_ids.iter().copied())
        .map(|(role_id, member_id)| (role_id, member_id, granted_by))
        .collect()
}

fn select_effective_revoke_grantors_for_membership(
    memberships: &[user_role_membership::Model],
    admin_graph: &HashMap<UserId, Vec<UserId>>,
    possession_graph: &HashMap<UserId, Vec<UserId>>,
    role_id: UserId,
    member_id: UserId,
    revoked_by: UserId,
) -> Vec<UserId> {
    let mut grantors = vec![];
    if has_role_membership_path_strict(admin_graph, revoked_by, role_id)
        && matching_role_membership_exists(memberships, role_id, member_id, revoked_by)
    {
        grantors.push(revoked_by);
    }

    grantors.extend(
        closest_reachable_role_ids(possession_graph, revoked_by)
            .into_iter()
            .filter(|&candidate| {
                has_role_membership_path_strict(admin_graph, candidate, role_id)
                    && matching_role_membership_exists(memberships, role_id, member_id, candidate)
            }),
    );
    grantors
}

fn select_effective_grantor_for_role(
    admin_graph: &HashMap<UserId, Vec<UserId>>,
    possession_graph: &HashMap<UserId, Vec<UserId>>,
    role_id: UserId,
    executed_by: UserId,
) -> Option<UserId> {
    if has_role_membership_path_strict(admin_graph, executed_by, role_id) {
        return Some(executed_by);
    }

    closest_reachable_role_ids(possession_graph, executed_by)
        .into_iter()
        .find(|&candidate| has_role_membership_path_strict(admin_graph, candidate, role_id))
}

fn apply_revoke_to_memberships(
    memberships: Vec<user_role_membership::Model>,
    targets: &HashSet<RoleMembershipTarget>,
    revoke_admin_option: bool,
    revoke_inherit_option: bool,
    revoke_set_option: bool,
) -> Vec<user_role_membership::Model> {
    let option_only = revoke_admin_option || revoke_inherit_option || revoke_set_option;
    memberships
        .into_iter()
        .filter_map(|mut membership| {
            let targeted = targets.contains(&role_membership_target(&membership));
            if !targeted {
                return Some(membership);
            }

            if option_only {
                if revoke_admin_option {
                    membership.admin_option = false;
                }
                if revoke_inherit_option {
                    membership.inherit_option = false;
                }
                if revoke_set_option {
                    membership.set_option = false;
                }
                Some(membership)
            } else {
                None
            }
        })
        .collect()
}

fn compute_cascading_role_memberships(
    memberships: &[user_role_membership::Model],
    superuser_ids: &HashSet<UserId>,
    targets: &HashSet<RoleMembershipTarget>,
    revoke_admin_option: bool,
    revoke_inherit_option: bool,
    revoke_set_option: bool,
    cascade: bool,
) -> MetaResult<Vec<user_role_membership::Model>> {
    let option_only = revoke_admin_option || revoke_inherit_option || revoke_set_option;
    let before_admin = build_role_membership_graph(memberships, RoleMembershipGraphKind::Admin);
    let mut current = apply_revoke_to_memberships(
        memberships.to_vec(),
        targets,
        revoke_admin_option,
        revoke_inherit_option,
        revoke_set_option,
    );

    loop {
        let after_admin = build_role_membership_graph(&current, RoleMembershipGraphKind::Admin);
        let mut dependent_ids = vec![];

        for membership in &current {
            let lost_admin_path = role_membership_dependency_is_valid(
                &before_admin,
                superuser_ids,
                membership.granted_by,
                membership.role_id,
            ) && !role_membership_dependency_is_valid(
                &after_admin,
                superuser_ids,
                membership.granted_by,
                membership.role_id,
            );
            if lost_admin_path {
                dependent_ids.push(membership.id);
            }
        }

        if dependent_ids.is_empty() {
            return Ok(current);
        }
        if !cascade {
            return Err(MetaError::permission_denied(
                "dependent role memberships exist; use CASCADE to revoke them".to_owned(),
            ));
        }

        let dependent_set: HashSet<_> = dependent_ids.into_iter().collect();
        current.retain(|membership| !dependent_set.contains(&membership.id));

        // Revoking INHERIT OPTION or SET OPTION alone does not invalidate
        // grantor dependencies; only ADMIN OPTION revocation can cascade.
        if option_only && !revoke_admin_option {
            return Ok(current);
        }
    }
}

impl CatalogController {
    async fn user_controller_db(&self) -> DatabaseConnection {
        self.inner.read().await.db.clone()
    }

    pub(crate) async fn notify_users_update(
        &self,
        user_infos: Vec<PbUserInfo>,
    ) -> NotificationVersion {
        let mut version = 0;
        for info in user_infos {
            version = self
                .notify_frontend(NotificationOperation::Update, NotificationInfo::User(info))
                .await;
        }
        version
    }

    pub async fn create_user(&self, pb_user: PbUserInfo) -> MetaResult<NotificationVersion> {
        let db = self.user_controller_db().await;
        let txn = db.begin().await?;
        check_user_name_duplicate(&pb_user.name, &txn).await?;

        let grant_privileges = pb_user.grant_privileges.clone();
        let user: user::ActiveModel = pb_user.into();
        let user = user.insert(&txn).await?;

        if !grant_privileges.is_empty() {
            let mut privileges = vec![];
            for gp in &grant_privileges {
                let id = extract_grant_obj_id(gp.get_object()?);
                for action_with_opt in &gp.action_with_opts {
                    privileges.push(user_privilege::ActiveModel {
                        user_id: Set(user.user_id),
                        oid: Set(id),
                        granted_by: Set(action_with_opt.granted_by),
                        action: Set(action_with_opt.get_action()?.into()),
                        with_grant_option: Set(action_with_opt.with_grant_option),
                        ..Default::default()
                    });
                }
            }
            UserPrivilege::insert_many(privileges).exec(&txn).await?;
        }
        txn.commit().await?;

        let mut user_info: PbUserInfo = user.into();
        user_info.grant_privileges = grant_privileges;
        let version = self
            .notify_frontend(
                NotificationOperation::Add,
                NotificationInfo::User(user_info),
            )
            .await;

        Ok(version)
    }

    pub async fn update_user(
        &self,
        update_user: PbUserInfo,
        update_fields: &[PbUpdateField],
    ) -> MetaResult<NotificationVersion> {
        let db = self.user_controller_db().await;
        let rename_flag = update_fields.contains(&PbUpdateField::Rename);
        if rename_flag {
            check_user_name_duplicate(&update_user.name, &db).await?;
        }

        let user = User::find_by_id(update_user.id)
            .one(&db)
            .await?
            .ok_or_else(|| MetaError::catalog_id_not_found("user", update_user.id))?;
        let mut user = user.into_active_model();
        update_fields.iter().for_each(|&field| match field {
            PbUpdateField::Unspecified => unreachable!(),
            PbUpdateField::Super => user.is_super = Set(update_user.is_super),
            PbUpdateField::Login => user.can_login = Set(update_user.can_login),
            PbUpdateField::CreateDb => user.can_create_db = Set(update_user.can_create_db),
            PbUpdateField::CreateUser => user.can_create_user = Set(update_user.can_create_user),
            PbUpdateField::AuthInfo => {
                user.auth_info = Set(update_user.auth_info.as_ref().map(AuthInfo::from))
            }
            PbUpdateField::Rename => user.name = Set(update_user.name.clone()),
            PbUpdateField::Admin => user.is_admin = Set(update_user.is_admin),
            PbUpdateField::Inherit => {
                if let Some(can_inherit) = update_user.can_inherit {
                    user.can_inherit = Set(can_inherit);
                }
            }
        });

        let user = user.update(&db).await?;
        let mut user_info: PbUserInfo = user.into();
        user_info.grant_privileges = get_user_privilege(user_info.id, &db).await?;
        let version = self
            .notify_frontend(
                NotificationOperation::Update,
                NotificationInfo::User(user_info),
            )
            .await;

        Ok(version)
    }

    pub async fn list_role_memberships(
        &self,
        member_ids: &[UserId],
        include_all: bool,
    ) -> MetaResult<Vec<PbRoleMembership>> {
        debug_assert!(
            !include_all || member_ids.is_empty(),
            "member_ids must be empty when include_all is true"
        );
        let db = self.user_controller_db().await;
        let memberships = if include_all {
            UserRoleMembership::find().all(&db).await?
        } else if member_ids.is_empty() {
            vec![]
        } else {
            UserRoleMembership::find()
                .filter(user_role_membership::Column::MemberId.is_in(member_ids.iter().copied()))
                .all(&db)
                .await?
        };
        Ok(memberships.into_iter().map(Into::into).collect())
    }

    pub async fn grant_role(
        &self,
        role_ids: Vec<UserId>,
        member_ids: Vec<UserId>,
        granted_by: UserId,
        executed_by: UserId,
        granted_by_specified: bool,
        admin_option: Option<bool>,
        inherit_option: Option<bool>,
        set_option: Option<bool>,
    ) -> MetaResult<(NotificationVersion, Vec<PbRoleMembership>)> {
        if role_ids.is_empty() || member_ids.is_empty() {
            return Ok((IGNORED_NOTIFICATION_VERSION, vec![]));
        }

        let inner = self.inner.write().await;
        let txn = inner.db.begin().await?;
        for role_id in &role_ids {
            ensure_user_id(*role_id, &txn).await?;
        }
        for member_id in &member_ids {
            ensure_user_id(*member_id, &txn).await?;
        }
        let executor = User::find_by_id(executed_by)
            .one(&txn)
            .await?
            .ok_or_else(|| MetaError::catalog_id_not_found("user", executed_by))?;
        let existing_memberships = UserRoleMembership::find().all(&txn).await?;
        let admin_graph =
            build_role_membership_graph(&existing_memberships, RoleMembershipGraphKind::Admin);
        let possession_graph =
            build_role_membership_graph(&existing_memberships, RoleMembershipGraphKind::Inherit);
        let effective_granted_by: HashMap<_, _> = if !granted_by_specified && executor.is_super {
            role_ids
                .iter()
                .copied()
                .map(|role_id| (role_id, DEFAULT_SUPER_USER_ID))
                .collect()
        } else if granted_by_specified {
            let grantor = User::find_by_id(granted_by)
                .one(&txn)
                .await?
                .ok_or_else(|| MetaError::catalog_id_not_found("user", granted_by))?;
            if !executor.is_super
                && !role_possesses_grantor(&possession_graph, executed_by, granted_by)
            {
                return Err(MetaError::permission_denied(format!(
                    "user {} does not possess grantor role {}",
                    executed_by, granted_by
                )));
            }
            if role_ids.iter().any(|&role_id| {
                !grantor_has_admin_option_for_grant(
                    &admin_graph,
                    granted_by,
                    role_id,
                    executor.is_super,
                    grantor.is_super,
                )
            }) {
                return Err(MetaError::permission_denied(format!(
                    "user {} does not have ADMIN OPTION for all requested roles",
                    granted_by
                )));
            }
            role_ids
                .iter()
                .copied()
                .map(|role_id| (role_id, granted_by))
                .collect()
        } else {
            let mut grantors = HashMap::new();
            for &role_id in &role_ids {
                let effective_grantor = select_effective_grantor_for_role(
                    &admin_graph,
                    &possession_graph,
                    role_id,
                    executed_by,
                )
                .ok_or_else(|| {
                    MetaError::permission_denied(format!(
                        "user {} does not have ADMIN OPTION for all requested roles",
                        executed_by
                    ))
                })?;
                grantors.insert(role_id, effective_grantor);
            }
            grantors
        };

        let mut graph =
            build_role_membership_graph(&existing_memberships, RoleMembershipGraphKind::All);
        for &role_id in &role_ids {
            for &member_id in &member_ids {
                if role_id == member_id {
                    return Err(MetaError::permission_denied(format!(
                        "cannot grant role {} to itself",
                        role_id
                    )));
                }
                if has_role_membership_path(&graph, role_id, member_id) {
                    return Err(MetaError::permission_denied(format!(
                        "granting role {} to {} would create a circular membership",
                        role_id, member_id
                    )));
                }
                graph.entry(member_id).or_default().push(role_id);
            }
        }

        for &role_id in &role_ids {
            let effective_granted_by = effective_granted_by
                .get(&role_id)
                .copied()
                .expect("effective grantor must be selected for each role");
            for &member_id in &member_ids {
                let member = User::find_by_id(member_id)
                    .one(&txn)
                    .await?
                    .ok_or_else(|| MetaError::catalog_id_not_found("user", member_id))?;
                let existing = UserRoleMembership::find()
                    .filter(user_role_membership::Column::RoleId.eq(role_id))
                    .filter(user_role_membership::Column::MemberId.eq(member_id))
                    .filter(user_role_membership::Column::GrantedBy.eq(effective_granted_by))
                    .one(&txn)
                    .await?;
                if let Some(existing) = existing {
                    let current_admin_option = existing.admin_option;
                    let current_inherit_option = existing.inherit_option;
                    let current_set_option = existing.set_option;
                    let mut model = existing.into_active_model();
                    model.admin_option = Set(admin_option.unwrap_or(current_admin_option));
                    model.inherit_option = Set(inherit_option.unwrap_or(current_inherit_option));
                    model.set_option = Set(set_option.unwrap_or(current_set_option));
                    model.update(&txn).await?;
                } else {
                    user_role_membership::ActiveModel {
                        role_id: Set(role_id),
                        member_id: Set(member_id),
                        granted_by: Set(effective_granted_by),
                        admin_option: Set(admin_option.unwrap_or(false)),
                        inherit_option: Set(inherit_option.unwrap_or(member.can_inherit)),
                        set_option: Set(set_option.unwrap_or(true)),
                        ..Default::default()
                    }
                    .insert(&txn)
                    .await?;
                }
            }
        }

        let memberships = UserRoleMembership::find()
            .filter(user_role_membership::Column::RoleId.is_in(role_ids.iter().copied()))
            .filter(user_role_membership::Column::MemberId.is_in(member_ids.iter().copied()))
            .all(&txn)
            .await?
            .into_iter()
            .filter(|membership| {
                effective_granted_by
                    .get(&membership.role_id)
                    .is_some_and(|&grantor| grantor == membership.granted_by)
            })
            .map(Into::into)
            .collect();
        let affected_user_infos = list_user_info_by_ids(
            role_ids
                .iter()
                .chain(member_ids.iter())
                .copied()
                .unique()
                .collect_vec(),
            &txn,
        )
        .await?;
        txn.commit().await?;
        drop(inner);

        let version = self.notify_users_update(affected_user_infos).await;
        Ok((version, memberships))
    }

    pub async fn revoke_role(
        &self,
        role_ids: Vec<UserId>,
        member_ids: Vec<UserId>,
        granted_by: UserId,
        granted_by_specified: bool,
        revoked_by: UserId,
        revoke_admin_option: bool,
        revoke_inherit_option: bool,
        revoke_set_option: bool,
        cascade: bool,
    ) -> MetaResult<(NotificationVersion, Vec<PbRoleMembership>)> {
        if role_ids.is_empty() || member_ids.is_empty() {
            return Ok((IGNORED_NOTIFICATION_VERSION, vec![]));
        }

        let inner = self.inner.write().await;
        let txn = inner.db.begin().await?;
        for role_id in &role_ids {
            ensure_user_id(*role_id, &txn).await?;
        }
        for member_id in &member_ids {
            ensure_user_id(*member_id, &txn).await?;
        }
        let revoker = User::find_by_id(revoked_by)
            .one(&txn)
            .await?
            .ok_or_else(|| MetaError::catalog_id_not_found("user", revoked_by))?;

        let existing_memberships = UserRoleMembership::find().all(&txn).await?;
        let superuser_ids: HashSet<UserId> = User::find()
            .select_only()
            .column(user::Column::UserId)
            .filter(user::Column::IsSuper.eq(true))
            .into_tuple()
            .all(&txn)
            .await?
            .into_iter()
            .collect();
        let admin_graph =
            build_role_membership_graph(&existing_memberships, RoleMembershipGraphKind::Admin);
        let possession_graph =
            build_role_membership_graph(&existing_memberships, RoleMembershipGraphKind::Inherit);
        let targets: HashSet<_> = if granted_by_specified {
            ensure_user_id(granted_by, &txn).await?;
            if !revoker.is_super
                && !role_possesses_grantor(&possession_graph, revoked_by, granted_by)
            {
                return Err(MetaError::permission_denied(format!(
                    "user {} does not possess grantor role {}",
                    revoked_by, granted_by
                )));
            }
            role_membership_targets_for_grantor(&role_ids, &member_ids, granted_by)
        } else if revoker.is_super {
            let targets: HashSet<_> = existing_memberships
                .iter()
                .filter(|membership| {
                    role_ids.contains(&membership.role_id)
                        && member_ids.contains(&membership.member_id)
                })
                .map(role_membership_target)
                .collect();
            if targets.is_empty() {
                return Ok((IGNORED_NOTIFICATION_VERSION, vec![]));
            }
            targets
        } else {
            let targets: HashSet<_> = role_ids
                .iter()
                .copied()
                .cartesian_product(member_ids.iter().copied())
                .flat_map(|(role_id, member_id)| {
                    select_effective_revoke_grantors_for_membership(
                        &existing_memberships,
                        &admin_graph,
                        &possession_graph,
                        role_id,
                        member_id,
                        revoked_by,
                    )
                    .into_iter()
                    .map(move |granted_by| (role_id, member_id, granted_by))
                })
                .collect();
            if targets.is_empty() {
                return Ok((IGNORED_NOTIFICATION_VERSION, vec![]));
            }
            targets
        };
        let final_memberships = compute_cascading_role_memberships(
            &existing_memberships,
            &superuser_ids,
            &targets,
            revoke_admin_option,
            revoke_inherit_option,
            revoke_set_option,
            cascade,
        )?;
        let final_by_id: HashMap<_, _> = final_memberships
            .into_iter()
            .map(|membership| (membership.id, membership))
            .collect();
        for membership in existing_memberships {
            if let Some(updated) = final_by_id.get(&membership.id) {
                if &membership != updated {
                    let mut model = membership.into_active_model();
                    model.admin_option = Set(updated.admin_option);
                    model.inherit_option = Set(updated.inherit_option);
                    model.set_option = Set(updated.set_option);
                    model.update(&txn).await?;
                }
            } else {
                UserRoleMembership::delete_by_id(membership.id)
                    .exec(&txn)
                    .await?;
            }
        }

        let memberships = UserRoleMembership::find()
            .filter(user_role_membership::Column::RoleId.is_in(role_ids.iter().copied()))
            .filter(user_role_membership::Column::MemberId.is_in(member_ids.iter().copied()))
            .all(&txn)
            .await?
            .into_iter()
            .filter(|membership| targets.contains(&role_membership_target(membership)))
            .map(Into::into)
            .collect();
        let affected_user_infos = list_user_info_by_ids(
            role_ids
                .iter()
                .chain(member_ids.iter())
                .copied()
                .unique()
                .collect_vec(),
            &txn,
        )
        .await?;
        txn.commit().await?;
        drop(inner);

        let version = self.notify_users_update(affected_user_infos).await;
        Ok((version, memberships))
    }

    #[cfg(test)]
    pub async fn get_user(&self, id: UserId) -> MetaResult<user::Model> {
        let db = self.user_controller_db().await;
        let user = User::find_by_id(id)
            .one(&db)
            .await?
            .ok_or_else(|| MetaError::catalog_id_not_found("user", id))?;
        Ok(user)
    }

    #[cfg(test)]
    pub async fn get_user_by_name(&self, name: &str) -> MetaResult<user::Model> {
        let db = self.user_controller_db().await;
        let user = User::find()
            .filter(user::Column::Name.eq(name))
            .one(&db)
            .await?
            .ok_or_else(|| anyhow::anyhow!("user {name} not found"))?;
        Ok(user)
    }

    pub async fn drop_user(
        &self,
        user_id: UserId,
        dropped_by: UserId,
        session_user: UserId,
    ) -> MetaResult<NotificationVersion> {
        let inner = self.inner.write().await;
        let txn = inner.db.begin().await?;
        let user = User::find_by_id(user_id)
            .one(&txn)
            .await?
            .ok_or_else(|| MetaError::catalog_id_not_found("user", user_id))?;
        let actor = User::find_by_id(dropped_by)
            .one(&txn)
            .await?
            .ok_or_else(|| MetaError::catalog_id_not_found("user", dropped_by))?;
        if dropped_by == user_id || session_user == user_id {
            return Err(MetaError::permission_denied(
                "current user cannot be dropped".to_owned(),
            ));
        }
        if user.name == DEFAULT_SUPER_USER
            || user.name == DEFAULT_SUPER_USER_FOR_PG
            || user.name == DEFAULT_SUPER_USER_FOR_ADMIN
        {
            return Err(MetaError::permission_denied(format!(
                "drop default super user {} is not allowed",
                user.name
            )));
        }
        if !actor.is_super {
            if user.is_super {
                return Err(MetaError::permission_denied(
                    "must be superuser to drop superusers".to_owned(),
                ));
            }
            if !actor.can_create_user {
                return Err(MetaError::permission_denied(
                    "permission denied to drop user".to_owned(),
                ));
            }
            let existing_memberships = UserRoleMembership::find().all(&txn).await?;
            let admin_graph =
                build_role_membership_graph(&existing_memberships, RoleMembershipGraphKind::Admin);
            if !has_role_membership_path(&admin_graph, dropped_by, user_id) {
                return Err(MetaError::permission_denied(format!(
                    "user {} does not have ADMIN OPTION for role {}",
                    dropped_by, user_id
                )));
            }
        }
        if user.is_admin && !actor.is_admin {
            return Err(MetaError::permission_denied(
                "only admin users can drop admin users".to_owned(),
            ));
        }

        // check if the user is the owner of any objects.
        let count = Object::find()
            .filter(object::Column::OwnerId.eq(user_id))
            .count(&txn)
            .await?;
        if count != 0 {
            return Err(MetaError::permission_denied(format!(
                "drop user {} is not allowed, because it owns {} objects",
                user.name, count
            )));
        }

        // check if the user granted any privileges to other users.
        let count = UserPrivilege::find()
            .filter(user_privilege::Column::GrantedBy.eq(user_id))
            .count(&txn)
            .await?;
        if count != 0 {
            return Err(MetaError::permission_denied(format!(
                "drop user {} is not allowed, because it granted {} privileges to others",
                user.name, count
            )));
        }

        // PostgreSQL keeps role-membership grantor dependencies for surviving memberships.
        // Memberships where the dropped role is the role or member are removed, but a role
        // that only granted a surviving membership still blocks DROP ROLE.
        let count = UserRoleMembership::find()
            .filter(user_role_membership::Column::GrantedBy.eq(user_id))
            .filter(user_role_membership::Column::RoleId.ne(user_id))
            .filter(user_role_membership::Column::MemberId.ne(user_id))
            .count(&txn)
            .await?;
        if count != 0 {
            return Err(MetaError::permission_denied(format!(
                "drop user {} is not allowed, because it granted {} role memberships to others",
                user.name, count
            )));
        }

        UserRoleMembership::delete_many()
            .filter(
                Condition::any()
                    .add(user_role_membership::Column::RoleId.eq(user_id))
                    .add(user_role_membership::Column::MemberId.eq(user_id)),
            )
            .exec(&txn)
            .await?;

        let res = User::delete_by_id(user_id).exec(&txn).await?;
        if res.rows_affected != 1 {
            return Err(MetaError::catalog_id_not_found("user", user_id));
        }
        txn.commit().await?;
        drop(inner);

        let version = self
            .notify_frontend(
                NotificationOperation::Delete,
                NotificationInfo::User(PbUserInfo {
                    id: user_id,
                    ..Default::default()
                }),
            )
            .await;

        Ok(version)
    }

    pub async fn grant_privilege(
        &self,
        user_ids: Vec<UserId>,
        new_grant_privileges: &[PbGrantPrivilege],
        grantor: UserId,
        with_grant_option: bool,
    ) -> MetaResult<NotificationVersion> {
        let db = self.user_controller_db().await;
        let txn = db.begin().await?;
        for user_id in &user_ids {
            ensure_user_id(*user_id, &txn).await?;
        }

        let mut privileges = vec![];
        for gp in new_grant_privileges {
            let id = extract_grant_obj_id(gp.get_object()?);
            let internal_table_ids = get_internal_tables_by_id(id.as_job_id(), &txn).await?;
            let index_state_table_ids =
                get_index_state_tables_by_table_id(id.as_table_id(), &txn).await?;
            for action_with_opt in &gp.action_with_opts {
                let action = action_with_opt.get_action()?.into();
                privileges.push(user_privilege::ActiveModel {
                    oid: Set(id),
                    granted_by: Set(grantor),
                    action: Set(action),
                    with_grant_option: Set(with_grant_option),
                    ..Default::default()
                });
                if action == Action::Select {
                    privileges.extend(
                        internal_table_ids
                            .iter()
                            .chain(index_state_table_ids.iter())
                            .map(|&tid| user_privilege::ActiveModel {
                                oid: Set(tid.as_object_id()),
                                granted_by: Set(grantor),
                                action: Set(Action::Select),
                                with_grant_option: Set(with_grant_option),
                                ..Default::default()
                            }),
                    );
                    let iceberg_privilege_object_ids =
                        get_iceberg_related_object_ids(id, &txn).await?;
                    privileges.extend(iceberg_privilege_object_ids.iter().map(
                        |&iceberg_object_id| user_privilege::ActiveModel {
                            oid: Set(iceberg_object_id),
                            granted_by: Set(grantor),
                            action: Set(action),
                            with_grant_option: Set(with_grant_option),
                            ..Default::default()
                        },
                    ));
                }
            }
        }

        // Check whether the grantor can grant each privilege. Inherited roles can supply
        // object ownership or WITH GRANT OPTION, matching PostgreSQL's effective privilege
        // checks. Record the effective role as the grantor so dependency tracking remains
        // attached to the privilege source.
        let user = User::find_by_id(grantor)
            .one(&txn)
            .await?
            .ok_or_else(|| MetaError::catalog_id_not_found("user", grantor))?;
        let filtered_privileges = if user.is_super {
            privileges
        } else {
            let existing_memberships = UserRoleMembership::find().all(&txn).await?;
            let inherit_graph = build_role_membership_graph(
                &existing_memberships,
                RoleMembershipGraphKind::Inherit,
            );
            let mut principals = vec![grantor];
            principals.extend(closest_reachable_role_ids(&inherit_graph, grantor));

            let mut filtered_privileges = vec![];
            for mut privilege in privileges {
                let oid = *privilege.oid.as_ref();
                let action = *privilege.action.as_ref();
                let owner = get_object_owner(oid, &txn).await?;
                if principals.contains(&owner) {
                    privilege.granted_by = Set(owner);
                    filtered_privileges.push(privilege);
                    continue;
                }

                let mut grant_option = None;
                for principal in &principals {
                    let filter = user_privilege::Column::UserId
                        .eq(*principal)
                        .and(user_privilege::Column::Oid.eq(oid))
                        .and(user_privilege::Column::Action.eq(action))
                        .and(user_privilege::Column::WithGrantOption.eq(true));
                    let privilege_id: Option<PrivilegeId> = UserPrivilege::find()
                        .select_only()
                        .column(user_privilege::Column::Id)
                        .filter(filter)
                        .into_tuple()
                        .one(&txn)
                        .await?;
                    if let Some(privilege_id) = privilege_id {
                        grant_option = Some((*principal, privilege_id));
                        break;
                    }
                }

                let Some((effective_grantor, privilege_id)) = grant_option else {
                    return Err(MetaError::permission_denied(format!(
                        "user {} does not have privilege {:?} with grant option on object {}",
                        grantor, action, oid
                    )));
                };
                privilege.granted_by = Set(effective_grantor);
                privilege.dependent_id = Set(Some(privilege_id));
                filtered_privileges.push(privilege);
            }
            filtered_privileges
        };

        // insert privileges
        let user_privileges = user_ids
            .iter()
            .flat_map(|user_id| {
                filtered_privileges.iter().map(|p| {
                    let mut p = p.clone();
                    p.user_id = Set(*user_id);
                    p
                })
            })
            .collect_vec();
        for privilege in user_privileges {
            let mut on_conflict = OnConflict::columns([
                user_privilege::Column::UserId,
                user_privilege::Column::Oid,
                user_privilege::Column::Action,
                user_privilege::Column::GrantedBy,
            ]);
            if *privilege.with_grant_option.as_ref() {
                on_conflict.update_column(user_privilege::Column::WithGrantOption);
            } else {
                // Workaround to support MYSQL for `DO NOTHING`.
                on_conflict.update_column(user_privilege::Column::UserId);
            }

            UserPrivilege::insert(privilege)
                .on_conflict(on_conflict)
                .do_nothing()
                .exec(&txn)
                .await?;
        }

        let user_infos = list_user_info_by_ids(user_ids, &txn).await?;

        txn.commit().await?;

        let version = self.notify_users_update(user_infos).await;
        Ok(version)
    }

    pub async fn revoke_privilege(
        &self,
        user_ids: Vec<UserId>,
        revoke_grant_privileges: &[PbGrantPrivilege],
        granted_by: UserId,
        revoke_by: UserId,
        revoke_grant_option: bool,
        cascade: bool,
    ) -> MetaResult<NotificationVersion> {
        let db = self.user_controller_db().await;
        let txn = db.begin().await?;
        for user_id in &user_ids {
            ensure_user_id(*user_id, &txn).await?;
        }
        // check whether revoke has the privilege to grant the privilege.
        let revoke_user = User::find_by_id(revoke_by)
            .one(&txn)
            .await?
            .ok_or_else(|| MetaError::catalog_id_not_found("user", revoke_by))?;

        // check whether user can revoke the privilege.
        if !revoke_user.is_super && granted_by != revoke_by {
            let granted_user_name: String = User::find_by_id(granted_by)
                .select_only()
                .column(user::Column::Name)
                .into_tuple()
                .one(&txn)
                .await?
                .ok_or_else(|| MetaError::catalog_id_not_found("user", granted_by))?;
            return Err(MetaError::permission_denied(format!(
                "user {} is not super, can't revoke privileges for {}",
                revoke_user.name, granted_user_name
            )));
        }

        let mut revoke_items = HashMap::new();
        for privilege in revoke_grant_privileges {
            let obj = extract_grant_obj_id(privilege.get_object()?);
            let internal_table_ids = get_internal_tables_by_id(obj.as_job_id(), &txn).await?;
            let index_state_table_ids =
                get_index_state_tables_by_table_id(obj.as_table_id(), &txn).await?;
            let mut include_select = false;
            let actions = privilege
                .action_with_opts
                .iter()
                .map(|ao| {
                    let action = Action::from(ao.get_action().unwrap());
                    if action == Action::Select {
                        include_select = true;
                    }
                    action
                })
                .collect_vec();
            revoke_items.insert(obj, actions);
            if include_select {
                revoke_items.extend(
                    internal_table_ids
                        .iter()
                        .chain(index_state_table_ids.iter())
                        .map(|&tid| (tid.as_object_id(), vec![Action::Select])),
                );
                let iceberg_privilege_object_ids =
                    get_iceberg_related_object_ids(obj, &txn).await?;
                if !iceberg_privilege_object_ids.is_empty() {
                    revoke_items.extend(
                        iceberg_privilege_object_ids
                            .into_iter()
                            .map(|iceberg_object_id| (iceberg_object_id, vec![Action::Select])),
                    );
                }
            }
        }

        let mut root_user_privileges: Vec<PartialUserPrivilege> = vec![];
        if revoke_user.is_super {
            let user_filter = user_privilege::Column::UserId.is_in(user_ids.clone());
            for (obj, actions) in revoke_items {
                root_user_privileges.extend(
                    UserPrivilege::find()
                        .select_only()
                        .columns([user_privilege::Column::Id, user_privilege::Column::UserId])
                        .filter(
                            user_filter
                                .clone()
                                .and(user_privilege::Column::Oid.eq(obj))
                                .and(user_privilege::Column::Action.is_in(actions)),
                        )
                        .into_partial_model()
                        .all(&txn)
                        .await?,
                );
            }
        } else {
            let existing_memberships = UserRoleMembership::find().all(&txn).await?;
            let inherit_graph = build_role_membership_graph(
                &existing_memberships,
                RoleMembershipGraphKind::Inherit,
            );
            let mut principals = vec![revoke_by];
            principals.extend(closest_reachable_role_ids(&inherit_graph, revoke_by));

            for (obj, actions) in revoke_items {
                let owner = get_object_owner(obj, &txn).await?;
                for action in actions {
                    let mut effective_grantors = HashSet::new();
                    if principals.contains(&owner) {
                        effective_grantors.insert(owner);
                    }
                    for principal in &principals {
                        let exists = UserPrivilege::find()
                            .filter(user_privilege::Column::UserId.eq(*principal))
                            .filter(user_privilege::Column::Oid.eq(obj))
                            .filter(user_privilege::Column::Action.eq(action))
                            .filter(user_privilege::Column::WithGrantOption.eq(true))
                            .one(&txn)
                            .await?
                            .is_some();
                        if exists {
                            effective_grantors.insert(*principal);
                        }
                    }
                    if effective_grantors.is_empty() {
                        return Err(MetaError::permission_denied(format!(
                            "user {} does not have privilege {:?} with grant option on object {}",
                            revoke_by, action, obj
                        )));
                    }

                    root_user_privileges.extend(
                        UserPrivilege::find()
                            .select_only()
                            .columns([user_privilege::Column::Id, user_privilege::Column::UserId])
                            .filter(user_privilege::Column::GrantedBy.is_in(effective_grantors))
                            .filter(user_privilege::Column::UserId.is_in(user_ids.clone()))
                            .filter(user_privilege::Column::Oid.eq(obj))
                            .filter(user_privilege::Column::Action.eq(action))
                            .into_partial_model()
                            .all(&txn)
                            .await?,
                    );
                }
            }
        }
        if root_user_privileges.is_empty() {
            tracing::warn!("no privilege to revoke, ignore it");
            return Ok(IGNORED_NOTIFICATION_VERSION);
        }

        // check if the user granted any privileges to other users.
        let root_privilege_ids = root_user_privileges.iter().map(|ur| ur.id).collect_vec();
        let (all_privilege_ids, to_update_user_ids) = if !cascade {
            ensure_privileges_not_referred(root_privilege_ids.clone(), &txn).await?;
            (
                root_privilege_ids.clone(),
                root_user_privileges
                    .iter()
                    .map(|ur| ur.user_id)
                    .collect_vec(),
            )
        } else {
            let all_user_privileges =
                get_referring_privileges_cascade(root_privilege_ids.clone(), &txn).await?;
            (
                all_user_privileges.iter().map(|ur| ur.id).collect_vec(),
                all_user_privileges
                    .iter()
                    .map(|ur| ur.user_id)
                    .collect_vec(),
            )
        };

        if revoke_grant_option {
            UserPrivilege::update_many()
                .col_expr(
                    user_privilege::Column::WithGrantOption,
                    SimpleExpr::Value(Value::Bool(Some(false))),
                )
                .filter(
                    user_privilege::Column::Id
                        .is_in(all_privilege_ids)
                        .and(user_privilege::Column::WithGrantOption.eq(true)),
                )
                .exec(&txn)
                .await?;
        } else {
            // The dependent privileges will be deleted cascade.
            UserPrivilege::delete_many()
                .filter(user_privilege::Column::Id.is_in(root_privilege_ids))
                .exec(&txn)
                .await?;
        }

        let user_infos = list_user_info_by_ids(to_update_user_ids, &txn).await?;

        txn.commit().await?;

        let version = self.notify_users_update(user_infos).await;
        Ok(version)
    }

    pub async fn grant_default_privileges(
        &self,
        user_ids: Vec<UserId>,
        database_id: DatabaseId,
        schema_ids: Vec<SchemaId>,
        grantor: UserId,
        actions: Vec<PbAction>,
        object_type: PbObjectType,
        grantees: Vec<UserId>,
        with_grant_option: bool,
    ) -> MetaResult<()> {
        tracing::debug!(
            ?user_ids,
            %database_id,
            ?schema_ids,
            ?actions,
            ?object_type,
            ?grantees,
            with_grant_option,
            "grant default privileges",
        );
        let db = self.user_controller_db().await;
        let txn = db.begin().await?;
        for user_id in &user_ids {
            ensure_user_id(*user_id, &txn).await?;
        }
        ensure_object_id(ObjectType::Database, database_id, &txn).await?;
        for schema_id in &schema_ids {
            ensure_object_id(ObjectType::Schema, *schema_id, &txn).await?;
        }
        for grantee in &grantees {
            ensure_user_id(*grantee, &txn).await?;
        }
        if object_type == PbObjectType::Schema {
            assert!(
                schema_ids.is_empty(),
                "schema_ids should be empty when object_type is Schema"
            );

            // Note that the UNIQUE constraint does not treat NULL values as equal, we cannot rely on conflict check
            // to update it and have to check existing default privileges manually.
            let actions = actions.iter().map(|&a| Action::from(a)).collect_vec();
            let existing_default_privileges: HashMap<_, _> = UserDefaultPrivilege::find()
                .select_only()
                .columns([
                    user_default_privilege::Column::Id,
                    user_default_privilege::Column::UserId,
                    user_default_privilege::Column::Grantee,
                    user_default_privilege::Column::Action,
                ])
                .filter(
                    user_default_privilege::Column::DatabaseId
                        .eq(database_id)
                        .and(user_default_privilege::Column::ObjectType.eq(ObjectType::Schema))
                        .and(user_default_privilege::Column::UserId.is_in(user_ids.clone()))
                        .and(user_default_privilege::Column::Grantee.is_in(grantees.clone()))
                        .and(user_default_privilege::Column::Action.is_in(actions.clone())),
                )
                .into_tuple::<(DefaultPrivilegeId, UserId, UserId, Action)>()
                .all(&txn)
                .await?
                .into_iter()
                .map(|(id, user_id, grantee, action)| ((user_id, grantee, action), id))
                .collect();

            for user_id in user_ids {
                for grantee in &grantees {
                    for action in &actions {
                        if let Some(existing_id) =
                            existing_default_privileges.get(&(user_id, *grantee, *action))
                            && with_grant_option
                        {
                            // If the default privilege already exists, we should update the grant option.
                            UserDefaultPrivilege::update(user_default_privilege::ActiveModel {
                                id: Set(*existing_id),
                                with_grant_option: Set(true),
                                granted_by: Set(grantor),
                                ..Default::default()
                            })
                            .exec(&txn)
                            .await?;
                        } else {
                            UserDefaultPrivilege::insert(user_default_privilege::ActiveModel {
                                id: Default::default(),
                                database_id: Set(database_id),
                                schema_id: Set(None),
                                object_type: Set(ObjectType::Schema),
                                for_materialized_view: Set(false),
                                user_id: Set(user_id),
                                grantee: Set(*grantee),
                                granted_by: Set(grantor),
                                action: Set(*action),
                                with_grant_option: Set(with_grant_option),
                            })
                            .exec(&txn)
                            .await?;
                        }
                    }
                }
            }
        } else {
            let mut default_privileges = vec![];
            for user_id in user_ids {
                for grantee in &grantees {
                    for action in &actions {
                        if schema_ids.is_empty() {
                            default_privileges.push(user_default_privilege::ActiveModel {
                                id: Default::default(),
                                database_id: Set(database_id),
                                schema_id: Set(None),
                                object_type: Set(object_type.into()),
                                for_materialized_view: Set(object_type == PbObjectType::Mview),
                                user_id: Set(user_id),
                                grantee: Set(*grantee),
                                granted_by: Set(grantor),
                                action: Set((*action).into()),
                                with_grant_option: Set(with_grant_option),
                            });
                            continue;
                        }
                        for schema_id in &schema_ids {
                            default_privileges.push(user_default_privilege::ActiveModel {
                                id: Default::default(),
                                database_id: Set(database_id),
                                schema_id: Set(Some(*schema_id)),
                                object_type: Set(object_type.into()),
                                for_materialized_view: Set(object_type == PbObjectType::Mview),
                                user_id: Set(user_id),
                                grantee: Set(*grantee),
                                granted_by: Set(grantor),
                                action: Set((*action).into()),
                                with_grant_option: Set(with_grant_option),
                            });
                        }
                    }
                }
            }

            let mut on_conflict = OnConflict::columns([
                user_default_privilege::Column::UserId,
                user_default_privilege::Column::DatabaseId,
                user_default_privilege::Column::SchemaId,
                user_default_privilege::Column::ObjectType,
                user_default_privilege::Column::ForMaterializedView,
                user_default_privilege::Column::Grantee,
                user_default_privilege::Column::Action,
            ]);
            if with_grant_option {
                on_conflict.update_column(user_default_privilege::Column::WithGrantOption);
            } else {
                // Workaround to support MYSQL for `DO NOTHING`.
                on_conflict.update_column(user_default_privilege::Column::UserId);
            }
            UserDefaultPrivilege::insert_many(default_privileges)
                .on_conflict(on_conflict)
                .do_nothing()
                .exec(&txn)
                .await?;
        }

        txn.commit().await?;
        Ok(())
    }

    pub async fn revoke_default_privileges(
        &self,
        user_ids: Vec<UserId>,
        database_id: DatabaseId,
        schema_ids: Vec<SchemaId>,
        actions: Vec<PbAction>,
        object_type: PbObjectType,
        grantees: Vec<UserId>,
        revoke_grant_option: bool,
    ) -> MetaResult<()> {
        let db = self.user_controller_db().await;
        let txn = db.begin().await?;
        for user_id in &user_ids {
            ensure_user_id(*user_id, &txn).await?;
        }

        let schema_filter = if schema_ids.is_empty() {
            user_default_privilege::Column::SchemaId.is_null()
        } else {
            user_default_privilege::Column::SchemaId.is_in(schema_ids)
        };
        let filter = user_default_privilege::Column::DatabaseId
            .eq(database_id)
            .and(schema_filter)
            .and(user_default_privilege::Column::UserId.is_in(user_ids))
            .and(user_default_privilege::Column::ObjectType.eq(ObjectType::from(object_type)))
            .and(user_default_privilege::Column::Grantee.is_in(grantees))
            .and(
                user_default_privilege::Column::Action
                    .is_in(actions.iter().map(|&a| Action::from(a))),
            );

        if revoke_grant_option {
            // update the `with_grant_option` field to false
            let res = UserDefaultPrivilege::update_many()
                .col_expr(
                    user_default_privilege::Column::WithGrantOption,
                    SimpleExpr::Value(Value::Bool(Some(false))),
                )
                .filter(filter.and(user_default_privilege::Column::WithGrantOption.eq(true)))
                .exec(&txn)
                .await?;
            tracing::info!(
                "revoke {count} grant option for default privileges",
                count = res.rows_affected
            );
        } else {
            let res = UserDefaultPrivilege::delete_many()
                .filter(filter)
                .exec(&txn)
                .await?;
            tracing::info!(
                "revoke {count} default privileges",
                count = res.rows_affected
            );
        }

        txn.commit().await?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use risingwave_pb::user::grant_privilege::{PbActionWithGrantOption, PbObject};

    use super::*;
    use crate::manager::MetaSrvEnv;

    const TEST_DATABASE_ID: DatabaseId = DatabaseId::new(1);
    const TEST_ROOT_USER_ID: UserId = UserId::new(1);

    fn make_test_user(name: &str) -> PbUserInfo {
        PbUserInfo {
            name: name.to_owned(),
            ..Default::default()
        }
    }

    fn make_privilege(
        object: PbObject,
        actions: &[PbAction],
        with_grant_option: bool,
    ) -> PbGrantPrivilege {
        PbGrantPrivilege {
            object: Some(object),
            action_with_opts: actions
                .iter()
                .map(|&action| PbActionWithGrantOption {
                    action: action as _,
                    with_grant_option,
                    ..Default::default()
                })
                .collect(),
        }
    }

    #[tokio::test]
    async fn test_user_and_privilege() -> MetaResult<()> {
        let mgr = CatalogController::new(MetaSrvEnv::for_test().await).await?;
        mgr.create_user(make_test_user("test_user_1")).await?;
        mgr.create_user(make_test_user("test_user_2")).await?;
        let user_1 = mgr.get_user_by_name("test_user_1").await?;
        let user_2 = mgr.get_user_by_name("test_user_2").await?;
        assert!(user_1.can_inherit);
        assert!(user_2.can_inherit);

        mgr.create_user(make_test_user("test_user_inherit_regression"))
            .await?;
        let inherit_regression_user = mgr.get_user_by_name("test_user_inherit_regression").await?;
        mgr.update_user(
            PbUserInfo {
                id: inherit_regression_user.user_id as _,
                can_inherit: Some(false),
                ..Default::default()
            },
            &[PbUpdateField::Inherit],
        )
        .await?;
        let inherit_regression_user = mgr.get_user(inherit_regression_user.user_id).await?;
        assert!(!inherit_regression_user.can_inherit);

        mgr.update_user(
            PbUserInfo {
                id: inherit_regression_user.user_id as _,
                can_create_db: true,
                ..Default::default()
            },
            &[PbUpdateField::CreateDb],
        )
        .await?;
        let inherit_regression_user = mgr.get_user(inherit_regression_user.user_id).await?;
        assert!(!inherit_regression_user.can_inherit);
        assert!(inherit_regression_user.can_create_db);

        assert!(
            mgr.create_user(make_test_user("test_user_1"))
                .await
                .is_err(),
            "user_1 already exists"
        );
        mgr.update_user(
            PbUserInfo {
                id: user_1.user_id,
                name: "test_user_1_new".to_owned(),
                ..Default::default()
            },
            &[PbUpdateField::Rename],
        )
        .await?;
        let user_1 = mgr.get_user(user_1.user_id).await?;
        assert_eq!(user_1.name, "test_user_1_new".to_owned());

        let conn_with_option = make_privilege(TEST_DATABASE_ID.into(), &[PbAction::Connect], true);
        let create_without_option =
            make_privilege(TEST_DATABASE_ID.into(), &[PbAction::Create], false);
        // ROOT grant CONN with grant option to user_1.
        mgr.grant_privilege(
            vec![user_1.user_id],
            std::slice::from_ref(&conn_with_option),
            TEST_ROOT_USER_ID,
            true,
        )
        .await?;
        // ROOT grant CREATE without grant option to user_1.
        mgr.grant_privilege(
            vec![user_1.user_id],
            std::slice::from_ref(&create_without_option),
            TEST_ROOT_USER_ID,
            false,
        )
        .await?;
        // user_1 grant CONN with grant option to user_2.
        mgr.grant_privilege(
            vec![user_2.user_id],
            std::slice::from_ref(&conn_with_option),
            user_1.user_id,
            true,
        )
        .await?;
        assert!(
            mgr.grant_privilege(
                vec![user_2.user_id],
                std::slice::from_ref(&create_without_option),
                user_1.user_id,
                false,
            )
            .await
            .is_err(),
            "user_1 does not have grant option for CREATE"
        );

        assert!(
            mgr.drop_user(user_1.user_id, TEST_ROOT_USER_ID, TEST_ROOT_USER_ID)
                .await
                .is_err(),
            "user_1 can't be dropped"
        );

        let privilege_1 = get_user_privilege(user_1.user_id, &mgr.inner.read().await.db).await?;
        assert_eq!(privilege_1.len(), 2);
        assert!(
            privilege_1
                .iter()
                .all(|gp| gp.object == Some(TEST_DATABASE_ID.into())
                    && gp.action_with_opts[0].granted_by == TEST_ROOT_USER_ID)
        );

        let privilege_2 = get_user_privilege(user_2.user_id, &mgr.inner.read().await.db).await?;
        assert_eq!(privilege_2.len(), 1);
        assert!(
            privilege_2
                .iter()
                .all(|gp| gp.object == Some(TEST_DATABASE_ID.into())
                    && gp.action_with_opts[0].granted_by == user_1.user_id
                    && gp.action_with_opts[0].with_grant_option)
        );

        // revoke privilege for others by non-super user.
        assert!(
            mgr.revoke_privilege(
                vec![user_1.user_id],
                std::slice::from_ref(&conn_with_option),
                TEST_ROOT_USER_ID,
                user_2.user_id,
                false,
                false
            )
            .await
            .is_err(),
            "user_2 can't revoke for user_1"
        );

        // revoke privilege without grant option.
        assert!(
            mgr.revoke_privilege(
                vec![user_2.user_id],
                std::slice::from_ref(&create_without_option),
                user_1.user_id,
                user_1.user_id,
                false,
                false
            )
            .await
            .is_err(),
            "user_2 don't have grant option for CREATE"
        );

        // revoke referred privilege in restrict mode.
        assert!(
            mgr.revoke_privilege(
                vec![user_1.user_id],
                std::slice::from_ref(&conn_with_option),
                TEST_ROOT_USER_ID,
                TEST_ROOT_USER_ID,
                false,
                false
            )
            .await
            .is_err(),
            "permission deny in restrict mode, CONN granted to user_2"
        );

        // revoke non-referred privilege in restrict mode.
        mgr.revoke_privilege(
            vec![user_1.user_id],
            std::slice::from_ref(&create_without_option),
            TEST_ROOT_USER_ID,
            TEST_ROOT_USER_ID,
            false,
            false,
        )
        .await?;

        let privilege_1 = get_user_privilege(user_1.user_id, &mgr.inner.read().await.db).await?;
        assert_eq!(privilege_1.len(), 1);
        assert!(
            privilege_1
                .iter()
                .all(|gp| gp.object == Some(TEST_DATABASE_ID.into())
                    && gp.action_with_opts[0].action == PbAction::Connect as i32)
        );

        // revoke grant option for referred privilege in cascade mode.
        mgr.revoke_privilege(
            vec![user_1.user_id],
            std::slice::from_ref(&conn_with_option),
            TEST_ROOT_USER_ID,
            TEST_ROOT_USER_ID,
            true,
            true,
        )
        .await?;
        let privilege_1 = get_user_privilege(user_1.user_id, &mgr.inner.read().await.db).await?;
        assert_eq!(privilege_1.len(), 1);
        assert!(
            privilege_1
                .iter()
                .all(|gp| gp.object == Some(TEST_DATABASE_ID.into())
                    && gp.action_with_opts[0].action == PbAction::Connect as i32
                    && !gp.action_with_opts[0].with_grant_option)
        );
        let privilege_2 = get_user_privilege(user_2.user_id, &mgr.inner.read().await.db).await?;
        assert_eq!(privilege_2.len(), 1);
        assert!(
            privilege_2
                .iter()
                .all(|gp| gp.object == Some(TEST_DATABASE_ID.into())
                    && gp.action_with_opts[0].action == PbAction::Connect as i32
                    && !gp.action_with_opts[0].with_grant_option)
        );

        // revoke referred privilege in cascade mode.
        mgr.revoke_privilege(
            vec![user_1.user_id],
            std::slice::from_ref(&conn_with_option),
            TEST_ROOT_USER_ID,
            TEST_ROOT_USER_ID,
            false,
            true,
        )
        .await?;
        let privilege_1 = get_user_privilege(user_1.user_id, &mgr.inner.read().await.db).await?;
        assert!(privilege_1.is_empty());
        let privilege_2 = get_user_privilege(user_2.user_id, &mgr.inner.read().await.db).await?;
        assert!(privilege_2.is_empty());

        mgr.drop_user(user_1.user_id, TEST_ROOT_USER_ID, TEST_ROOT_USER_ID)
            .await?;
        mgr.drop_user(user_2.user_id, TEST_ROOT_USER_ID, TEST_ROOT_USER_ID)
            .await?;
        Ok(())
    }

    #[tokio::test]
    async fn test_list_role_memberships_filters_and_requires_explicit_full_scan() -> MetaResult<()>
    {
        let mgr = CatalogController::new(MetaSrvEnv::for_test().await).await?;
        mgr.create_user(make_test_user("role_a")).await?;
        mgr.create_user(make_test_user("member_a")).await?;
        mgr.create_user(make_test_user("grantor_a")).await?;
        let role = mgr.get_user_by_name("role_a").await?;
        let member = mgr.get_user_by_name("member_a").await?;
        let grantor = mgr.get_user_by_name("grantor_a").await?;

        {
            let inner = mgr.inner.read().await;
            user_role_membership::ActiveModel {
                role_id: Set(role.user_id),
                member_id: Set(member.user_id),
                granted_by: Set(grantor.user_id),
                admin_option: Set(false),
                inherit_option: Set(true),
                set_option: Set(true),
                ..Default::default()
            }
            .insert(&inner.db)
            .await?;
        }

        assert!(
            mgr.list_role_memberships(&[], false).await?.is_empty(),
            "filtered list should not treat an empty filter as a full scan"
        );

        let member_memberships = mgr.list_role_memberships(&[member.user_id], false).await?;
        assert_eq!(member_memberships.len(), 1);
        assert_eq!(member_memberships[0].role_id, role.user_id);
        assert_eq!(member_memberships[0].member_id, member.user_id);
        assert_eq!(member_memberships[0].granted_by, grantor.user_id);

        assert!(
            mgr.list_role_memberships(&[role.user_id], false)
                .await?
                .is_empty(),
            "the filter should match member_id only"
        );

        let all_memberships = mgr.list_role_memberships(&[], true).await?;
        assert_eq!(all_memberships.len(), 1);
        Ok(())
    }

    #[tokio::test]
    async fn test_superuser_can_grant_role_with_explicit_superuser_grantor() -> MetaResult<()> {
        let mgr = CatalogController::new(MetaSrvEnv::for_test().await).await?;
        mgr.create_user(PbUserInfo {
            name: "super_grantor".to_owned(),
            is_super: true,
            ..Default::default()
        })
        .await?;
        mgr.create_user(make_test_user("role_a")).await?;
        mgr.create_user(make_test_user("member_a")).await?;

        let super_grantor = mgr.get_user_by_name("super_grantor").await?;
        let role = mgr.get_user_by_name("role_a").await?;
        let member = mgr.get_user_by_name("member_a").await?;

        let (_, memberships) = mgr
            .grant_role(
                vec![role.user_id],
                vec![member.user_id],
                super_grantor.user_id,
                TEST_ROOT_USER_ID,
                true,
                None,
                None,
                None,
            )
            .await?;

        assert_eq!(memberships.len(), 1);
        assert_eq!(memberships[0].role_id, role.user_id);
        assert_eq!(memberships[0].member_id, member.user_id);
        assert_eq!(memberships[0].granted_by, super_grantor.user_id);
        Ok(())
    }

    #[tokio::test]
    async fn test_cascade_preserves_non_bootstrap_superuser_grant() -> MetaResult<()> {
        let mgr = CatalogController::new(MetaSrvEnv::for_test().await).await?;
        mgr.create_user(PbUserInfo {
            name: "super_grantor".to_owned(),
            is_super: true,
            ..Default::default()
        })
        .await?;
        mgr.create_user(make_test_user("role_a")).await?;
        mgr.create_user(make_test_user("member_a")).await?;

        let super_grantor = mgr.get_user_by_name("super_grantor").await?;
        let role = mgr.get_user_by_name("role_a").await?;
        let member = mgr.get_user_by_name("member_a").await?;

        mgr.grant_role(
            vec![role.user_id],
            vec![super_grantor.user_id],
            TEST_ROOT_USER_ID,
            TEST_ROOT_USER_ID,
            false,
            Some(true),
            None,
            None,
        )
        .await?;
        mgr.grant_role(
            vec![role.user_id],
            vec![member.user_id],
            super_grantor.user_id,
            super_grantor.user_id,
            true,
            None,
            None,
            None,
        )
        .await?;

        mgr.revoke_role(
            vec![role.user_id],
            vec![super_grantor.user_id],
            TEST_ROOT_USER_ID,
            false,
            TEST_ROOT_USER_ID,
            true,
            false,
            false,
            true,
        )
        .await?;

        let memberships = mgr.list_role_memberships(&[member.user_id], false).await?;
        assert_eq!(memberships.len(), 1);
        assert_eq!(memberships[0].role_id, role.user_id);
        assert_eq!(memberships[0].member_id, member.user_id);
        assert_eq!(memberships[0].granted_by, super_grantor.user_id);
        Ok(())
    }

    #[tokio::test]
    async fn test_role_membership_lifecycle() -> MetaResult<()> {
        let mgr = CatalogController::new(MetaSrvEnv::for_test().await).await?;
        mgr.create_user(make_test_user("role_a")).await?;
        mgr.create_user(make_test_user("role_b")).await?;
        mgr.create_user(make_test_user("member_1")).await?;
        mgr.create_user(make_test_user("member_2")).await?;

        let role_a = mgr.get_user_by_name("role_a").await?;
        let role_b = mgr.get_user_by_name("role_b").await?;
        let member_1 = mgr.get_user_by_name("member_1").await?;
        let member_2 = mgr.get_user_by_name("member_2").await?;

        let (_, memberships) = mgr
            .grant_role(
                vec![role_a.user_id],
                vec![role_b.user_id],
                TEST_ROOT_USER_ID,
                TEST_ROOT_USER_ID,
                false,
                Some(true),
                None,
                None,
            )
            .await?;
        assert_eq!(memberships.len(), 1);
        assert!(memberships[0].admin_option);
        assert!(memberships[0].inherit_option);
        assert!(memberships[0].set_option);

        let (_, memberships) = mgr
            .grant_role(
                vec![role_a.user_id],
                vec![member_1.user_id],
                role_b.user_id,
                role_b.user_id,
                true,
                None,
                None,
                None,
            )
            .await?;
        assert_eq!(memberships.len(), 1);
        assert_eq!(memberships[0].granted_by, role_b.user_id);
        assert!(!memberships[0].admin_option);
        assert!(memberships[0].inherit_option);
        assert!(memberships[0].set_option);

        let member_1_memberships = mgr
            .list_role_memberships(&[member_1.user_id], false)
            .await?;
        assert_eq!(member_1_memberships.len(), 1);
        assert_eq!(member_1_memberships[0].role_id, role_a.user_id);
        assert_eq!(member_1_memberships[0].member_id, member_1.user_id);

        assert!(
            mgr.grant_role(
                vec![role_b.user_id],
                vec![role_a.user_id],
                TEST_ROOT_USER_ID,
                TEST_ROOT_USER_ID,
                false,
                None,
                None,
                None,
            )
            .await
            .is_err()
        );

        assert!(
            mgr.revoke_role(
                vec![role_a.user_id],
                vec![role_b.user_id],
                TEST_ROOT_USER_ID,
                false,
                TEST_ROOT_USER_ID,
                true,
                false,
                false,
                false,
            )
            .await
            .is_err()
        );
        mgr.revoke_role(
            vec![role_a.user_id],
            vec![role_b.user_id],
            TEST_ROOT_USER_ID,
            false,
            TEST_ROOT_USER_ID,
            true,
            false,
            false,
            true,
        )
        .await?;

        assert!(
            mgr.grant_role(
                vec![role_a.user_id],
                vec![member_2.user_id],
                role_b.user_id,
                role_b.user_id,
                true,
                None,
                None,
                None,
            )
            .await
            .is_err()
        );

        mgr.revoke_role(
            vec![role_a.user_id],
            vec![role_b.user_id],
            TEST_ROOT_USER_ID,
            false,
            TEST_ROOT_USER_ID,
            false,
            false,
            false,
            false,
        )
        .await?;

        mgr.drop_user(role_b.user_id, TEST_ROOT_USER_ID, TEST_ROOT_USER_ID)
            .await?;
        Ok(())
    }

    #[tokio::test]
    async fn test_role_membership_revoke_admin_option_cascade() -> MetaResult<()> {
        let mgr = CatalogController::new(MetaSrvEnv::for_test().await).await?;
        mgr.create_user(make_test_user("role_a")).await?;
        mgr.create_user(make_test_user("role_b")).await?;
        mgr.create_user(make_test_user("member_1")).await?;
        mgr.create_user(make_test_user("member_2")).await?;

        let role_a = mgr.get_user_by_name("role_a").await?;
        let role_b = mgr.get_user_by_name("role_b").await?;
        let member_1 = mgr.get_user_by_name("member_1").await?;
        let member_2 = mgr.get_user_by_name("member_2").await?;

        mgr.grant_role(
            vec![role_a.user_id],
            vec![role_b.user_id],
            TEST_ROOT_USER_ID,
            TEST_ROOT_USER_ID,
            false,
            Some(true),
            None,
            None,
        )
        .await?;
        mgr.grant_role(
            vec![role_a.user_id],
            vec![member_1.user_id],
            role_b.user_id,
            role_b.user_id,
            true,
            Some(true),
            None,
            None,
        )
        .await?;
        mgr.grant_role(
            vec![role_a.user_id],
            vec![member_2.user_id],
            member_1.user_id,
            member_1.user_id,
            true,
            None,
            None,
            None,
        )
        .await?;

        let err = mgr
            .revoke_role(
                vec![role_a.user_id],
                vec![role_b.user_id],
                TEST_ROOT_USER_ID,
                false,
                TEST_ROOT_USER_ID,
                true,
                false,
                false,
                false,
            )
            .await
            .unwrap_err();
        assert!(
            err.to_string()
                .contains("dependent role memberships exist; use CASCADE")
        );

        mgr.revoke_role(
            vec![role_a.user_id],
            vec![role_b.user_id],
            TEST_ROOT_USER_ID,
            false,
            TEST_ROOT_USER_ID,
            true,
            false,
            false,
            true,
        )
        .await?;

        let memberships = mgr.list_role_memberships(&[], true).await?;
        assert_eq!(memberships.len(), 1);
        assert_eq!(memberships[0].member_id, role_b.user_id);
        assert_eq!(memberships[0].role_id, role_a.user_id);
        assert!(!memberships[0].admin_option);

        Ok(())
    }

    #[tokio::test]
    async fn test_role_membership_inherit_defaults_from_member() -> MetaResult<()> {
        let mgr = CatalogController::new(MetaSrvEnv::for_test().await).await?;
        mgr.create_user(make_test_user("role_a")).await?;
        let mut noinherit_member = make_test_user("noinherit_member");
        noinherit_member.can_inherit = Some(false);
        mgr.create_user(noinherit_member).await?;

        let role_a = mgr.get_user_by_name("role_a").await?;
        let noinherit_member = mgr.get_user_by_name("noinherit_member").await?;

        let (_, memberships) = mgr
            .grant_role(
                vec![role_a.user_id],
                vec![noinherit_member.user_id],
                TEST_ROOT_USER_ID,
                TEST_ROOT_USER_ID,
                false,
                None,
                None,
                None,
            )
            .await?;
        assert_eq!(memberships.len(), 1);
        assert!(!memberships[0].inherit_option);
        assert!(memberships[0].set_option);

        Ok(())
    }

    #[tokio::test]
    async fn test_role_grant_requires_executor_to_possess_grantor_in_meta() -> MetaResult<()> {
        let mgr = CatalogController::new(MetaSrvEnv::for_test().await).await?;
        mgr.create_user(make_test_user("role_a")).await?;
        mgr.create_user(make_test_user("manager")).await?;
        mgr.create_user(make_test_user("outsider")).await?;
        mgr.create_user(make_test_user("member_a")).await?;

        let role_a = mgr.get_user_by_name("role_a").await?;
        let manager = mgr.get_user_by_name("manager").await?;
        let outsider = mgr.get_user_by_name("outsider").await?;
        let member_a = mgr.get_user_by_name("member_a").await?;

        mgr.grant_role(
            vec![role_a.user_id],
            vec![manager.user_id],
            TEST_ROOT_USER_ID,
            TEST_ROOT_USER_ID,
            false,
            Some(true),
            None,
            None,
        )
        .await?;

        let err = mgr
            .grant_role(
                vec![role_a.user_id],
                vec![member_a.user_id],
                manager.user_id,
                outsider.user_id,
                true,
                None,
                None,
                None,
            )
            .await
            .unwrap_err();
        assert!(err.to_string().contains("does not possess grantor role"));

        Ok(())
    }

    #[tokio::test]
    async fn test_role_grant_does_not_treat_target_role_as_admin_option() -> MetaResult<()> {
        let mgr = CatalogController::new(MetaSrvEnv::for_test().await).await?;
        mgr.create_user(make_test_user("role_a")).await?;
        mgr.create_user(make_test_user("member_a")).await?;

        let role_a = mgr.get_user_by_name("role_a").await?;
        let member_a = mgr.get_user_by_name("member_a").await?;

        let err = mgr
            .grant_role(
                vec![role_a.user_id],
                vec![member_a.user_id],
                role_a.user_id,
                role_a.user_id,
                true,
                None,
                None,
                None,
            )
            .await
            .unwrap_err();
        assert!(err.to_string().contains("ADMIN OPTION"));

        Ok(())
    }

    #[tokio::test]
    async fn test_role_grant_does_not_inherit_superuser_grantor_authority() -> MetaResult<()> {
        let mgr = CatalogController::new(MetaSrvEnv::for_test().await).await?;
        mgr.create_user(make_test_user("target_role")).await?;
        let mut super_role = make_test_user("super_role");
        super_role.is_super = true;
        mgr.create_user(super_role).await?;
        mgr.create_user(make_test_user("member_a")).await?;
        mgr.create_user(make_test_user("grantee")).await?;

        let target_role = mgr.get_user_by_name("target_role").await?;
        let super_role = mgr.get_user_by_name("super_role").await?;
        let member_a = mgr.get_user_by_name("member_a").await?;
        let grantee = mgr.get_user_by_name("grantee").await?;

        mgr.grant_role(
            vec![super_role.user_id],
            vec![member_a.user_id],
            TEST_ROOT_USER_ID,
            TEST_ROOT_USER_ID,
            false,
            None,
            Some(true),
            Some(true),
        )
        .await?;

        let err = mgr
            .grant_role(
                vec![target_role.user_id],
                vec![grantee.user_id],
                super_role.user_id,
                member_a.user_id,
                true,
                None,
                None,
                None,
            )
            .await
            .unwrap_err();
        assert!(err.to_string().contains("ADMIN OPTION"));

        let err = mgr
            .grant_role(
                vec![target_role.user_id],
                vec![grantee.user_id],
                super_role.user_id,
                TEST_ROOT_USER_ID,
                true,
                None,
                None,
                None,
            )
            .await
            .unwrap_err();
        assert!(err.to_string().contains("ADMIN OPTION"));

        let (_, memberships) = mgr
            .grant_role(
                vec![target_role.user_id],
                vec![grantee.user_id],
                TEST_ROOT_USER_ID,
                TEST_ROOT_USER_ID,
                true,
                None,
                None,
                None,
            )
            .await?;
        assert_eq!(memberships.len(), 1);
        assert_eq!(memberships[0].granted_by, TEST_ROOT_USER_ID);

        Ok(())
    }

    #[tokio::test]
    async fn test_role_grant_selects_possessed_admin_grantor() -> MetaResult<()> {
        let mgr = CatalogController::new(MetaSrvEnv::for_test().await).await?;
        mgr.create_user(make_test_user("target_role")).await?;
        mgr.create_user(make_test_user("manager")).await?;
        mgr.create_user(make_test_user("grantor")).await?;
        mgr.create_user(make_test_user("member_a")).await?;
        mgr.create_user(make_test_user("explicit_member")).await?;

        let target_role = mgr.get_user_by_name("target_role").await?;
        let manager = mgr.get_user_by_name("manager").await?;
        let grantor = mgr.get_user_by_name("grantor").await?;
        let member_a = mgr.get_user_by_name("member_a").await?;
        let explicit_member = mgr.get_user_by_name("explicit_member").await?;

        mgr.grant_role(
            vec![target_role.user_id],
            vec![manager.user_id],
            TEST_ROOT_USER_ID,
            TEST_ROOT_USER_ID,
            false,
            Some(true),
            None,
            None,
        )
        .await?;
        mgr.grant_role(
            vec![manager.user_id],
            vec![grantor.user_id],
            TEST_ROOT_USER_ID,
            TEST_ROOT_USER_ID,
            false,
            None,
            Some(true),
            None,
        )
        .await?;

        let (_, memberships) = mgr
            .grant_role(
                vec![target_role.user_id],
                vec![member_a.user_id],
                grantor.user_id,
                grantor.user_id,
                false,
                None,
                None,
                None,
            )
            .await?;
        assert_eq!(memberships.len(), 1);
        assert_eq!(memberships[0].granted_by, manager.user_id);

        let err = mgr
            .grant_role(
                vec![target_role.user_id],
                vec![explicit_member.user_id],
                grantor.user_id,
                grantor.user_id,
                true,
                None,
                None,
                None,
            )
            .await
            .unwrap_err();
        assert!(err.to_string().contains("ADMIN OPTION"));
        assert!(
            mgr.list_role_memberships(&[explicit_member.user_id], false)
                .await?
                .is_empty()
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_role_grant_selects_closest_possessed_admin_grantor() -> MetaResult<()> {
        let mgr = CatalogController::new(MetaSrvEnv::for_test().await).await?;
        mgr.create_user(make_test_user("target_role")).await?;
        mgr.create_user(make_test_user("indirect_manager")).await?;
        mgr.create_user(make_test_user("direct_manager")).await?;
        mgr.create_user(make_test_user("grantor")).await?;
        mgr.create_user(make_test_user("member_a")).await?;

        let target_role = mgr.get_user_by_name("target_role").await?;
        let indirect_manager = mgr.get_user_by_name("indirect_manager").await?;
        let direct_manager = mgr.get_user_by_name("direct_manager").await?;
        let grantor = mgr.get_user_by_name("grantor").await?;
        let member_a = mgr.get_user_by_name("member_a").await?;
        assert!(indirect_manager.user_id < direct_manager.user_id);

        mgr.grant_role(
            vec![target_role.user_id],
            vec![indirect_manager.user_id],
            TEST_ROOT_USER_ID,
            TEST_ROOT_USER_ID,
            false,
            Some(true),
            None,
            None,
        )
        .await?;
        mgr.grant_role(
            vec![target_role.user_id],
            vec![direct_manager.user_id],
            TEST_ROOT_USER_ID,
            TEST_ROOT_USER_ID,
            false,
            Some(true),
            None,
            None,
        )
        .await?;
        mgr.grant_role(
            vec![indirect_manager.user_id],
            vec![direct_manager.user_id],
            TEST_ROOT_USER_ID,
            TEST_ROOT_USER_ID,
            false,
            None,
            Some(true),
            None,
        )
        .await?;
        mgr.grant_role(
            vec![direct_manager.user_id],
            vec![grantor.user_id],
            TEST_ROOT_USER_ID,
            TEST_ROOT_USER_ID,
            false,
            None,
            Some(true),
            None,
        )
        .await?;

        let (_, memberships) = mgr
            .grant_role(
                vec![target_role.user_id],
                vec![member_a.user_id],
                grantor.user_id,
                grantor.user_id,
                false,
                None,
                None,
                None,
            )
            .await?;
        assert_eq!(memberships.len(), 1);
        assert_eq!(memberships[0].granted_by, direct_manager.user_id);

        Ok(())
    }

    #[tokio::test]
    async fn test_role_grant_omitted_grantor_requires_inherit_possession() -> MetaResult<()> {
        let mgr = CatalogController::new(MetaSrvEnv::for_test().await).await?;
        mgr.create_user(make_test_user("target_role")).await?;
        mgr.create_user(make_test_user("manager")).await?;
        mgr.create_user(make_test_user("grantor")).await?;
        mgr.create_user(make_test_user("member_a")).await?;

        let target_role = mgr.get_user_by_name("target_role").await?;
        let manager = mgr.get_user_by_name("manager").await?;
        let grantor = mgr.get_user_by_name("grantor").await?;
        let member_a = mgr.get_user_by_name("member_a").await?;

        mgr.grant_role(
            vec![target_role.user_id],
            vec![manager.user_id],
            TEST_ROOT_USER_ID,
            TEST_ROOT_USER_ID,
            false,
            Some(true),
            None,
            None,
        )
        .await?;
        mgr.grant_role(
            vec![manager.user_id],
            vec![grantor.user_id],
            TEST_ROOT_USER_ID,
            TEST_ROOT_USER_ID,
            false,
            None,
            Some(false),
            Some(true),
        )
        .await?;

        let err = mgr
            .grant_role(
                vec![target_role.user_id],
                vec![member_a.user_id],
                grantor.user_id,
                grantor.user_id,
                false,
                None,
                None,
                None,
            )
            .await
            .unwrap_err();
        assert!(err.to_string().contains("ADMIN OPTION"));

        let err = mgr
            .grant_role(
                vec![target_role.user_id],
                vec![member_a.user_id],
                manager.user_id,
                grantor.user_id,
                true,
                None,
                None,
                None,
            )
            .await
            .unwrap_err();
        assert!(err.to_string().contains("does not possess grantor role"));

        Ok(())
    }

    #[tokio::test]
    async fn test_role_grant_omitted_can_select_per_role_grantors() -> MetaResult<()> {
        let mgr = CatalogController::new(MetaSrvEnv::for_test().await).await?;
        mgr.create_user(make_test_user("role_a")).await?;
        mgr.create_user(make_test_user("role_b")).await?;
        mgr.create_user(make_test_user("manager_a")).await?;
        mgr.create_user(make_test_user("manager_b")).await?;
        mgr.create_user(make_test_user("grantor")).await?;
        mgr.create_user(make_test_user("member_a")).await?;

        let role_a = mgr.get_user_by_name("role_a").await?;
        let role_b = mgr.get_user_by_name("role_b").await?;
        let manager_a = mgr.get_user_by_name("manager_a").await?;
        let manager_b = mgr.get_user_by_name("manager_b").await?;
        let grantor = mgr.get_user_by_name("grantor").await?;
        let member_a = mgr.get_user_by_name("member_a").await?;

        mgr.grant_role(
            vec![role_a.user_id],
            vec![manager_a.user_id],
            TEST_ROOT_USER_ID,
            TEST_ROOT_USER_ID,
            false,
            Some(true),
            None,
            None,
        )
        .await?;
        mgr.grant_role(
            vec![role_b.user_id],
            vec![manager_b.user_id],
            TEST_ROOT_USER_ID,
            TEST_ROOT_USER_ID,
            false,
            Some(true),
            None,
            None,
        )
        .await?;
        mgr.grant_role(
            vec![manager_a.user_id, manager_b.user_id],
            vec![grantor.user_id],
            TEST_ROOT_USER_ID,
            TEST_ROOT_USER_ID,
            false,
            None,
            Some(true),
            None,
        )
        .await?;

        let (_, memberships) = mgr
            .grant_role(
                vec![role_a.user_id, role_b.user_id],
                vec![member_a.user_id],
                grantor.user_id,
                grantor.user_id,
                false,
                None,
                None,
                None,
            )
            .await?;
        assert_eq!(memberships.len(), 2);
        let role_a_membership = memberships
            .iter()
            .find(|membership| membership.role_id == role_a.user_id)
            .unwrap();
        let role_b_membership = memberships
            .iter()
            .find(|membership| membership.role_id == role_b.user_id)
            .unwrap();
        assert_eq!(role_a_membership.granted_by, manager_a.user_id);
        assert_eq!(role_b_membership.granted_by, manager_b.user_id);

        Ok(())
    }

    #[tokio::test]
    async fn test_role_revoke_selects_possessed_admin_grantor() -> MetaResult<()> {
        let mgr = CatalogController::new(MetaSrvEnv::for_test().await).await?;
        mgr.create_user(make_test_user("role_a")).await?;
        mgr.create_user(make_test_user("manager")).await?;
        mgr.create_user(make_test_user("revoker")).await?;
        mgr.create_user(make_test_user("member_a")).await?;

        let role_a = mgr.get_user_by_name("role_a").await?;
        let manager = mgr.get_user_by_name("manager").await?;
        let revoker = mgr.get_user_by_name("revoker").await?;
        let member_a = mgr.get_user_by_name("member_a").await?;

        mgr.grant_role(
            vec![role_a.user_id],
            vec![manager.user_id],
            TEST_ROOT_USER_ID,
            TEST_ROOT_USER_ID,
            false,
            Some(true),
            None,
            None,
        )
        .await?;
        mgr.grant_role(
            vec![manager.user_id],
            vec![revoker.user_id],
            TEST_ROOT_USER_ID,
            TEST_ROOT_USER_ID,
            false,
            None,
            Some(true),
            None,
        )
        .await?;
        mgr.grant_role(
            vec![role_a.user_id],
            vec![member_a.user_id],
            manager.user_id,
            manager.user_id,
            true,
            None,
            None,
            None,
        )
        .await?;

        mgr.revoke_role(
            vec![role_a.user_id],
            vec![member_a.user_id],
            TEST_ROOT_USER_ID,
            false,
            revoker.user_id,
            false,
            false,
            false,
            false,
        )
        .await?;

        let memberships = mgr
            .list_role_memberships(&[member_a.user_id], false)
            .await?;
        assert!(memberships.is_empty());

        Ok(())
    }

    #[tokio::test]
    async fn test_role_revoke_selects_grantor_per_membership_edge() -> MetaResult<()> {
        let mgr = CatalogController::new(MetaSrvEnv::for_test().await).await?;
        mgr.create_user(make_test_user("role_a")).await?;
        mgr.create_user(make_test_user("role_b")).await?;
        mgr.create_user(make_test_user("manager_a")).await?;
        mgr.create_user(make_test_user("manager_b")).await?;
        mgr.create_user(make_test_user("revoker")).await?;
        mgr.create_user(make_test_user("member_a")).await?;

        let role_a = mgr.get_user_by_name("role_a").await?;
        let role_b = mgr.get_user_by_name("role_b").await?;
        let manager_a = mgr.get_user_by_name("manager_a").await?;
        let manager_b = mgr.get_user_by_name("manager_b").await?;
        let revoker = mgr.get_user_by_name("revoker").await?;
        let member_a = mgr.get_user_by_name("member_a").await?;

        mgr.grant_role(
            vec![role_a.user_id],
            vec![manager_a.user_id],
            TEST_ROOT_USER_ID,
            TEST_ROOT_USER_ID,
            false,
            Some(true),
            None,
            None,
        )
        .await?;
        mgr.grant_role(
            vec![role_b.user_id],
            vec![manager_b.user_id],
            TEST_ROOT_USER_ID,
            TEST_ROOT_USER_ID,
            false,
            Some(true),
            None,
            None,
        )
        .await?;
        mgr.grant_role(
            vec![manager_a.user_id, manager_b.user_id],
            vec![revoker.user_id],
            TEST_ROOT_USER_ID,
            TEST_ROOT_USER_ID,
            false,
            None,
            Some(true),
            None,
        )
        .await?;
        mgr.grant_role(
            vec![role_a.user_id],
            vec![member_a.user_id],
            manager_a.user_id,
            manager_a.user_id,
            true,
            None,
            None,
            None,
        )
        .await?;
        mgr.grant_role(
            vec![role_b.user_id],
            vec![member_a.user_id],
            manager_b.user_id,
            manager_b.user_id,
            true,
            None,
            None,
            None,
        )
        .await?;

        mgr.revoke_role(
            vec![role_a.user_id, role_b.user_id],
            vec![member_a.user_id],
            TEST_ROOT_USER_ID,
            false,
            revoker.user_id,
            false,
            false,
            false,
            false,
        )
        .await?;

        let memberships = mgr
            .list_role_memberships(&[member_a.user_id], false)
            .await?;
        assert!(memberships.is_empty());

        Ok(())
    }

    #[tokio::test]
    async fn test_role_revoke_removes_all_effective_grantors_for_same_edge() -> MetaResult<()> {
        let mgr = CatalogController::new(MetaSrvEnv::for_test().await).await?;
        mgr.create_user(make_test_user("role_a")).await?;
        mgr.create_user(make_test_user("manager_a")).await?;
        mgr.create_user(make_test_user("manager_b")).await?;
        mgr.create_user(make_test_user("revoker")).await?;
        mgr.create_user(make_test_user("member_a")).await?;

        let role_a = mgr.get_user_by_name("role_a").await?;
        let manager_a = mgr.get_user_by_name("manager_a").await?;
        let manager_b = mgr.get_user_by_name("manager_b").await?;
        let revoker = mgr.get_user_by_name("revoker").await?;
        let member_a = mgr.get_user_by_name("member_a").await?;

        for manager in [manager_a.user_id, manager_b.user_id] {
            mgr.grant_role(
                vec![role_a.user_id],
                vec![manager],
                TEST_ROOT_USER_ID,
                TEST_ROOT_USER_ID,
                false,
                Some(true),
                None,
                None,
            )
            .await?;
        }
        mgr.grant_role(
            vec![manager_a.user_id, manager_b.user_id],
            vec![revoker.user_id],
            TEST_ROOT_USER_ID,
            TEST_ROOT_USER_ID,
            false,
            None,
            Some(true),
            None,
        )
        .await?;
        for manager in [manager_a.user_id, manager_b.user_id] {
            mgr.grant_role(
                vec![role_a.user_id],
                vec![member_a.user_id],
                manager,
                manager,
                true,
                None,
                None,
                None,
            )
            .await?;
        }

        let memberships = mgr
            .list_role_memberships(&[member_a.user_id], false)
            .await?;
        assert_eq!(memberships.len(), 2);

        mgr.revoke_role(
            vec![role_a.user_id],
            vec![member_a.user_id],
            TEST_ROOT_USER_ID,
            false,
            revoker.user_id,
            false,
            false,
            false,
            false,
        )
        .await?;

        let memberships = mgr
            .list_role_memberships(&[member_a.user_id], false)
            .await?;
        assert!(memberships.is_empty());
        Ok(())
    }

    #[tokio::test]
    async fn test_superuser_revoke_without_granted_by_removes_non_bootstrap_grant() -> MetaResult<()>
    {
        let mgr = CatalogController::new(MetaSrvEnv::for_test().await).await?;
        mgr.create_user(make_test_user("role_a")).await?;
        mgr.create_user(make_test_user("manager")).await?;
        mgr.create_user(make_test_user("member_a")).await?;

        let role_a = mgr.get_user_by_name("role_a").await?;
        let manager = mgr.get_user_by_name("manager").await?;
        let member_a = mgr.get_user_by_name("member_a").await?;

        mgr.grant_role(
            vec![role_a.user_id],
            vec![manager.user_id],
            TEST_ROOT_USER_ID,
            TEST_ROOT_USER_ID,
            false,
            Some(true),
            None,
            None,
        )
        .await?;
        mgr.grant_role(
            vec![role_a.user_id],
            vec![member_a.user_id],
            manager.user_id,
            manager.user_id,
            true,
            None,
            None,
            None,
        )
        .await?;

        let memberships = mgr
            .list_role_memberships(&[member_a.user_id], false)
            .await?;
        assert_eq!(memberships.len(), 1);
        assert_eq!(memberships[0].granted_by, manager.user_id);

        mgr.revoke_role(
            vec![role_a.user_id],
            vec![member_a.user_id],
            TEST_ROOT_USER_ID,
            false,
            TEST_ROOT_USER_ID,
            false,
            false,
            false,
            false,
        )
        .await?;

        let memberships = mgr
            .list_role_memberships(&[member_a.user_id], false)
            .await?;
        assert!(memberships.is_empty());

        Ok(())
    }

    #[tokio::test]
    async fn test_role_revoke_omitted_grantor_requires_inherit_possession() -> MetaResult<()> {
        let mgr = CatalogController::new(MetaSrvEnv::for_test().await).await?;
        mgr.create_user(make_test_user("role_a")).await?;
        mgr.create_user(make_test_user("manager")).await?;
        mgr.create_user(make_test_user("revoker")).await?;
        mgr.create_user(make_test_user("member_a")).await?;

        let role_a = mgr.get_user_by_name("role_a").await?;
        let manager = mgr.get_user_by_name("manager").await?;
        let revoker = mgr.get_user_by_name("revoker").await?;
        let member_a = mgr.get_user_by_name("member_a").await?;

        mgr.grant_role(
            vec![role_a.user_id],
            vec![manager.user_id],
            TEST_ROOT_USER_ID,
            TEST_ROOT_USER_ID,
            false,
            Some(true),
            None,
            None,
        )
        .await?;
        mgr.grant_role(
            vec![manager.user_id],
            vec![revoker.user_id],
            TEST_ROOT_USER_ID,
            TEST_ROOT_USER_ID,
            false,
            None,
            Some(false),
            Some(true),
        )
        .await?;
        mgr.grant_role(
            vec![role_a.user_id],
            vec![member_a.user_id],
            manager.user_id,
            manager.user_id,
            true,
            None,
            None,
            None,
        )
        .await?;

        mgr.revoke_role(
            vec![role_a.user_id],
            vec![member_a.user_id],
            TEST_ROOT_USER_ID,
            false,
            revoker.user_id,
            false,
            false,
            false,
            false,
        )
        .await?;

        let memberships = mgr
            .list_role_memberships(&[member_a.user_id], false)
            .await?;
        assert_eq!(memberships.len(), 1);
        assert_eq!(memberships[0].granted_by, manager.user_id);

        let err = mgr
            .revoke_role(
                vec![role_a.user_id],
                vec![member_a.user_id],
                manager.user_id,
                true,
                revoker.user_id,
                false,
                false,
                false,
                false,
            )
            .await
            .unwrap_err();
        assert!(err.to_string().contains("does not possess grantor role"));

        Ok(())
    }

    #[tokio::test]
    async fn test_role_revoke_explicit_grantor_is_strict() -> MetaResult<()> {
        let mgr = CatalogController::new(MetaSrvEnv::for_test().await).await?;
        mgr.create_user(make_test_user("role_a")).await?;
        mgr.create_user(make_test_user("manager")).await?;
        mgr.create_user(make_test_user("revoker")).await?;
        mgr.create_user(make_test_user("outsider")).await?;
        mgr.create_user(make_test_user("member_a")).await?;

        let role_a = mgr.get_user_by_name("role_a").await?;
        let manager = mgr.get_user_by_name("manager").await?;
        let revoker = mgr.get_user_by_name("revoker").await?;
        let outsider = mgr.get_user_by_name("outsider").await?;
        let member_a = mgr.get_user_by_name("member_a").await?;

        mgr.grant_role(
            vec![role_a.user_id],
            vec![manager.user_id],
            TEST_ROOT_USER_ID,
            TEST_ROOT_USER_ID,
            false,
            Some(true),
            None,
            None,
        )
        .await?;
        mgr.grant_role(
            vec![manager.user_id],
            vec![revoker.user_id],
            TEST_ROOT_USER_ID,
            TEST_ROOT_USER_ID,
            false,
            None,
            Some(true),
            None,
        )
        .await?;
        mgr.grant_role(
            vec![role_a.user_id],
            vec![member_a.user_id],
            manager.user_id,
            manager.user_id,
            true,
            None,
            None,
            None,
        )
        .await?;

        let err = mgr
            .revoke_role(
                vec![role_a.user_id],
                vec![member_a.user_id],
                manager.user_id,
                true,
                outsider.user_id,
                false,
                false,
                false,
                false,
            )
            .await
            .unwrap_err();
        assert!(err.to_string().contains("does not possess grantor role"));

        mgr.revoke_role(
            vec![role_a.user_id],
            vec![member_a.user_id],
            revoker.user_id,
            true,
            revoker.user_id,
            false,
            false,
            false,
            false,
        )
        .await?;
        let memberships = mgr
            .list_role_memberships(&[member_a.user_id], false)
            .await?;
        assert_eq!(memberships.len(), 1);

        mgr.revoke_role(
            vec![role_a.user_id],
            vec![member_a.user_id],
            manager.user_id,
            true,
            revoker.user_id,
            false,
            false,
            false,
            false,
        )
        .await?;

        let memberships = mgr
            .list_role_memberships(&[member_a.user_id], false)
            .await?;
        assert!(memberships.is_empty());

        Ok(())
    }

    #[tokio::test]
    async fn test_role_membership_option_downgrade_and_omitted_preserve() -> MetaResult<()> {
        let mgr = CatalogController::new(MetaSrvEnv::for_test().await).await?;
        mgr.create_user(make_test_user("role_a")).await?;
        mgr.create_user(make_test_user("member_a")).await?;

        let role_a = mgr.get_user_by_name("role_a").await?;
        let member_a = mgr.get_user_by_name("member_a").await?;

        mgr.grant_role(
            vec![role_a.user_id],
            vec![member_a.user_id],
            TEST_ROOT_USER_ID,
            TEST_ROOT_USER_ID,
            false,
            Some(true),
            Some(true),
            Some(true),
        )
        .await?;

        let (_, memberships) = mgr
            .grant_role(
                vec![role_a.user_id],
                vec![member_a.user_id],
                TEST_ROOT_USER_ID,
                TEST_ROOT_USER_ID,
                false,
                Some(false),
                None,
                None,
            )
            .await?;
        assert!(!memberships[0].admin_option);
        assert!(memberships[0].inherit_option);
        assert!(memberships[0].set_option);

        let (_, memberships) = mgr
            .grant_role(
                vec![role_a.user_id],
                vec![member_a.user_id],
                TEST_ROOT_USER_ID,
                TEST_ROOT_USER_ID,
                false,
                None,
                Some(false),
                None,
            )
            .await?;
        assert!(!memberships[0].admin_option);
        assert!(!memberships[0].inherit_option);
        assert!(memberships[0].set_option);

        let (_, memberships) = mgr
            .grant_role(
                vec![role_a.user_id],
                vec![member_a.user_id],
                TEST_ROOT_USER_ID,
                TEST_ROOT_USER_ID,
                false,
                None,
                None,
                Some(false),
            )
            .await?;
        assert!(!memberships[0].admin_option);
        assert!(!memberships[0].inherit_option);
        assert!(!memberships[0].set_option);

        Ok(())
    }

    #[tokio::test]
    async fn test_drop_user_requires_create_user_and_admin_option() -> MetaResult<()> {
        let mgr = CatalogController::new(MetaSrvEnv::for_test().await).await?;
        mgr.create_user(make_test_user("target_role")).await?;
        let mut manager = make_test_user("manager");
        manager.can_create_user = true;
        mgr.create_user(manager).await?;

        let target_role = mgr.get_user_by_name("target_role").await?;
        let manager = mgr.get_user_by_name("manager").await?;

        let err = mgr
            .drop_user(target_role.user_id, manager.user_id, manager.user_id)
            .await
            .unwrap_err();
        assert!(err.to_string().contains("ADMIN OPTION"));

        mgr.grant_role(
            vec![target_role.user_id],
            vec![manager.user_id],
            TEST_ROOT_USER_ID,
            TEST_ROOT_USER_ID,
            false,
            Some(true),
            None,
            None,
        )
        .await?;

        mgr.drop_user(target_role.user_id, manager.user_id, manager.user_id)
            .await?;
        assert!(mgr.get_user_by_name("target_role").await.is_err());

        Ok(())
    }

    #[tokio::test]
    async fn test_drop_user_rejects_default_admin_user() -> MetaResult<()> {
        let mgr = CatalogController::new(MetaSrvEnv::for_test().await).await?;
        let admin_user = mgr.get_user_by_name(DEFAULT_SUPER_USER_FOR_ADMIN).await?;

        let err = mgr
            .drop_user(admin_user.user_id, TEST_ROOT_USER_ID, TEST_ROOT_USER_ID)
            .await
            .unwrap_err();
        assert!(err.to_string().contains("drop default super user"));

        Ok(())
    }

    #[tokio::test]
    async fn test_drop_user_rejects_surviving_role_membership_grantor_dependency() -> MetaResult<()>
    {
        let mgr = CatalogController::new(MetaSrvEnv::for_test().await).await?;
        mgr.create_user(make_test_user("role_a")).await?;
        mgr.create_user(make_test_user("grantor")).await?;
        mgr.create_user(make_test_user("member_a")).await?;

        let role_a = mgr.get_user_by_name("role_a").await?;
        let grantor = mgr.get_user_by_name("grantor").await?;
        let member_a = mgr.get_user_by_name("member_a").await?;

        mgr.grant_role(
            vec![role_a.user_id],
            vec![grantor.user_id],
            TEST_ROOT_USER_ID,
            TEST_ROOT_USER_ID,
            false,
            Some(true),
            None,
            None,
        )
        .await?;
        mgr.grant_role(
            vec![role_a.user_id],
            vec![member_a.user_id],
            grantor.user_id,
            grantor.user_id,
            true,
            None,
            None,
            None,
        )
        .await?;

        let err = mgr
            .drop_user(grantor.user_id, TEST_ROOT_USER_ID, TEST_ROOT_USER_ID)
            .await
            .unwrap_err();
        assert!(err.to_string().contains("role memberships"));

        let memberships = mgr
            .list_role_memberships(&[member_a.user_id], false)
            .await?;
        assert_eq!(memberships.len(), 1);
        assert_eq!(memberships[0].granted_by, grantor.user_id);

        mgr.revoke_role(
            vec![role_a.user_id],
            vec![member_a.user_id],
            TEST_ROOT_USER_ID,
            false,
            TEST_ROOT_USER_ID,
            false,
            false,
            false,
            false,
        )
        .await?;
        mgr.drop_user(grantor.user_id, TEST_ROOT_USER_ID, TEST_ROOT_USER_ID)
            .await?;

        Ok(())
    }

    #[tokio::test]
    async fn test_drop_user_auto_revokes_role_memberships() -> MetaResult<()> {
        let mgr = CatalogController::new(MetaSrvEnv::for_test().await).await?;
        mgr.create_user(make_test_user("role_a")).await?;
        mgr.create_user(make_test_user("member_a")).await?;

        let role_a = mgr.get_user_by_name("role_a").await?;
        let member_a = mgr.get_user_by_name("member_a").await?;

        mgr.grant_role(
            vec![role_a.user_id],
            vec![member_a.user_id],
            TEST_ROOT_USER_ID,
            TEST_ROOT_USER_ID,
            false,
            None,
            None,
            None,
        )
        .await?;
        assert_eq!(mgr.list_role_memberships(&[], true).await?.len(), 1);

        mgr.drop_user(role_a.user_id, TEST_ROOT_USER_ID, TEST_ROOT_USER_ID)
            .await?;

        assert!(mgr.get_user_by_name("role_a").await.is_err());
        assert!(mgr.list_role_memberships(&[], true).await?.is_empty());

        Ok(())
    }
}
