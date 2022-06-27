// Copyright 2022 Singularity Data
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use risingwave_common::catalog::{DEFAULT_SUPPER_USER, DEFAULT_SUPPER_USER_FOR_PG};
use risingwave_common::error::ErrorCode::InternalError;
use risingwave_common::error::{Result, RwError};
use risingwave_pb::meta::subscribe_response::{Info, Operation};
use risingwave_pb::user::grant_privilege::{ActionWithGrantOption, Object};
use risingwave_pb::user::{GrantPrivilege, UserInfo};
use tokio::sync::{Mutex, MutexGuard};

use crate::manager::{MetaSrvEnv, NotificationVersion};
use crate::model::{MetadataModel, Transactional};
use crate::storage::{MetaStore, Transaction};

/// We use `UserName` as the key for the user info.
type UserName = String;

/// `UserManager` managers the user info, including authentication and privileges. It only responds
/// to manager the user info and some basic validation. Other authorization relate to the current
/// session user should be done in Frontend before passing to Meta.
pub struct UserManager<S: MetaStore> {
    env: MetaSrvEnv<S>,
    core: Mutex<UserManagerInner>,
}

pub struct UserManagerInner {
    user_info: HashMap<UserName, UserInfo>,
    user_grant_relation: HashMap<UserName, HashSet<UserName>>,
}

fn get_relation(user_info: &HashMap<UserName, UserInfo>) -> HashMap<UserName, HashSet<UserName>> {
    let mut user_grant_relation: HashMap<UserName, HashSet<UserName>> = HashMap::new();
    for (user_name, info) in user_info {
        for grant_privilege_item in &info.grant_privileges {
            for option in &grant_privilege_item.action_with_opts {
                user_grant_relation
                    .entry(option.get_granted_by().to_string())
                    .or_insert_with(HashSet::new);
                let realtion_item = user_grant_relation
                    .get_mut(option.get_granted_by())
                    .unwrap();
                realtion_item.insert(user_name.clone());
            }
        }
    }
    user_grant_relation
}

impl UserManagerInner {
    pub fn new(user_info: HashMap<UserName, UserInfo>) -> Self {
        let user_grant_relation = get_relation(&user_info);
        Self {
            user_info,
            user_grant_relation,
        }
    }

    pub fn get_user_info(&self) -> &HashMap<UserName, UserInfo> {
        &self.user_info
    }
}

pub type UserInfoManagerRef<S> = Arc<UserManager<S>>;

impl<S: MetaStore> UserManager<S> {
    pub async fn new(env: MetaSrvEnv<S>) -> Result<Self> {
        let users = UserInfo::list(env.meta_store()).await?;
        let user_info = HashMap::from_iter(users.into_iter().map(|user| (user.name.clone(), user)));
        let inner = UserManagerInner::new(user_info);
        let user_manager = Self {
            env,
            core: Mutex::new(inner),
        };
        user_manager.init().await?;
        Ok(user_manager)
    }

    async fn init(&self) -> Result<()> {
        let mut core = self.core.lock().await;
        for user in [DEFAULT_SUPPER_USER, DEFAULT_SUPPER_USER_FOR_PG] {
            if !core.user_info.contains_key(user) {
                let default_user = UserInfo {
                    name: user.to_string(),
                    is_supper: true,
                    can_create_db: true,
                    can_login: true,
                    ..Default::default()
                };

                default_user.insert(self.env.meta_store()).await?;
                core.user_info.insert(user.to_string(), default_user);
            }
        }

        Ok(())
    }

    /// Used in `NotificationService::subscribe`.
    /// Need to pay attention to the order of acquiring locks to prevent deadlock problems.
    pub async fn get_user_core_guard(&self) -> MutexGuard<'_, UserManagerInner> {
        self.core.lock().await
    }

    pub async fn list_users(&self) -> Result<Vec<UserInfo>> {
        let core = self.core.lock().await;
        Ok(core.user_info.values().cloned().collect())
    }

    pub async fn create_user(&self, user: &UserInfo) -> Result<NotificationVersion> {
        let mut core = self.core.lock().await;
        if core.user_info.contains_key(&user.name) {
            return Err(RwError::from(InternalError(format!(
                "User {} already exists",
                user.name
            ))));
        }
        user.insert(self.env.meta_store()).await?;
        core.user_info.insert(user.name.clone(), user.clone());

        let version = self
            .env
            .notification_manager()
            .notify_frontend(Operation::Add, Info::User(user.to_owned()))
            .await;
        Ok(version)
    }

    pub async fn get_user(&self, user_name: &UserName) -> Result<UserInfo> {
        let core = self.core.lock().await;

        core.user_info
            .get(user_name)
            .cloned()
            .ok_or_else(|| RwError::from(InternalError(format!("User {} not found", user_name))))
    }

    pub async fn drop_user(&self, user_name: &UserName) -> Result<NotificationVersion> {
        let mut core = self.core.lock().await;
        if !core.user_info.contains_key(user_name) {
            return Err(RwError::from(InternalError(format!(
                "User {} does not exist",
                user_name
            ))));
        }
        if user_name == DEFAULT_SUPPER_USER || user_name == DEFAULT_SUPPER_USER_FOR_PG {
            return Err(RwError::from(InternalError(format!(
                "Cannot drop default super user {}",
                user_name
            ))));
        }
        if !core
            .user_info
            .get(user_name)
            .unwrap()
            .grant_privileges
            .is_empty()
        {
            return Err(RwError::from(InternalError(format!(
                "Cannot drop user {} with privileges",
                user_name
            ))));
        }
        if core.user_grant_relation.contains_key(user_name)
            && !core.user_grant_relation.get(user_name).unwrap().is_empty()
        {
            return Err(RwError::from(InternalError(format!(
                "Cannot drop user {} with privileges granted to others",
                user_name
            ))));
        }
        // TODO: add more check, like whether he owns any database/schema/table/source.
        UserInfo::delete(self.env.meta_store(), user_name).await?;
        let user = core.user_info.remove(user_name).unwrap();

        let version = self
            .env
            .notification_manager()
            .notify_frontend(Operation::Delete, Info::User(user))
            .await;
        Ok(version)
    }
}

// Defines privilege grant for a user.
impl<S: MetaStore> UserManager<S> {
    // Merge new granted privilege.
    #[inline(always)]
    fn merge_privilege(origin_privilege: &mut GrantPrivilege, new_privilege: &GrantPrivilege) {
        assert_eq!(origin_privilege.object, new_privilege.object);

        let mut action_map = HashMap::<i32, (bool, String)>::from_iter(
            origin_privilege
                .action_with_opts
                .iter()
                .map(|ao| (ao.action, (ao.with_grant_option, ao.granted_by.clone()))),
        );
        for nao in &new_privilege.action_with_opts {
            if let Some(o) = action_map.get_mut(&nao.action) {
                (*o).0 |= nao.with_grant_option;
            } else {
                action_map.insert(nao.action, (nao.with_grant_option, nao.granted_by.clone()));
            }
        }
        origin_privilege.action_with_opts = action_map
            .into_iter()
            .map(
                |(action, (with_grant_option, granted_by))| ActionWithGrantOption {
                    action,
                    with_grant_option,
                    granted_by,
                },
            )
            .collect();
    }

    #[inline(always)]
    fn check_privilege(origin_privilege: &GrantPrivilege, new_privilege: &GrantPrivilege) -> bool {
        assert_eq!(origin_privilege.object, new_privilege.object);

        let action_map = HashMap::<i32, bool>::from_iter(
            origin_privilege
                .action_with_opts
                .iter()
                .map(|ao| (ao.action, ao.with_grant_option)),
        );
        for nao in &new_privilege.action_with_opts {
            if let Some(with_grant_option) = action_map.get(&nao.action) {
                if !with_grant_option {
                    return false;
                }
            } else {
                return false;
            }
        }
        true
    }

    pub async fn grant_privilege(
        &self,
        users: &[UserName],
        new_grant_privileges: &[GrantPrivilege],
        grantor: String,
    ) -> Result<NotificationVersion> {
        let mut core = self.core.lock().await;
        let mut transaction = Transaction::default();
        let mut user_updated = Vec::with_capacity(users.len());
        let grantor_info = core
            .user_info
            .get(&grantor)
            .ok_or_else(|| InternalError(format!("User {} does not exist", &grantor)))
            .cloned()?;
        for user_name in users {
            let mut user = core
                .user_info
                .get(user_name)
                .ok_or_else(|| InternalError(format!("User {} does not exist", user_name)))
                .cloned()?;

            if !core.user_grant_relation.contains_key(&grantor) {
                core.user_grant_relation
                    .insert(grantor.clone(), HashSet::new());
            }
            let grant_user = core.user_grant_relation.get_mut(&grantor).unwrap();

            if user.is_supper {
                return Err(RwError::from(InternalError(format!(
                    "Cannot grant privilege to supper user {}",
                    user_name
                ))));
            }
            if !grantor_info.is_supper {
                for new_grant_privilege in new_grant_privileges {
                    if let Some(privilege) = grantor_info
                        .grant_privileges
                        .iter()
                        .find(|p| p.object == new_grant_privilege.object)
                    {
                        if !Self::check_privilege(privilege, new_grant_privilege) {
                            return Err(RwError::from(InternalError(format!(
                                "Cannot grant privilege without grant permission for user {}",
                                grantor
                            ))));
                        }
                    } else {
                        return Err(RwError::from(InternalError(format!(
                            "Grantor {} do not have one of the privileges",
                            grantor
                        ))));
                    }
                }
            }
            grant_user.insert(user_name.clone());
            new_grant_privileges.iter().for_each(|new_grant_privilege| {
                if let Some(privilege) = user
                    .grant_privileges
                    .iter_mut()
                    .find(|p| p.object == new_grant_privilege.object)
                {
                    Self::merge_privilege(privilege, new_grant_privilege);
                } else {
                    user.grant_privileges.push(new_grant_privilege.clone());
                }
            });
            user.upsert_in_transaction(&mut transaction)?;
            user_updated.push(user);
        }

        self.env.meta_store().txn(transaction).await?;
        let mut version = 0;
        for user in user_updated {
            core.user_info.insert(user.name.clone(), user.clone());
            version = self
                .env
                .notification_manager()
                .notify_frontend(Operation::Update, Info::User(user))
                .await;
        }

        Ok(version)
    }

    // Revoke privilege from object.
    #[inline(always)]
    fn revoke_privilege_inner(
        origin_privilege: &mut GrantPrivilege,
        revoke_grant_privilege: &GrantPrivilege,
        revoke_grant_option: bool,
    ) -> bool {
        assert_eq!(origin_privilege.object, revoke_grant_privilege.object);
        let mut has_change = false;
        if revoke_grant_option {
            // Only revoke with grant option.
            origin_privilege.action_with_opts.iter_mut().for_each(|ao| {
                if revoke_grant_privilege
                    .action_with_opts
                    .iter()
                    .any(|ro| ro.action == ao.action)
                {
                    ao.with_grant_option = false;
                    has_change = true;
                }
            })
        } else {
            let sz = origin_privilege.action_with_opts.len();
            // Revoke all privileges matched with revoke_grant_privilege.
            origin_privilege.action_with_opts.retain(|ao| {
                !revoke_grant_privilege
                    .action_with_opts
                    .iter()
                    .any(|rao| rao.action == ao.action)
            });
            has_change = sz != origin_privilege.action_with_opts.len();
        }
        has_change
    }

    pub async fn revoke_privilege(
        &self,
        users: &[UserName],
        revoke_grant_privileges: &[GrantPrivilege],
        revoke_grant_option: bool,
        cascade: bool,
    ) -> Result<NotificationVersion> {
        let mut core = self.core.lock().await;
        let mut transaction = Transaction::default();
        let mut user_updated = HashMap::new();
        let mut users_info = Vec::new();
        let mut visited = HashSet::new();
        for user_name in users {
            let user = core
                .user_info
                .get(user_name)
                .ok_or_else(|| InternalError(format!("User {} does not exist", user_name)))
                .cloned()?;
            if !core.user_grant_relation.contains_key(user_name) {
                core.user_grant_relation
                    .insert(user_name.clone(), HashSet::new());
            }
            if user.is_supper {
                return Err(RwError::from(InternalError(format!(
                    "Cannot revoke privilege from supper user {}",
                    user_name
                ))));
            }
            users_info.push(user.clone());
        }
        while !users_info.is_empty() {
            let mut now_user = users_info.first_mut().unwrap().clone();
            if !core.user_grant_relation.contains_key(&now_user.name) {
                core.user_grant_relation
                    .insert(now_user.name.clone(), HashSet::new());
            }
            let now_relations = core.user_grant_relation.get(&now_user.name).unwrap();
            let mut recursive_flag = false;
            let mut empty_privilege = false;
            let grant_option_now = revoke_grant_option && users.contains(&now_user.name);
            users_info.remove(0);
            visited.insert(now_user.name.clone());
            revoke_grant_privileges
                .iter()
                .for_each(|revoke_grant_privilege| {
                    for privilege in &mut now_user.grant_privileges {
                        if privilege.object == revoke_grant_privilege.object {
                            recursive_flag |= Self::revoke_privilege_inner(
                                privilege,
                                revoke_grant_privilege,
                                grant_option_now,
                            );
                            empty_privilege |= privilege.action_with_opts.is_empty();
                            break;
                        }
                    }
                });
            if recursive_flag {
                // check with cascade/restrict strategy
                if !cascade && !users.contains(&now_user.name) {
                    return Err(RwError::from(InternalError(format!(
                        "Cannot revoke privilege from user {} for restrict",
                        &now_user.name
                    ))));
                }
                for next_user_name in now_relations {
                    if core.user_info.contains_key(next_user_name)
                        && !visited.contains(next_user_name)
                    {
                        users_info.push(core.user_info.get(next_user_name).unwrap().clone());
                    }
                }
                if empty_privilege {
                    now_user
                        .grant_privileges
                        .retain(|privilege| !privilege.action_with_opts.is_empty());
                }
                if !user_updated.contains_key(&now_user.name) {
                    now_user.upsert_in_transaction(&mut transaction)?;
                    user_updated.insert(now_user.name.clone(), now_user);
                }
            }
        }

        self.env.meta_store().txn(transaction).await?;
        let mut version = 0;
        for (user_name, user_info) in user_updated {
            core.user_info.insert(user_name.clone(), user_info.clone());
            version = self
                .env
                .notification_manager()
                .notify_frontend(Operation::Update, Info::User(user_info))
                .await;
        }

        Ok(version)
    }

    /// `release_privileges` removes the privileges with given object from all users, it will be
    /// called when a database/schema/table/source is dropped.
    pub async fn release_privileges(&self, object: &Object) -> Result<()> {
        let mut core = self.core.lock().await;
        let mut transaction = Transaction::default();
        let mut users_need_update = vec![];
        for user in core.user_info.values() {
            let cnt = user.grant_privileges.len();
            let mut user = user.clone();
            user.grant_privileges
                .retain(|p| p.object.as_ref().unwrap() != object);
            if cnt != user.grant_privileges.len() {
                user.upsert_in_transaction(&mut transaction)?;
                users_need_update.push(user);
            }
        }

        self.env.meta_store().txn(transaction).await?;
        for user in users_need_update {
            core.user_info.insert(user.name.clone(), user.clone());
            self.env
                .notification_manager()
                .notify_frontend(Operation::Update, Info::User(user))
                .await;
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use risingwave_pb::user::grant_privilege::Action;

    use super::*;

    fn make_test_user(name: &str) -> UserInfo {
        UserInfo {
            name: name.to_string(),
            ..Default::default()
        }
    }

    fn make_privilege(
        object: Object,
        actions: &[Action],
        with_grant_option: bool,
    ) -> GrantPrivilege {
        GrantPrivilege {
            object: Some(object),
            action_with_opts: actions
                .iter()
                .map(|&action| ActionWithGrantOption {
                    action: action as i32,
                    with_grant_option,
                    granted_by: DEFAULT_SUPPER_USER.to_string(),
                })
                .collect(),
        }
    }

    #[tokio::test]
    async fn test_user_manager() -> Result<()> {
        let user_manager = UserManager::new(MetaSrvEnv::for_test().await).await?;
        let test_user = "test_user";
        let test_sub_user = "test_sub_user";
        user_manager.create_user(&make_test_user(test_user)).await?;
        user_manager
            .create_user(&make_test_user(test_sub_user))
            .await?;
        assert!(user_manager
            .create_user(&make_test_user(DEFAULT_SUPPER_USER))
            .await
            .is_err());

        let users = user_manager.list_users().await?;
        assert_eq!(users.len(), 4);

        let object = Object::TableId(0);
        // Grant when grantor does not have privilege.
        let res = user_manager
            .grant_privilege(
                &[test_sub_user.to_string()],
                &[make_privilege(
                    object.clone(),
                    &[Action::Select, Action::Update, Action::Delete],
                    true,
                )],
                test_user.to_string(),
            )
            .await;
        assert!(res.is_err());
        let sub_user = user_manager.get_user(&test_sub_user.to_string()).await?;
        assert_eq!(sub_user.grant_privileges.len(), 0);
        // Grant Select/Insert without grant option.
        user_manager
            .grant_privilege(
                &[test_user.to_string()],
                &[make_privilege(
                    object.clone(),
                    &[Action::Select, Action::Insert],
                    false,
                )],
                DEFAULT_SUPPER_USER.to_string(),
            )
            .await?;
        let user = user_manager.get_user(&test_user.to_string()).await?;
        assert_eq!(user.grant_privileges.len(), 1);
        assert_eq!(user.grant_privileges[0].object, Some(object.clone()));
        assert_eq!(user.grant_privileges[0].action_with_opts.len(), 2);
        assert!(user.grant_privileges[0]
            .action_with_opts
            .iter()
            .all(|p| !p.with_grant_option));
        // Grant when grantor does not have privilege's grant option.
        let res = user_manager
            .grant_privilege(
                &[test_sub_user.to_string()],
                &[make_privilege(
                    object.clone(),
                    &[Action::Select, Action::Insert],
                    true,
                )],
                test_user.to_string(),
            )
            .await;
        assert!(res.is_err());
        let sub_user = user_manager.get_user(&test_sub_user.to_string()).await?;
        assert_eq!(sub_user.grant_privileges.len(), 0);
        // Grant Select/Insert with grant option.
        user_manager
            .grant_privilege(
                &[test_user.to_string()],
                &[make_privilege(
                    object.clone(),
                    &[Action::Select, Action::Insert],
                    true,
                )],
                DEFAULT_SUPPER_USER.to_string(),
            )
            .await?;
        let user = user_manager.get_user(&test_user.to_string()).await?;
        assert_eq!(user.grant_privileges.len(), 1);
        assert_eq!(user.grant_privileges[0].object, Some(object.clone()));
        assert_eq!(user.grant_privileges[0].action_with_opts.len(), 2);
        assert!(user.grant_privileges[0]
            .action_with_opts
            .iter()
            .all(|p| p.with_grant_option));
        // Grant to subuser
        let res = user_manager
            .grant_privilege(
                &[test_sub_user.to_string()],
                &[make_privilege(
                    object.clone(),
                    &[Action::Select, Action::Insert],
                    true,
                )],
                test_user.to_string(),
            )
            .await;
        assert!(res.is_ok());
        let sub_user = user_manager.get_user(&test_sub_user.to_string()).await?;
        assert_eq!(sub_user.grant_privileges.len(), 1);
        // Grant Select/Update/Delete with grant option, while Select is duplicated.
        user_manager
            .grant_privilege(
                &[test_user.to_string()],
                &[make_privilege(
                    object.clone(),
                    &[Action::Select, Action::Update, Action::Delete],
                    true,
                )],
                DEFAULT_SUPPER_USER.to_string(),
            )
            .await?;
        let user = user_manager.get_user(&test_user.to_string()).await?;
        assert_eq!(user.grant_privileges.len(), 1);
        assert_eq!(user.grant_privileges[0].object, Some(object.clone()));
        assert_eq!(user.grant_privileges[0].action_with_opts.len(), 4);
        assert!(user.grant_privileges[0]
            .action_with_opts
            .iter()
            .all(|p| p.with_grant_option));

        // Revoke with restrict
        let res = user_manager
            .revoke_privilege(
                &[test_user.to_string()],
                &[make_privilege(
                    object.clone(),
                    &[
                        Action::Select,
                        Action::Insert,
                        Action::Delete,
                        Action::Update,
                    ],
                    false,
                )],
                true,
                false,
            )
            .await;
        assert!(res.is_err());
        let sub_user = user_manager.get_user(&test_sub_user.to_string()).await?;
        assert_eq!(sub_user.grant_privileges.len(), 1);
        let user = user_manager.get_user(&test_user.to_string()).await?;
        assert_eq!(user.grant_privileges[0].action_with_opts.len(), 4);
        // Revoke Select/Update/Delete/Insert with grant option.
        user_manager
            .revoke_privilege(
                &[test_user.to_string()],
                &[make_privilege(
                    object.clone(),
                    &[
                        Action::Select,
                        Action::Insert,
                        Action::Delete,
                        Action::Update,
                    ],
                    false,
                )],
                true,
                true,
            )
            .await?;
        let user = user_manager.get_user(&test_user.to_string()).await?;
        assert_eq!(user.grant_privileges[0].action_with_opts.len(), 4);
        assert!(user.grant_privileges[0]
            .action_with_opts
            .iter()
            .all(|p| !p.with_grant_option));
        let sub_user = user_manager.get_user(&test_sub_user.to_string()).await?;
        assert_eq!(sub_user.grant_privileges.len(), 0);
        // Revoke Select/Delete/Insert.
        user_manager
            .revoke_privilege(
                &[test_user.to_string()],
                &[make_privilege(
                    object.clone(),
                    &[Action::Select, Action::Insert, Action::Delete],
                    false,
                )],
                false,
                true,
            )
            .await?;
        let user = user_manager.get_user(&test_user.to_string()).await?;
        assert_eq!(user.grant_privileges.len(), 1);
        assert_eq!(user.grant_privileges[0].action_with_opts.len(), 1);

        // Release all privileges with object.
        user_manager.release_privileges(&object).await?;
        let user = user_manager.get_user(&test_user.to_string()).await?;
        assert!(user.grant_privileges.is_empty());

        Ok(())
    }
}
