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

use std::collections::hash_map::Entry;
use std::collections::{HashMap, HashSet};

use risingwave_pb::user::update_user_request::UpdateField;
use risingwave_pb::user::UserInfo;

use super::UserId;
use crate::manager::MetaSrvEnv;
use crate::model::MetadataModel;
use crate::storage::MetaStore;
use crate::MetaResult;

pub struct UserManager {
    user_info: HashMap<UserId, UserInfo>,
    user_grant_relation: HashMap<UserId, HashSet<UserId>>,
    all_users: HashSet<String>,
}

fn get_relation(user_info: &HashMap<UserId, UserInfo>) -> HashMap<UserId, HashSet<UserId>> {
    let mut user_grant_relation: HashMap<UserId, HashSet<UserId>> = HashMap::new();
    for (user_id, info) in user_info {
        for grant_privilege_item in &info.grant_privileges {
            for option in &grant_privilege_item.action_with_opts {
                user_grant_relation
                    .entry(option.get_granted_by())
                    .or_insert_with(HashSet::new)
                    .insert(*user_id);
            }
        }
    }
    user_grant_relation
}

impl UserManager {
    pub async fn new<S: MetaStore>(env: MetaSrvEnv<S>) -> MetaResult<Self> {
        let users = UserInfo::list(env.meta_store()).await?;
        let all_users = HashSet::from_iter(users.iter().map(|user| user.name.clone()));
        let user_info = HashMap::from_iter(users.into_iter().map(|user| (user.id, user)));
        let user_grant_relation = get_relation(&user_info);
        Ok(Self {
            user_info,
            user_grant_relation,
            all_users,
        })
    }

    pub fn create_user(&mut self, user: UserInfo) {
        self.all_users.insert(user.name.clone());
        self.user_info.insert(user.id, user);
    }

    pub fn update_user(
        &mut self,
        update_user: &UserInfo,
        update_fields: &[UpdateField],
    ) -> UserInfo {
        let mut user = self.user_info.get(&update_user.id).unwrap().clone();
        update_fields.iter().for_each(|&field| match field {
            UpdateField::Unspecified => unreachable!(),
            UpdateField::Super => user.is_super = update_user.is_super,
            UpdateField::Login => user.can_login = update_user.can_login,
            UpdateField::CreateDb => user.can_create_db = update_user.can_create_db,
            UpdateField::CreateUser => user.can_create_user = update_user.can_create_user,
            UpdateField::AuthInfo => user.auth_info = update_user.auth_info.clone(),
            UpdateField::Rename => {
                self.all_users.remove(&user.name);
                user.name = update_user.name.clone();
                self.all_users.insert(update_user.name.clone());
            }
        });

        self.user_info.insert(update_user.id, user.clone());
        user
    }

    pub fn drop_user(&mut self, user_id: UserId) {
        // user in user_grant_relation (as key or value) are already checked before entering this
        // function.
        if let Some(user) = self.user_info.remove(&user_id) {
            self.all_users.remove(&user.name);
        }
    }

    pub fn list_users(&self) -> Vec<UserInfo> {
        self.user_info.values().cloned().collect()
    }

    pub fn insert_user_info(&mut self, user_id: UserId, user_info: UserInfo) {
        self.user_info.insert(user_id, user_info);
    }

    pub fn get_user_info(&self, user_id: &UserId) -> Option<UserInfo> {
        self.user_info.get(user_id).cloned()
    }

    pub fn get_user_grant_relation(&self, user_id: &UserId) -> Option<&HashSet<UserId>> {
        self.user_grant_relation.get(user_id)
    }

    pub fn get_user_grant_relation_entry(
        &mut self,
        user_id: UserId,
    ) -> Entry<'_, UserId, HashSet<u32>> {
        self.user_grant_relation.entry(user_id)
    }

    pub fn has_user_id(&self, id: &UserId) -> bool {
        self.user_info.contains_key(id)
    }

    pub fn has_user_name(&self, user: &str) -> bool {
        self.all_users.contains(user)
    }
}

#[cfg(test)]
mod tests {
    use risingwave_common::catalog::{DEFAULT_SUPER_USER, DEFAULT_SUPER_USER_ID};
    use risingwave_pb::user::grant_privilege::{Action, ActionWithGrantOption, Object};
    use risingwave_pb::user::GrantPrivilege;

    use super::*;
    use crate::manager::CatalogManager;
    use crate::storage::{MemStore, Transaction};

    fn make_test_user(id: u32, name: &str) -> UserInfo {
        UserInfo {
            id,
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
                    granted_by: DEFAULT_SUPER_USER_ID,
                })
                .collect(),
        }
    }

    #[tokio::test]
    async fn test_catalog_manager() -> MetaResult<()> {
        let catalog_manager = CatalogManager::new(MetaSrvEnv::for_test().await).await?;
        let (test_user_id, test_user) = (10, "test_user");

        let (test_sub_user_id, test_sub_user) = (11, "test_sub_user");
        catalog_manager
            .create_user(&make_test_user(test_user_id, test_user))
            .await?;
        catalog_manager
            .create_user(&make_test_user(test_sub_user_id, test_sub_user))
            .await?;
        assert!(catalog_manager
            .create_user(&make_test_user(DEFAULT_SUPER_USER_ID, DEFAULT_SUPER_USER))
            .await
            .is_err());

        let users = catalog_manager.list_users().await;
        assert_eq!(users.len(), 4);

        let object = Object::TableId(0);
        let other_object = Object::TableId(1);
        // Grant when grantor does not have privilege.
        let res = catalog_manager
            .grant_privilege(
                &[test_sub_user_id],
                &[make_privilege(
                    object.clone(),
                    &[Action::Select, Action::Update, Action::Delete],
                    true,
                )],
                test_user_id,
            )
            .await;
        assert!(res.is_err());
        let sub_user = catalog_manager.get_user(test_sub_user_id).await?;
        assert_eq!(sub_user.grant_privileges.len(), 0);
        // Grant Select/Insert without grant option.
        catalog_manager
            .grant_privilege(
                &[test_user_id],
                &[make_privilege(
                    object.clone(),
                    &[Action::Select, Action::Insert],
                    false,
                )],
                DEFAULT_SUPER_USER_ID,
            )
            .await?;
        let user = catalog_manager.get_user(test_user_id).await?;
        assert_eq!(user.grant_privileges.len(), 1);
        assert_eq!(user.grant_privileges[0].object, Some(object.clone()));
        assert_eq!(user.grant_privileges[0].action_with_opts.len(), 2);
        assert!(user.grant_privileges[0]
            .action_with_opts
            .iter()
            .all(|p| !p.with_grant_option));
        // Grant when grantor does not have privilege's grant option.
        let res = catalog_manager
            .grant_privilege(
                &[test_sub_user_id],
                &[make_privilege(
                    object.clone(),
                    &[Action::Select, Action::Insert],
                    true,
                )],
                test_user_id,
            )
            .await;
        assert!(res.is_err());
        let sub_user = catalog_manager.get_user(test_sub_user_id).await?;
        assert_eq!(sub_user.grant_privileges.len(), 0);
        // Grant Select/Insert with grant option.
        catalog_manager
            .grant_privilege(
                &[test_user_id],
                &[make_privilege(
                    object.clone(),
                    &[Action::Select, Action::Insert],
                    true,
                )],
                DEFAULT_SUPER_USER_ID,
            )
            .await?;
        let user = catalog_manager.get_user(test_user_id).await?;
        assert_eq!(user.grant_privileges.len(), 1);
        assert_eq!(user.grant_privileges[0].object, Some(object.clone()));
        assert_eq!(user.grant_privileges[0].action_with_opts.len(), 2);
        assert!(user.grant_privileges[0]
            .action_with_opts
            .iter()
            .all(|p| p.with_grant_option));
        // Grant to subuser
        let res = catalog_manager
            .grant_privilege(
                &[test_sub_user_id],
                &[make_privilege(
                    object.clone(),
                    &[Action::Select, Action::Insert],
                    true,
                )],
                test_user_id,
            )
            .await;
        assert!(res.is_ok());
        let sub_user = catalog_manager.get_user(test_sub_user_id).await?;
        assert_eq!(sub_user.grant_privileges.len(), 1);
        // Grant Select/Update/Delete with grant option, while Select is duplicated.
        catalog_manager
            .grant_privilege(
                &[test_user_id],
                &[make_privilege(
                    object.clone(),
                    &[Action::Select, Action::Update, Action::Delete],
                    true,
                )],
                DEFAULT_SUPER_USER_ID,
            )
            .await?;
        let user = catalog_manager.get_user(test_user_id).await?;
        assert_eq!(user.grant_privileges.len(), 1);
        assert_eq!(user.grant_privileges[0].object, Some(object.clone()));
        assert_eq!(user.grant_privileges[0].action_with_opts.len(), 4);
        assert!(user.grant_privileges[0]
            .action_with_opts
            .iter()
            .all(|p| p.with_grant_option));

        // Revoke without privilege action
        let res = catalog_manager
            .revoke_privilege(
                &[test_user_id],
                &[make_privilege(object.clone(), &[Action::Connect], false)],
                0,
                test_sub_user_id,
                true,
                false,
            )
            .await;
        assert!(res.is_err());
        let sub_user = catalog_manager.get_user(test_sub_user_id).await?;
        assert_eq!(sub_user.grant_privileges.len(), 1);
        let user = catalog_manager.get_user(test_user_id).await?;
        assert_eq!(user.grant_privileges[0].action_with_opts.len(), 4);
        // Revoke without privilege object
        let res = catalog_manager
            .revoke_privilege(
                &[test_user_id],
                &[make_privilege(
                    other_object.clone(),
                    &[Action::Connect],
                    false,
                )],
                0,
                test_sub_user_id,
                true,
                false,
            )
            .await;
        assert!(res.is_err());
        let sub_user = catalog_manager.get_user(test_sub_user_id).await?;
        assert_eq!(sub_user.grant_privileges.len(), 1);
        let user = catalog_manager.get_user(test_user_id).await?;
        assert_eq!(user.grant_privileges[0].action_with_opts.len(), 4);
        // Revoke with restrict
        let res = catalog_manager
            .revoke_privilege(
                &[test_user_id],
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
                0,
                DEFAULT_SUPER_USER_ID,
                true,
                false,
            )
            .await;
        assert!(res.is_err());
        let sub_user = catalog_manager.get_user(test_sub_user_id).await?;
        assert_eq!(sub_user.grant_privileges.len(), 1);
        let user = catalog_manager.get_user(test_user_id).await?;
        assert_eq!(user.grant_privileges[0].action_with_opts.len(), 4);
        // Revoke Select/Update/Delete/Insert with grant option.
        catalog_manager
            .revoke_privilege(
                &[test_user_id],
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
                0,
                DEFAULT_SUPER_USER_ID,
                true,
                true,
            )
            .await?;
        let user = catalog_manager.get_user(test_user_id).await?;
        assert_eq!(user.grant_privileges[0].action_with_opts.len(), 4);
        assert!(user.grant_privileges[0]
            .action_with_opts
            .iter()
            .all(|p| !p.with_grant_option));
        let sub_user = catalog_manager.get_user(test_sub_user_id).await?;
        assert_eq!(sub_user.grant_privileges.len(), 0);
        // Revoke Select/Delete/Insert.
        catalog_manager
            .revoke_privilege(
                &[test_user_id],
                &[make_privilege(
                    object.clone(),
                    &[Action::Select, Action::Insert, Action::Delete],
                    false,
                )],
                0,
                DEFAULT_SUPER_USER_ID,
                false,
                true,
            )
            .await?;
        let user = catalog_manager.get_user(test_user_id).await?;
        assert_eq!(user.grant_privileges.len(), 1);
        assert_eq!(user.grant_privileges[0].action_with_opts.len(), 1);

        // Release all privileges with object.
        let mut txn = Transaction::default();
        let users_need_update = CatalogManager::<MemStore>::release_privileges(
            catalog_manager.list_users().await,
            &[object],
            &mut txn,
        )?;
        catalog_manager.env.meta_store().txn(txn).await?;
        for user in users_need_update {
            catalog_manager
                .core
                .lock()
                .await
                .user
                .insert_user_info(user.id, user);
        }
        let user = catalog_manager.get_user(test_user_id).await?;
        assert!(user.grant_privileges.is_empty());

        Ok(())
    }
}
