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

use std::collections::hash_map::Entry;
use std::collections::{BTreeMap, HashMap, HashSet};

use anyhow::anyhow;
use risingwave_pb::user::UserInfo;

use super::database::DatabaseManager;
use super::UserId;
use crate::manager::MetaSrvEnv;
use crate::model::MetadataModel;
use crate::storage::MetaStore;
use crate::MetaResult;

pub struct UserManager {
    pub(super) user_info: BTreeMap<UserId, UserInfo>,
    /// The mapping from privilege grantor to their granted users.
    pub(super) user_grant_relation: HashMap<UserId, HashSet<UserId>>,
    /// The number of catalog whose owner is the user.
    pub(super) catalog_create_ref_count: HashMap<UserId, usize>,
}

impl UserManager {
    pub async fn new<S: MetaStore>(
        env: MetaSrvEnv<S>,
        database: &DatabaseManager,
    ) -> MetaResult<Self> {
        let users = UserInfo::list(env.meta_store()).await?;
        let user_info = BTreeMap::from_iter(users.into_iter().map(|user| (user.id, user)));

        let mut user_manager = Self {
            user_info,
            user_grant_relation: HashMap::new(),
            catalog_create_ref_count: HashMap::new(),
        };
        user_manager.build_grant_relation_map();

        database
            .databases
            .values()
            .map(|database| database.owner)
            .chain(database.schemas.values().map(|schema| schema.owner))
            .chain(database.sources.values().map(|source| source.owner))
            .chain(database.sinks.values().map(|sink| sink.owner))
            .chain(database.indexes.values().map(|index| index.owner))
            .chain(database.tables.values().map(|table| table.owner))
            .chain(database.views.values().map(|view| view.owner))
            .for_each(|owner_id| user_manager.increase_ref(owner_id));

        Ok(user_manager)
    }

    pub fn list_users(&self) -> Vec<UserInfo> {
        self.user_info.values().cloned().collect()
    }

    pub fn has_user_name(&self, user: &str) -> bool {
        self.user_info.values().any(|x| x.name.eq(user))
    }

    #[allow(dead_code)]
    pub fn ensure_user_id(&self, user_id: UserId) -> MetaResult<()> {
        if self.user_info.contains_key(&user_id) {
            Ok(())
        } else {
            Err(anyhow!("user {} was concurrently dropped", user_id).into())
        }
    }

    /// Build grant relation map from exist user infos.
    pub fn build_grant_relation_map(&mut self) {
        self.user_grant_relation.clear();
        for (user_id, info) in &self.user_info {
            for grant_privilege_item in &info.grant_privileges {
                for option in &grant_privilege_item.action_with_opts {
                    self.user_grant_relation
                        .entry(option.get_granted_by())
                        .or_insert_with(HashSet::new)
                        .insert(*user_id);
                }
            }
        }
    }

    #[inline(always)]
    pub fn increase_ref(&mut self, user_id: UserId) {
        self.increase_ref_count(user_id, 1)
    }

    pub fn increase_ref_count(&mut self, user_id: UserId, count: usize) {
        self.catalog_create_ref_count
            .entry(user_id)
            .and_modify(|c| *c += count)
            .or_insert(count);
    }

    #[inline(always)]
    pub fn decrease_ref(&mut self, user_id: UserId) {
        self.decrease_ref_count(user_id, 1)
    }

    pub fn decrease_ref_count(&mut self, user_id: UserId, count: usize) {
        match self.catalog_create_ref_count.entry(user_id) {
            Entry::Occupied(mut o) => {
                assert!(*o.get_mut() >= count);
                *o.get_mut() -= count;
                if *o.get() == 0 {
                    o.remove_entry();
                }
            }
            Entry::Vacant(_) => unreachable!(),
        }
    }
}

#[cfg(test)]
mod tests {
    use risingwave_common::catalog::{DEFAULT_SUPER_USER, DEFAULT_SUPER_USER_ID};
    use risingwave_pb::user::grant_privilege::{Action, ActionWithGrantOption, Object};
    use risingwave_pb::user::GrantPrivilege;

    use super::*;
    use crate::manager::{commit_meta, CatalogManager};
    use crate::model::{BTreeMapTransaction, ValTransaction};
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
        let user_core = &mut catalog_manager.core.lock().await.user;
        let mut users = BTreeMapTransaction::new(&mut user_core.user_info);
        CatalogManager::<MemStore>::update_user_privileges(&mut users, &[object]);
        commit_meta!(catalog_manager, users)?;
        let user = user_core.user_info.get(&test_user_id).unwrap();
        assert!(user.grant_privileges.is_empty());

        Ok(())
    }
}
