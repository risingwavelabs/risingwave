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

use std::collections::{HashMap, HashSet};

use itertools::Itertools;
use risingwave_common::catalog::{DEFAULT_SUPER_USER, DEFAULT_SUPER_USER_FOR_PG, SYSTEM_SCHEMAS};
use risingwave_meta_model::object::ObjectType;
use risingwave_meta_model::prelude::{Object, Schema, User, UserDefaultPrivilege, UserPrivilege};
use risingwave_meta_model::user_privilege::Action;
use risingwave_meta_model::{
    AuthInfo, DatabaseId, PrivilegeId, SchemaId, UserId, object, schema, user,
    user_default_privilege, user_privilege,
};
use risingwave_pb::common::PbObjectType;
use risingwave_pb::meta::subscribe_response::{
    Info as NotificationInfo, Operation as NotificationOperation,
};
use risingwave_pb::user::update_user_request::PbUpdateField;
use risingwave_pb::user::{PbAction, PbGrantPrivilege, PbUserInfo};
use sea_orm::ActiveValue::Set;
use sea_orm::sea_query::{OnConflict, SimpleExpr, Value};
use sea_orm::{
    ActiveModelTrait, ColumnTrait, EntityTrait, IntoActiveModel, PaginatorTrait, QueryFilter,
    QuerySelect, TransactionTrait,
};

use crate::controller::catalog::CatalogController;
use crate::controller::utils::{
    PartialUserPrivilege, check_user_name_duplicate, ensure_object_id,
    ensure_privileges_not_referred, ensure_user_id, extract_grant_obj_id,
    get_index_state_tables_by_table_id, get_internal_tables_by_id, get_object_owner,
    get_referring_privileges_cascade, get_user_privilege, list_user_info_by_ids,
};
use crate::manager::{IGNORED_NOTIFICATION_VERSION, NotificationVersion};
use crate::{MetaError, MetaResult};

impl CatalogController {
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
        let inner = self.inner.write().await;
        let txn = inner.db.begin().await?;
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
                        granted_by: Set(action_with_opt.granted_by as _),
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
        let inner = self.inner.write().await;
        let rename_flag = update_fields.contains(&PbUpdateField::Rename);
        if rename_flag {
            check_user_name_duplicate(&update_user.name, &inner.db).await?;
        }

        let user = User::find_by_id(update_user.id as UserId)
            .one(&inner.db)
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
        });

        let user = user.update(&inner.db).await?;
        let mut user_info: PbUserInfo = user.into();
        user_info.grant_privileges = get_user_privilege(user_info.id as _, &inner.db).await?;
        let version = self
            .notify_frontend(
                NotificationOperation::Update,
                NotificationInfo::User(user_info),
            )
            .await;

        Ok(version)
    }

    #[cfg(test)]
    pub async fn get_user(&self, id: UserId) -> MetaResult<user::Model> {
        let inner = self.inner.read().await;
        let user = User::find_by_id(id)
            .one(&inner.db)
            .await?
            .ok_or_else(|| MetaError::catalog_id_not_found("user", id))?;
        Ok(user)
    }

    #[cfg(test)]
    pub async fn get_user_by_name(&self, name: &str) -> MetaResult<user::Model> {
        let inner = self.inner.read().await;
        let user = User::find()
            .filter(user::Column::Name.eq(name))
            .one(&inner.db)
            .await?
            .ok_or_else(|| anyhow::anyhow!("user {name} not found"))?;
        Ok(user)
    }

    pub async fn drop_user(&self, user_id: UserId) -> MetaResult<NotificationVersion> {
        let inner = self.inner.write().await;
        let txn = inner.db.begin().await?;
        let user = User::find_by_id(user_id)
            .one(&txn)
            .await?
            .ok_or_else(|| MetaError::catalog_id_not_found("user", user_id))?;
        if user.name == DEFAULT_SUPER_USER || user.name == DEFAULT_SUPER_USER_FOR_PG {
            return Err(MetaError::permission_denied(format!(
                "drop default super user {} is not allowed",
                user.name
            )));
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

        let res = User::delete_by_id(user_id).exec(&txn).await?;
        if res.rows_affected != 1 {
            return Err(MetaError::catalog_id_not_found("user", user_id));
        }
        txn.commit().await?;

        let version = self
            .notify_frontend(
                NotificationOperation::Delete,
                NotificationInfo::User(PbUserInfo {
                    id: user_id as _,
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
        let inner = self.inner.write().await;
        let txn = inner.db.begin().await?;
        for user_id in &user_ids {
            ensure_user_id(*user_id, &txn).await?;
        }

        let mut privileges = vec![];
        for gp in new_grant_privileges {
            let id = extract_grant_obj_id(gp.get_object()?);
            let internal_table_ids = get_internal_tables_by_id(id, &txn).await?;
            let index_state_table_ids = get_index_state_tables_by_table_id(id, &txn).await?;
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
                                oid: Set(tid),
                                granted_by: Set(grantor),
                                action: Set(Action::Select),
                                with_grant_option: Set(with_grant_option),
                                ..Default::default()
                            }),
                    );
                }
            }
        }

        // check whether grantor has the privilege to grant the privilege.
        let user = User::find_by_id(grantor)
            .one(&txn)
            .await?
            .ok_or_else(|| MetaError::catalog_id_not_found("user", grantor))?;
        let mut filtered_privileges = vec![];
        if !user.is_super {
            for mut privilege in privileges {
                if grantor == get_object_owner(*privilege.oid.as_ref(), &txn).await? {
                    filtered_privileges.push(privilege);
                    continue;
                }
                let filter = user_privilege::Column::UserId
                    .eq(grantor)
                    .and(user_privilege::Column::Oid.eq(*privilege.oid.as_ref()))
                    .and(user_privilege::Column::Action.eq(*privilege.action.as_ref()))
                    .and(user_privilege::Column::WithGrantOption.eq(true));
                let privilege_id: Option<PrivilegeId> = UserPrivilege::find()
                    .select_only()
                    .column(user_privilege::Column::Id)
                    .filter(filter)
                    .into_tuple()
                    .one(&txn)
                    .await?;
                let Some(privilege_id) = privilege_id else {
                    tracing::warn!(
                        "user {} don't have privilege {:?} or grant option",
                        grantor,
                        privilege.action
                    );
                    continue;
                };
                privilege.dependent_id = Set(Some(privilege_id));
                filtered_privileges.push(privilege);
            }
        } else {
            filtered_privileges = privileges;
        }

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
                on_conflict.update_columns([user_privilege::Column::WithGrantOption]);
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
        let inner = self.inner.write().await;
        let txn = inner.db.begin().await?;
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
            let internal_table_ids = get_internal_tables_by_id(obj, &txn).await?;
            let index_state_table_ids = get_index_state_tables_by_table_id(obj, &txn).await?;
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
                        .map(|&tid| (tid, vec![Action::Select])),
                );
            }
        }

        let filter = if !revoke_user.is_super {
            // ensure user have grant options or is owner of the object.
            for (obj, actions) in &revoke_items {
                if revoke_by == get_object_owner(*obj, &txn).await? {
                    continue;
                }
                let owned_actions: HashSet<Action> = UserPrivilege::find()
                    .select_only()
                    .column(user_privilege::Column::Action)
                    .filter(
                        user_privilege::Column::UserId
                            .eq(granted_by)
                            .and(user_privilege::Column::Oid.eq(*obj))
                            .and(user_privilege::Column::WithGrantOption.eq(true)),
                    )
                    .into_tuple::<Action>()
                    .all(&txn)
                    .await?
                    .into_iter()
                    .collect();
                if actions.iter().any(|ac| !owned_actions.contains(ac)) {
                    return Err(MetaError::permission_denied(format!(
                        "user {} don't have privileges {:?} or grant option",
                        revoke_user.name, actions,
                    )));
                }
            }

            user_privilege::Column::GrantedBy
                .eq(granted_by)
                .and(user_privilege::Column::UserId.is_in(user_ids.clone()))
        } else {
            user_privilege::Column::UserId.is_in(user_ids.clone())
        };
        let mut root_user_privileges: Vec<PartialUserPrivilege> = vec![];
        for (obj, actions) in revoke_items {
            let filter = filter
                .clone()
                .and(user_privilege::Column::Oid.eq(obj))
                .and(user_privilege::Column::Action.is_in(actions));
            root_user_privileges.extend(
                UserPrivilege::find()
                    .select_only()
                    .columns([user_privilege::Column::Id, user_privilege::Column::UserId])
                    .filter(filter)
                    .into_partial_model()
                    .all(&txn)
                    .await?,
            );
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
        let inner = self.inner.write().await;
        let txn = inner.db.begin().await?;
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
        if schema_ids.is_empty() {
            assert_eq!(
                object_type,
                PbObjectType::Schema,
                "object type must be Schema when schema_ids is empty"
            );
        }
        // TODO: add a new column to distinguish the object type between
        // table and materialized view.

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
                            user_id: Set(user_id),
                            grantee: Set(*grantee),
                            granted_by: Set(grantor as _),
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
                            user_id: Set(user_id),
                            grantee: Set(*grantee),
                            granted_by: Set(grantor as _),
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

        txn.commit().await?;
        Ok(())
    }

    pub async fn revoke_default_privileges(
        &self,
        user_ids: Vec<UserId>,
        database_id: DatabaseId,
        schema_ids: Vec<SchemaId>,
        grantor: UserId,
        actions: Vec<PbAction>,
        object_type: PbObjectType,
        grantees: Vec<UserId>,
        revoke_grant_option: bool,
    ) -> MetaResult<()> {
        let inner = self.inner.write().await;
        let txn = inner.db.begin().await?;
        for user_id in &user_ids {
            ensure_user_id(*user_id, &txn).await?;
        }
        todo!("Implement default privileges for users")
    }
}

#[cfg(test)]
mod tests {
    use risingwave_pb::user::grant_privilege::{PbActionWithGrantOption, PbObject};

    use super::*;
    use crate::manager::MetaSrvEnv;

    const TEST_DATABASE_ID: DatabaseId = 1;
    const TEST_ROOT_USER_ID: UserId = 1;

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

        assert!(
            mgr.create_user(make_test_user("test_user_1"))
                .await
                .is_err(),
            "user_1 already exists"
        );
        mgr.update_user(
            PbUserInfo {
                id: user_1.user_id as _,
                name: "test_user_1_new".to_owned(),
                ..Default::default()
            },
            &[PbUpdateField::Rename],
        )
        .await?;
        let user_1 = mgr.get_user(user_1.user_id).await?;
        assert_eq!(user_1.name, "test_user_1_new".to_owned());

        let conn_with_option = make_privilege(
            PbObject::DatabaseId(TEST_DATABASE_ID as _),
            &[PbAction::Connect],
            true,
        );
        let create_without_option = make_privilege(
            PbObject::DatabaseId(TEST_DATABASE_ID as _),
            &[PbAction::Create],
            false,
        );
        // ROOT grant CONN with grant option to user_1.
        mgr.grant_privilege(
            vec![user_1.user_id],
            &[conn_with_option.clone()],
            TEST_ROOT_USER_ID,
            true,
        )
        .await?;
        // ROOT grant CREATE without grant option to user_1.
        mgr.grant_privilege(
            vec![user_1.user_id],
            &[create_without_option.clone()],
            TEST_ROOT_USER_ID,
            false,
        )
        .await?;
        // user_1 grant CONN with grant option to user_2.
        mgr.grant_privilege(
            vec![user_2.user_id],
            &[conn_with_option.clone()],
            user_1.user_id,
            true,
        )
        .await?;
        mgr.grant_privilege(
            vec![user_2.user_id],
            &[create_without_option.clone()],
            user_1.user_id,
            false,
        )
        .await?;

        assert!(
            mgr.drop_user(user_1.user_id).await.is_err(),
            "user_1 can't be dropped"
        );

        let privilege_1 = get_user_privilege(user_1.user_id, &mgr.inner.read().await.db).await?;
        assert_eq!(privilege_1.len(), 2);
        assert!(privilege_1.iter().all(|gp| gp.object
            == Some(PbObject::DatabaseId(TEST_DATABASE_ID as _))
            && gp.action_with_opts[0].granted_by == TEST_ROOT_USER_ID as u32));

        let privilege_2 = get_user_privilege(user_2.user_id, &mgr.inner.read().await.db).await?;
        assert_eq!(privilege_2.len(), 1);
        assert!(privilege_2.iter().all(|gp| gp.object
            == Some(PbObject::DatabaseId(TEST_DATABASE_ID as _))
            && gp.action_with_opts[0].granted_by == user_1.user_id as u32
            && gp.action_with_opts[0].with_grant_option));

        // revoke privilege for others by non-super user.
        assert!(
            mgr.revoke_privilege(
                vec![user_1.user_id],
                &[conn_with_option.clone()],
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
                &[create_without_option.clone()],
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
                &[conn_with_option.clone()],
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
            &[create_without_option.clone()],
            TEST_ROOT_USER_ID,
            TEST_ROOT_USER_ID,
            false,
            false,
        )
        .await?;

        let privilege_1 = get_user_privilege(user_1.user_id, &mgr.inner.read().await.db).await?;
        assert_eq!(privilege_1.len(), 1);
        assert!(privilege_1.iter().all(|gp| gp.object
            == Some(PbObject::DatabaseId(TEST_DATABASE_ID as _))
            && gp.action_with_opts[0].action == PbAction::Connect as i32));

        // revoke grant option for referred privilege in cascade mode.
        mgr.revoke_privilege(
            vec![user_1.user_id],
            &[conn_with_option.clone()],
            TEST_ROOT_USER_ID,
            TEST_ROOT_USER_ID,
            true,
            true,
        )
        .await?;
        let privilege_1 = get_user_privilege(user_1.user_id, &mgr.inner.read().await.db).await?;
        assert_eq!(privilege_1.len(), 1);
        assert!(privilege_1.iter().all(|gp| gp.object
            == Some(PbObject::DatabaseId(TEST_DATABASE_ID as _))
            && gp.action_with_opts[0].action == PbAction::Connect as i32
            && !gp.action_with_opts[0].with_grant_option));
        let privilege_2 = get_user_privilege(user_2.user_id, &mgr.inner.read().await.db).await?;
        assert_eq!(privilege_2.len(), 1);
        assert!(privilege_2.iter().all(|gp| gp.object
            == Some(PbObject::DatabaseId(TEST_DATABASE_ID as _))
            && gp.action_with_opts[0].action == PbAction::Connect as i32
            && !gp.action_with_opts[0].with_grant_option));

        // revoke referred privilege in cascade mode.
        mgr.revoke_privilege(
            vec![user_1.user_id],
            &[conn_with_option.clone()],
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

        mgr.drop_user(user_1.user_id).await?;
        mgr.drop_user(user_2.user_id).await?;
        Ok(())
    }
}
