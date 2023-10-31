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

use std::collections::{HashMap, HashSet};

use itertools::Itertools;
use risingwave_common::catalog::{DEFAULT_SUPER_USER, DEFAULT_SUPER_USER_FOR_PG};
use risingwave_meta_model_v2::prelude::{Object, User, UserPrivilege};
use risingwave_meta_model_v2::user_privilege::Action;
use risingwave_meta_model_v2::{object, user, user_privilege, AuthInfo, UserId};
use risingwave_pb::meta::subscribe_response::{
    Info as NotificationInfo, Operation as NotificationOperation,
};
use risingwave_pb::user::update_user_request::PbUpdateField;
use risingwave_pb::user::{PbGrantPrivilege, PbUserInfo};
use sea_orm::sea_query::{OnConflict, SimpleExpr, Value};
use sea_orm::{
    ActiveModelTrait, ActiveValue, ColumnTrait, EntityTrait, IntoActiveModel, PaginatorTrait,
    QueryFilter, QuerySelect, TransactionTrait,
};

use crate::controller::catalog::CatalogController;
use crate::controller::utils::{
    check_user_name_duplicate, ensure_privileges_not_referred, ensure_user_id,
    extract_grant_obj_id, get_referring_privileges_cascade, get_user_privilege,
};
use crate::manager::NotificationVersion;
use crate::{MetaError, MetaResult};

impl CatalogController {
    async fn create_user(&self, user: PbUserInfo) -> MetaResult<NotificationVersion> {
        let inner = self.inner.write().await;
        check_user_name_duplicate(&user.name, &inner.db).await?;

        let mut user: user::ActiveModel = user.into();
        user.user_id = ActiveValue::NotSet;
        let user = user.insert(&inner.db).await?;
        let version = self
            .notify_frontend(
                NotificationOperation::Add,
                NotificationInfo::User(user.into()),
            )
            .await;

        Ok(version)
    }

    async fn update_user(
        &self,
        update_user: PbUserInfo,
        update_fields: &[PbUpdateField],
    ) -> MetaResult<NotificationVersion> {
        let inner = self.inner.write().await;
        let rename_flag = update_fields
            .iter()
            .any(|&field| field == PbUpdateField::Rename);
        if rename_flag {
            check_user_name_duplicate(&update_user.name, &inner.db).await?;
        }

        let mut user = User::find_by_id(update_user.id)
            .one(&inner.db)
            .await?
            .ok_or_else(|| MetaError::catalog_id_not_found("user", update_user.id))?;
        update_fields.iter().for_each(|&field| match field {
            PbUpdateField::Unspecified => unreachable!(),
            PbUpdateField::Super => user.is_super = update_user.is_super,
            PbUpdateField::Login => user.can_login = update_user.can_login,
            PbUpdateField::CreateDb => user.can_create_db = update_user.can_create_db,
            PbUpdateField::CreateUser => user.can_create_user = update_user.can_create_user,
            PbUpdateField::AuthInfo => user.auth_info = update_user.auth_info.clone().map(AuthInfo),
            PbUpdateField::Rename => user.name = update_user.name.clone(),
        });

        let user = user.into_active_model().update(&inner.db).await?;
        let mut user_info: PbUserInfo = user.into();
        user_info.grant_privileges = get_user_privilege(user_info.id, &inner.db).await?;
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

    async fn drop_user(&self, user_id: UserId) -> MetaResult<NotificationVersion> {
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
                    id: user_id,
                    ..Default::default()
                }),
            )
            .await;

        Ok(version)
    }

    pub async fn grant_privilege(
        &self,
        user_ids: &[UserId],
        new_grant_privileges: &[PbGrantPrivilege],
        grantor: UserId,
    ) -> MetaResult<NotificationVersion> {
        let inner = self.inner.write().await;
        let txn = inner.db.begin().await?;
        for user_id in user_ids {
            ensure_user_id(*user_id, &txn).await?;
        }

        let mut privileges = vec![];
        for gp in new_grant_privileges {
            let id = extract_grant_obj_id(gp.get_object()?);
            for action_with_opt in &gp.action_with_opts {
                privileges.push(user_privilege::ActiveModel {
                    oid: ActiveValue::Set(id),
                    granted_by: ActiveValue::Set(grantor),
                    action: ActiveValue::Set(action_with_opt.get_action()?.into()),
                    with_grant_option: ActiveValue::Set(action_with_opt.with_grant_option),
                    ..Default::default()
                });
            }
        }

        // check whether grantor has the privilege to grant the privilege.
        let user = User::find_by_id(grantor)
            .one(&txn)
            .await?
            .ok_or_else(|| MetaError::catalog_id_not_found("user", grantor))?;
        if !user.is_super {
            for privilege in &mut privileges {
                let mut filter = user_privilege::Column::UserId
                    .eq(grantor)
                    .and(user_privilege::Column::Oid.eq(*privilege.oid.as_ref()))
                    .and(user_privilege::Column::Action.eq(privilege.action.as_ref().clone()));
                if *privilege.with_grant_option.as_ref() {
                    filter = filter.and(user_privilege::Column::WithGrantOption.eq(true));
                }
                let privilege_id: Option<i32> = UserPrivilege::find()
                    .select_only()
                    .column(user_privilege::Column::Id)
                    .filter(filter)
                    .into_tuple()
                    .one(&txn)
                    .await?;
                let Some(privilege_id) = privilege_id else {
                    return Err(MetaError::permission_denied(format!(
                        "user {} don't have privilege {:?} or grant option",
                        grantor, privilege.action,
                    )));
                };
                privilege.dependent_id = ActiveValue::Set(privilege_id);
            }
        }

        // insert privileges
        let user_privileges = user_ids.iter().flat_map(|user_id| {
            privileges.iter().map(|p| {
                let mut p = p.clone();
                p.user_id = ActiveValue::Set(*user_id);
                p
            })
        });
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
                on_conflict.do_nothing();
            }
            UserPrivilege::insert(privilege)
                .on_conflict(on_conflict)
                .exec(&txn)
                .await?;
        }

        let mut user_infos = vec![];
        for user_id in user_ids {
            let user = User::find_by_id(*user_id)
                .one(&txn)
                .await?
                .ok_or_else(|| MetaError::catalog_id_not_found("user", *user_id))?;
            let mut user_info: PbUserInfo = user.into();
            user_info.grant_privileges = get_user_privilege(user_info.id, &txn).await?;
            user_infos.push(user_info);
        }
        txn.commit().await?;

        // todo: change to only notify privilege change.
        let mut version = 0;
        for info in user_infos {
            version = self
                .notify_frontend(NotificationOperation::Update, NotificationInfo::User(info))
                .await;
        }
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

        let mut revoke_items = HashMap::new();
        for privilege in revoke_grant_privileges {
            let obj = extract_grant_obj_id(privilege.get_object()?);
            let actions = privilege
                .action_with_opts
                .iter()
                .map(|ao| Action::from(ao.get_action().unwrap()))
                .collect_vec();
            revoke_items.insert(obj, actions);
        }

        let filter = if !revoke_user.is_super {
            // todo: do not request meta if granted by is not specified and revoke user is not super.
            if granted_by != revoke_by {
                return Err(MetaError::permission_denied(format!(
                    "user {} is not super, can't revoke privileges for {}",
                    revoke_user.name, granted_by,
                )));
            };
            // ensure user have grant options.
            for (obj, actions) in &revoke_items {
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
        let mut root_privilege_ids: Vec<i32> = vec![];
        for (obj, actions) in &revoke_items {
            let filter = filter
                .clone()
                .and(user_privilege::Column::Oid.eq(*obj))
                .and(user_privilege::Column::Action.is_in(actions.clone()));
            root_privilege_ids.extend(
                UserPrivilege::find()
                    .select_only()
                    .column(user_privilege::Column::Id)
                    .filter(filter)
                    .into_tuple::<i32>()
                    .all(&txn)
                    .await?,
            );
        }

        // check if the user granted any privileges to other users.
        let all_privilege_ids = if !cascade {
            ensure_privileges_not_referred(root_privilege_ids.clone(), &txn).await?;
            root_privilege_ids.clone()
        } else {
            get_referring_privileges_cascade(root_privilege_ids.clone(), &txn).await?
        };

        // todo: return them in cte.
        let to_update_users = UserPrivilege::find()
            .select_only()
            .column(user_privilege::Column::UserId)
            .filter(user_privilege::Column::Id.is_in(all_privilege_ids.clone()))
            .into_tuple::<UserId>()
            .all(&txn)
            .await?;

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

        let mut user_infos = vec![];
        for user_id in to_update_users {
            let user = User::find_by_id(user_id)
                .one(&txn)
                .await?
                .ok_or_else(|| MetaError::catalog_id_not_found("user", user_id))?;
            let mut user_info: PbUserInfo = user.into();
            user_info.grant_privileges = get_user_privilege(user_info.id, &txn).await?;
            user_infos.push(user_info);
        }
        txn.commit().await?;

        // todo: change to only notify privilege change.
        let mut version = 0;
        for info in user_infos {
            version = self
                .notify_frontend(NotificationOperation::Update, NotificationInfo::User(info))
                .await;
        }
        Ok(version)
    }
}
