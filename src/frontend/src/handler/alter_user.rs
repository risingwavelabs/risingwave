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

use std::collections::HashSet;

use pgwire::pg_response::StatementType;
use risingwave_common::catalog::{DEFAULT_SUPER_USER_FOR_ADMIN, is_reserved_admin_user};
use risingwave_pb::user::UserInfo;
use risingwave_pb::user::update_user_request::UpdateField;
use risingwave_sqlparser::ast::{AlterUserStatement, ObjectName, UserOption, UserOptions};

use super::RwPgResponse;
use crate::binder::Binder;
use crate::catalog::CatalogError;
use crate::error::ErrorCode::{self, InternalError, PermissionDenied};
use crate::error::Result;
use crate::handler::HandlerArgs;
use crate::user::user_authentication::{
    OAUTH_ISSUER_KEY, OAUTH_JWKS_URL_KEY, build_oauth_info, encrypted_password,
};
use crate::user::user_catalog::UserCatalog;

fn alter_prost_user_info(
    mut user_info: UserInfo,
    options: &UserOptions,
    session_user: &UserCatalog,
) -> Result<(UserInfo, Vec<UpdateField>, Vec<String>)> {
    let change_self_password_only = session_user.id == user_info.id
        && options.0.len() == 1
        && matches!(
            &options.0[0],
            UserOption::EncryptedPassword(_) | UserOption::Password(_) | UserOption::OAuth(_)
        );
    let require_admin = user_info.is_admin
        || options
            .0
            .iter()
            .any(|option| matches!(option, UserOption::Admin | UserOption::NoAdmin));
    if !change_self_password_only && require_admin && !session_user.is_admin {
        return Err(
            PermissionDenied(format!("{} cannot be altered", user_info.name).to_owned()).into(),
        );
    }

    if !session_user.is_super {
        let require_super = user_info.is_super
            || options
                .0
                .iter()
                .any(|option| matches!(option, UserOption::SuperUser | UserOption::NoSuperUser));
        if require_super {
            return Err(PermissionDenied(
                "must be superuser to alter superuser roles or change superuser attribute"
                    .to_owned(),
            )
            .into());
        }

        if !session_user.can_create_user && !change_self_password_only {
            return Err(PermissionDenied("permission denied to alter user".to_owned()).into());
        }
    }

    let mut update_fields = HashSet::new();
    let mut notices = vec![];

    for option in &options.0 {
        match option {
            UserOption::SuperUser => {
                user_info.is_super = true;
                update_fields.insert(UpdateField::Super);
            }
            UserOption::NoSuperUser => {
                if user_info.is_admin {
                    // Admin users cannot have superuser status removed - show notice but ignore
                    notices.push("admin users must remain superusers, ignoring NOSUPERUSER".to_owned());
                } else {
                    user_info.is_super = false;
                    update_fields.insert(UpdateField::Super);
                }
            }
            UserOption::CreateDB => {
                user_info.can_create_db = true;
                update_fields.insert(UpdateField::CreateDb);
            }
            UserOption::NoCreateDB => {
                user_info.can_create_db = false;
                update_fields.insert(UpdateField::CreateDb);
            }
            UserOption::CreateUser => {
                user_info.can_create_user = true;
                update_fields.insert(UpdateField::CreateUser);
            }
            UserOption::NoCreateUser => {
                user_info.can_create_user = false;
                update_fields.insert(UpdateField::CreateUser);
            }
            UserOption::Login => {
                user_info.can_login = true;
                update_fields.insert(UpdateField::Login);
            }
            UserOption::NoLogin => {
                user_info.can_login = false;
                update_fields.insert(UpdateField::Login);
            }
            UserOption::Admin => {
                user_info.is_admin = true;
                update_fields.insert(UpdateField::Admin);
            }
            UserOption::NoAdmin => {
                user_info.is_admin = false;
                update_fields.insert(UpdateField::Admin);
            }
            UserOption::EncryptedPassword(p) => {
                if !p.0.is_empty() {
                    user_info.auth_info = encrypted_password(&user_info.name, &p.0);
                } else {
                    user_info.auth_info = None;
                    notices
                        .push("empty string is not a valid password, clearing password".to_owned());
                };
                update_fields.insert(UpdateField::AuthInfo);
            }
            UserOption::Password(opt) => {
                if let Some(password) = opt
                    && !password.0.is_empty()
                {
                    user_info.auth_info = encrypted_password(&user_info.name, &password.0);
                } else {
                    user_info.auth_info = None;
                    notices
                        .push("empty string is not a valid password, clearing password".to_owned());
                }
                update_fields.insert(UpdateField::AuthInfo);
            }
            UserOption::OAuth(options) => {
                let auth_info = build_oauth_info(options).ok_or_else(|| {
                    ErrorCode::InvalidParameterValue(format!(
                        "{} and {} must be provided",
                        OAUTH_JWKS_URL_KEY, OAUTH_ISSUER_KEY
                    ))
                })?;
                user_info.auth_info = Some(auth_info);
                update_fields.insert(UpdateField::AuthInfo);
            }
        }
    }

    if user_info.is_admin && !user_info.is_super {
        user_info.is_super = true;
        update_fields.insert(UpdateField::Super);
    }

    Ok((user_info, update_fields.into_iter().collect(), notices))
}

fn alter_rename_prost_user_info(
    mut user_info: UserInfo,
    new_name: ObjectName,
    session_user: &UserCatalog,
) -> Result<(UserInfo, Vec<UpdateField>)> {
    if session_user.id == user_info.id {
        return Err(InternalError("session user cannot be renamed".to_owned()).into());
    }

    if !session_user.is_super {
        return Err(PermissionDenied("must be superuser to rename users".to_owned()).into());
    }

    let new_name = Binder::resolve_user_name(new_name)?;
    if is_reserved_admin_user(&new_name) {
        return Err(PermissionDenied(
            format!("{} is reserved for admin", DEFAULT_SUPER_USER_FOR_ADMIN).to_owned(),
        )
        .into());
    }
    if is_reserved_admin_user(&user_info.name) {
        return Err(PermissionDenied(
            format!("{} cannot be renamed", DEFAULT_SUPER_USER_FOR_ADMIN).to_owned(),
        )
        .into());
    }

    user_info.name = new_name;
    user_info.auth_info = None;
    Ok((user_info, vec![UpdateField::Rename, UpdateField::AuthInfo]))
}

pub async fn handle_alter_user(
    handler_args: HandlerArgs,
    stmt: AlterUserStatement,
) -> Result<RwPgResponse> {
    let session = handler_args.session;
    let (user_info, update_fields, notices) = {
        let user_name = Binder::resolve_user_name(stmt.user_name.clone())?;
        let user_reader = session.env().user_info_reader().read_guard();

        let old_info = user_reader
            .get_user_by_name(&user_name)
            .ok_or(CatalogError::NotFound("user", user_name))?
            .to_prost();

        let session_user = user_reader
            .get_user_by_name(&session.user_name())
            .ok_or_else(|| CatalogError::NotFound("user", session.user_name().to_owned()))?;

        match stmt.mode {
            risingwave_sqlparser::ast::AlterUserMode::Options(options) => {
                alter_prost_user_info(old_info, &options, session_user)?
            }
            risingwave_sqlparser::ast::AlterUserMode::Rename(new_name) => {
                let (user_info, fields) =
                    alter_rename_prost_user_info(old_info, new_name, session_user)?;
                (user_info, fields, vec![])
            }
        }
    };

    let user_info_writer = session.user_info_writer()?;
    user_info_writer
        .update_user(user_info, update_fields)
        .await?;
    let mut response_builder = RwPgResponse::builder(StatementType::UPDATE_USER);
    for notice in notices {
        response_builder = response_builder.notice(notice);
    }
    Ok(response_builder.into())
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use risingwave_common::catalog::{
        DEFAULT_DATABASE_NAME, DEFAULT_SUPER_USER_FOR_ADMIN, DEFAULT_SUPER_USER_FOR_ADMIN_ID,
    };
    use risingwave_pb::user::AuthInfo;
    use risingwave_pb::user::auth_info::EncryptionType;

    use crate::test_utils::LocalFrontend;

    #[tokio::test]
    async fn test_alter_user() {
        let frontend = LocalFrontend::new(Default::default()).await;
        let session = frontend.session_ref();
        let user_info_reader = session.env().user_info_reader();

        frontend.run_sql("CREATE USER userB WITH SUPERUSER NOCREATEDB PASSWORD 'md5827ccb0eea8a706c4c34a16891f84e7b'").await.unwrap();
        frontend
            .run_sql("ALTER USER userB RENAME TO user")
            .await
            .unwrap();
        assert!(
            user_info_reader
                .read_guard()
                .get_user_by_name("userB")
                .is_none()
        );
        assert!(
            user_info_reader
                .read_guard()
                .get_user_by_name("user")
                .is_some()
        );

        frontend.run_sql("ALTER USER user WITH NOSUPERUSER CREATEDB PASSWORD 'md59f2fa6a30871a92249bdd2f1eeee4ef6'").await.unwrap();

        let user_info = user_info_reader
            .read_guard()
            .get_user_by_name("user")
            .cloned()
            .unwrap();
        assert!(!user_info.is_super);
        assert!(user_info.can_create_db);
        assert_eq!(
            user_info.auth_info,
            Some(AuthInfo {
                encryption_type: EncryptionType::Md5 as i32,
                encrypted_value: b"9f2fa6a30871a92249bdd2f1eeee4ef6".to_vec(),
                metadata: HashMap::new(),
            })
        );
    }

    #[tokio::test]
    async fn test_alter_admin_user() {
        let frontend = LocalFrontend::new(Default::default()).await;
        let session = frontend.session_ref();
        let user_info_reader = session.env().user_info_reader();

        // Create admin user
        frontend
            .run_user_sql(
                "CREATE USER admin_user WITH ADMIN",
                DEFAULT_DATABASE_NAME.to_owned(),
                DEFAULT_SUPER_USER_FOR_ADMIN.to_owned(),
                DEFAULT_SUPER_USER_FOR_ADMIN_ID,
            )
            .await
            .unwrap();

        let admin_user = user_info_reader
            .read_guard()
            .get_user_by_name("admin_user")
            .cloned()
            .unwrap();

        // Should be both admin and superuser
        assert!(admin_user.is_admin);
        assert!(admin_user.is_super);

        // Try to remove superuser status from admin user - should be ignored with notice
        frontend
            .run_user_sql(
                "ALTER USER admin_user WITH NOSUPERUSER",
                DEFAULT_DATABASE_NAME.to_owned(),
                DEFAULT_SUPER_USER_FOR_ADMIN.to_owned(),
                DEFAULT_SUPER_USER_FOR_ADMIN_ID,
            )
            .await
            .unwrap();

        let admin_user_after = user_info_reader
            .read_guard()
            .get_user_by_name("admin_user")
            .cloned()
            .unwrap();

        assert!(admin_user_after.is_admin);
        assert!(admin_user_after.is_super);

        // Now remove admin and super status from admin user
        frontend
            .run_user_sql(
                "ALTER USER admin_user WITH NOADMIN NOSUPERUSER",
                DEFAULT_DATABASE_NAME.to_owned(),
                DEFAULT_SUPER_USER_FOR_ADMIN.to_owned(),
                DEFAULT_SUPER_USER_FOR_ADMIN_ID,
            )
            .await
            .unwrap();

        let admin_user_after = user_info_reader
            .read_guard()
            .get_user_by_name("admin_user")
            .cloned()
            .unwrap();

        assert!(!admin_user_after.is_admin);
        assert!(!admin_user_after.is_super);
    }
}
