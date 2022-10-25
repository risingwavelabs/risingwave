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

use pgwire::pg_response::{PgResponse, StatementType};
use risingwave_common::error::ErrorCode::{InternalError, PermissionDenied};
use risingwave_common::error::Result;
use risingwave_pb::user::update_user_request::UpdateField;
use risingwave_pb::user::UserInfo;
use risingwave_sqlparser::ast::{AlterUserStatement, ObjectName, UserOption, UserOptions};

use super::RwPgResponse;
use crate::binder::Binder;
use crate::catalog::CatalogError;
use crate::session::OptimizerContext;
use crate::user::user_authentication::encrypted_password;

fn alter_prost_user_info(
    mut user_info: UserInfo,
    options: &UserOptions,
    session_user: &UserInfo,
) -> Result<(UserInfo, Vec<UpdateField>)> {
    if !session_user.is_super {
        let require_super = user_info.is_super
            || options
                .0
                .iter()
                .any(|option| matches!(option, UserOption::SuperUser | UserOption::NoSuperUser));
        if require_super {
            return Err(PermissionDenied(
                "must be superuser to alter superuser roles or change superuser attribute"
                    .to_string(),
            )
            .into());
        }

        let change_self_password = session_user.id == user_info.id
            && options.0.len() == 1
            && matches!(
                &options.0[0],
                UserOption::EncryptedPassword(_) | UserOption::Password(_)
            );
        if !session_user.can_create_user && !change_self_password {
            return Err(PermissionDenied("Do not have the privilege".to_string()).into());
        }
    }

    let mut update_fields = Vec::new();
    for option in &options.0 {
        match option {
            UserOption::SuperUser => {
                user_info.is_super = true;
                update_fields.push(UpdateField::Super);
            }
            UserOption::NoSuperUser => {
                user_info.is_super = false;
                update_fields.push(UpdateField::Super);
            }
            UserOption::CreateDB => {
                user_info.can_create_db = true;
                update_fields.push(UpdateField::CreateDb);
            }
            UserOption::NoCreateDB => {
                user_info.can_create_db = false;
                update_fields.push(UpdateField::CreateDb);
            }
            UserOption::CreateUser => {
                user_info.can_create_user = true;
                update_fields.push(UpdateField::CreateUser);
            }
            UserOption::NoCreateUser => {
                user_info.can_create_user = false;
                update_fields.push(UpdateField::CreateUser);
            }
            UserOption::Login => {
                user_info.can_login = true;
                update_fields.push(UpdateField::Login);
            }
            UserOption::NoLogin => {
                user_info.can_login = false;
                update_fields.push(UpdateField::Login);
            }
            UserOption::EncryptedPassword(p) => {
                // TODO: Behaviour of PostgreSQL: Notice when password is empty string.
                if !p.0.is_empty() {
                    user_info.auth_info = encrypted_password(&user_info.name, &p.0);
                } else {
                    user_info.auth_info = None;
                };
                update_fields.push(UpdateField::AuthInfo);
            }
            UserOption::Password(opt) => {
                // TODO: Behaviour of PostgreSQL: Notice when password is empty string.
                if let Some(password) = opt && !password.0.is_empty() {
                    user_info.auth_info = encrypted_password(&user_info.name, &password.0);
                } else {
                    user_info.auth_info = None;
                }
                update_fields.push(UpdateField::AuthInfo);
            }
        }
    }
    Ok((user_info, update_fields))
}

fn alter_rename_prost_user_info(
    mut user_info: UserInfo,
    new_name: ObjectName,
    session_user: &UserInfo,
) -> Result<(UserInfo, Vec<UpdateField>)> {
    if session_user.id == user_info.id {
        return Err(InternalError("session user cannot be renamed".to_string()).into());
    }

    if !session_user.is_super {
        if user_info.is_super {
            return Err(
                PermissionDenied("must be superuser to rename superusers".to_string()).into(),
            );
        }

        if !session_user.can_create_user {
            return Err(
                PermissionDenied("Do not have the privilege to rename user".to_string()).into(),
            );
        }
    }

    user_info.name = Binder::resolve_user_name(new_name)?;
    Ok((user_info, vec![UpdateField::Rename]))
}

pub async fn handle_alter_user(
    context: OptimizerContext,
    stmt: AlterUserStatement,
) -> Result<RwPgResponse> {
    let session = context.session_ctx;
    let (user_info, update_fields) = {
        let user_name = Binder::resolve_user_name(stmt.user_name.clone())?;
        let user_reader = session.env().user_info_reader().read_guard();

        let old_info = user_reader
            .get_user_by_name(&user_name)
            .ok_or(CatalogError::NotFound("user", user_name))?
            .clone();

        let session_user = user_reader
            .get_user_by_name(session.user_name())
            .ok_or_else(|| CatalogError::NotFound("user", session.user_name().to_string()))?;

        match stmt.mode {
            risingwave_sqlparser::ast::AlterUserMode::Options(options) => {
                alter_prost_user_info(old_info, &options, session_user)?
            }
            risingwave_sqlparser::ast::AlterUserMode::Rename(new_name) => {
                alter_rename_prost_user_info(old_info, new_name, session_user)?
            }
        }
    };

    let user_info_writer = session.env().user_info_writer();
    user_info_writer
        .update_user(user_info, update_fields)
        .await?;
    Ok(PgResponse::empty_result(StatementType::UPDATE_USER))
}

#[cfg(test)]
mod tests {
    use risingwave_pb::user::auth_info::EncryptionType;
    use risingwave_pb::user::AuthInfo;

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
        assert!(user_info_reader
            .read_guard()
            .get_user_by_name("userB")
            .is_none());
        assert!(user_info_reader
            .read_guard()
            .get_user_by_name("user")
            .is_some());

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
                encrypted_value: b"9f2fa6a30871a92249bdd2f1eeee4ef6".to_vec()
            })
        );
    }
}
