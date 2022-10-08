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
use risingwave_common::error::ErrorCode::PermissionDenied;
use risingwave_common::error::Result;
use risingwave_pb::user::update_user_request::UpdateField;
use risingwave_pb::user::{UpdateUserRequest, UserInfo};
use risingwave_sqlparser::ast::{AlterUserStatement, UserOption, UserOptions};

use super::RwPgResponse;
use crate::binder::Binder;
use crate::catalog::CatalogError;
use crate::session::OptimizerContext;
use crate::user::user_authentication::{encrypt_default, encrypted_password};

fn alter_prost_user_info(
    mut user_info: UserInfo,
    options: &UserOptions,
    session_user: &UserInfo,
) -> Result<UpdateUserRequest> {
    let mut update_fields = Vec::new();
    let mut has_privilege = true;
    for option in &options.0 {
        match option {
            UserOption::SuperUser => {
                if !session_user.is_super {
                    has_privilege = false;
                }
                user_info.is_super = true;
                update_fields.push(UpdateField::Super as i32);
            }
            UserOption::NoSuperUser => {
                if !session_user.is_super {
                    has_privilege = false;
                }
                user_info.is_super = false;
                update_fields.push(UpdateField::Super as i32);
            }
            UserOption::CreateDB => {
                if !session_user.can_create_db {
                    has_privilege = false;
                }
                user_info.can_create_db = true;
                update_fields.push(UpdateField::CreateDb as i32);
            }
            UserOption::NoCreateDB => {
                if !session_user.can_create_db {
                    has_privilege = false;
                }
                user_info.can_create_db = false;
                update_fields.push(UpdateField::CreateDb as i32);
            }
            UserOption::CreateUser => {
                if !session_user.can_create_user {
                    has_privilege = false;
                }
                user_info.can_create_user = true;
                update_fields.push(UpdateField::CreateUser as i32);
            }
            UserOption::NoCreateUser => {
                if !session_user.can_create_user {
                    has_privilege = false;
                }
                user_info.can_create_user = false;
                update_fields.push(UpdateField::CreateUser as i32);
            }
            UserOption::Login => {
                user_info.can_login = true;
                update_fields.push(UpdateField::Login as i32);
            }
            UserOption::NoLogin => {
                user_info.can_login = false;
                update_fields.push(UpdateField::Login as i32);
            }
            UserOption::EncryptedPassword(p) => {
                if !p.0.is_empty() {
                    user_info.auth_info = Some(encrypt_default(&user_info.name, &p.0));
                    update_fields.push(UpdateField::AuthInfo as i32);
                }
            }
            UserOption::Password(opt) => {
                if let Some(password) = opt {
                    user_info.auth_info = encrypted_password(&user_info.name, &password.0);
                    update_fields.push(UpdateField::AuthInfo as i32);
                }
            }
        }
        if !session_user.is_super && !has_privilege {
            return Err(PermissionDenied("Do not have the privilege".to_string()).into());
        }
    }
    let request = UpdateUserRequest {
        user: Some(user_info),
        update_fields,
    };
    Ok(request)
}

pub async fn handle_alter_user(
    context: OptimizerContext,
    stmt: AlterUserStatement,
) -> Result<RwPgResponse> {
    let session = context.session_ctx;
    let user_name = Binder::resolve_user_name(stmt.user_name.clone())?;
    let (mut old_info, session_user) = {
        let user_reader = session.env().user_info_reader();
        let reader = user_reader.read_guard();
        if let Some(origin_info) = reader.get_user_by_name(&user_name) {
            (
                origin_info.clone(),
                reader
                    .get_user_by_name(session.user_name())
                    .unwrap()
                    .clone(),
            )
        } else {
            return Err(CatalogError::NotFound("user", user_name).into());
        }
    };
    let request = match stmt.mode {
        risingwave_sqlparser::ast::AlterUserMode::Options(options) => {
            alter_prost_user_info(old_info, &options, &session_user)?
        }
        risingwave_sqlparser::ast::AlterUserMode::Rename(new_name) => {
            old_info.name = Binder::resolve_user_name(new_name)?;
            UpdateUserRequest {
                user: Some(old_info),
                update_fields: vec![UpdateField::Rename as i32],
            }
        }
    };
    let user_info_writer = session.env().user_info_writer();
    user_info_writer.update_user(request).await?;
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
