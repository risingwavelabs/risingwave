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

use pgwire::pg_response::StatementType;
use risingwave_pb::user::grant_privilege::{Action, ActionWithGrantOption, Object};
use risingwave_pb::user::{GrantPrivilege, UserInfo};
use risingwave_sqlparser::ast::{CreateUserStatement, UserOption};

use super::RwPgResponse;
use crate::binder::Binder;
use crate::catalog::CatalogError;
use crate::error::ErrorCode::{self, PermissionDenied};
use crate::error::Result;
use crate::handler::HandlerArgs;
use crate::user::user_authentication::{
    OAUTH_ISSUER_KEY, OAUTH_JWKS_URL_KEY, build_oauth_info, encrypted_password,
};

pub async fn handle_create_user(
    handler_args: HandlerArgs,
    stmt: CreateUserStatement,
) -> Result<RwPgResponse> {
    let session = handler_args.session;
    let database_id = {
        let catalog_reader = session.env().catalog_reader().read_guard();
        catalog_reader
            .get_database_by_name(&session.database())
            .expect("session database should exist")
            .id()
    };

    let user_name = Binder::resolve_user_name(stmt.user_name)?;
    let mut user_info = UserInfo {
        name: user_name.clone(),
        // the LOGIN option is implied if it is not explicitly specified.
        can_login: true,
        ..Default::default()
    };
    let mut notice = None;
    {
        let user_reader = session.env().user_info_reader().read_guard();
        if user_reader.get_user_by_name(&user_name).is_some() {
            return Err(CatalogError::duplicated("user", user_name).into());
        }

        let session_user = user_reader
            .get_user_by_name(&session.user_name())
            .ok_or_else(|| CatalogError::NotFound("user", session.user_name()))?;

        if !session_user.is_super {
            let require_super = stmt
                .with_options
                .0
                .iter()
                .any(|option| matches!(option, UserOption::SuperUser));
            if require_super {
                return Err(
                    PermissionDenied("must be superuser to create superusers".to_owned()).into(),
                );
            }

            if !session_user.can_create_user {
                return Err(PermissionDenied("permission denied to create user".to_owned()).into());
            }
        }

        // Since we don't have a concept of PUBLIC group yet, here we simply grant new user with CONNECT
        // action for the session database.
        user_info.grant_privileges = vec![GrantPrivilege {
            action_with_opts: vec![ActionWithGrantOption {
                action: Action::Connect as i32,
                with_grant_option: true,
                granted_by: session_user.id,
            }],
            object: Some(Object::DatabaseId(database_id)),
        }];

        for option in stmt.with_options.0 {
            match option {
                UserOption::SuperUser => user_info.is_super = true,
                UserOption::NoSuperUser => user_info.is_super = false,
                UserOption::CreateDB => user_info.can_create_db = true,
                UserOption::NoCreateDB => user_info.can_create_db = false,
                UserOption::CreateUser => user_info.can_create_user = true,
                UserOption::NoCreateUser => user_info.can_create_user = false,
                UserOption::Login => user_info.can_login = true,
                UserOption::NoLogin => user_info.can_login = false,
                UserOption::EncryptedPassword(password) => {
                    if !password.0.is_empty() {
                        user_info.auth_info = encrypted_password(&user_info.name, &password.0);
                    } else {
                        notice = Some(
                            "empty string is not a valid password, clearing password".to_owned(),
                        );
                    }
                }
                UserOption::Password(opt) => {
                    if let Some(password) = opt
                        && !password.0.is_empty()
                    {
                        user_info.auth_info = encrypted_password(&user_info.name, &password.0);
                    } else {
                        notice = Some(
                            "empty string is not a valid password, clearing password".to_owned(),
                        );
                    }
                }
                UserOption::OAuth(options) => {
                    let auth_info = build_oauth_info(&options).ok_or_else(|| {
                        ErrorCode::InvalidParameterValue(format!(
                            "{} and {} must be provided",
                            OAUTH_JWKS_URL_KEY, OAUTH_ISSUER_KEY
                        ))
                    })?;
                    user_info.auth_info = Some(auth_info);
                }
            }
        }
    };

    let user_info_writer = session.user_info_writer()?;
    user_info_writer.create_user(user_info).await?;
    let response_builder = RwPgResponse::builder(StatementType::CREATE_USER);
    if let Some(notice) = notice {
        Ok(response_builder.notice(notice).into())
    } else {
        Ok(response_builder.into())
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use risingwave_common::catalog::DEFAULT_DATABASE_NAME;
    use risingwave_pb::user::AuthInfo;
    use risingwave_pb::user::auth_info::EncryptionType;

    use crate::test_utils::LocalFrontend;

    #[tokio::test]
    async fn test_create_user() {
        let frontend = LocalFrontend::new(Default::default()).await;
        let session = frontend.session_ref();
        let user_info_reader = session.env().user_info_reader();

        frontend.run_sql("CREATE USER user WITH NOSUPERUSER CREATEDB PASSWORD 'md5827ccb0eea8a706c4c34a16891f84e7b'").await.unwrap();

        let user_info = user_info_reader
            .read_guard()
            .get_user_by_name("user")
            .cloned()
            .unwrap();
        assert!(!user_info.is_super);
        assert!(user_info.can_login);
        assert!(user_info.can_create_db);
        assert!(!user_info.can_create_user);
        assert_eq!(
            user_info.auth_info,
            Some(AuthInfo {
                encryption_type: EncryptionType::Md5 as i32,
                encrypted_value: b"827ccb0eea8a706c4c34a16891f84e7b".to_vec(),
                metadata: HashMap::new(),
            })
        );
        frontend
            .run_sql("CREATE USER usercreator WITH NOSUPERUSER CREATEUSER PASSWORD ''")
            .await
            .unwrap();
        assert!(
            frontend
                .run_user_sql(
                    "CREATE USER fail WITH PASSWORD 'md5827ccb0eea8a706c4c34a16891f84e7b'",
                    DEFAULT_DATABASE_NAME.to_owned(),
                    "user".to_owned(),
                    user_info.id
                )
                .await
                .is_err()
        );

        assert!(
            frontend
                .run_user_sql(
                    "CREATE USER success WITH NOSUPERUSER PASSWORD ''",
                    DEFAULT_DATABASE_NAME.to_owned(),
                    "usercreator".to_owned(),
                    user_info.id
                )
                .await
                .is_ok()
        );
        assert!(
            frontend
                .run_user_sql(
                    "CREATE USER fail2 WITH SUPERUSER PASSWORD ''",
                    DEFAULT_DATABASE_NAME.to_owned(),
                    "usercreator".to_owned(),
                    user_info.id
                )
                .await
                .is_err()
        );
    }
}
