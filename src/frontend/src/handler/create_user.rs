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
use risingwave_pb::user::UserInfo;
use risingwave_sqlparser::ast::{CreateUserStatement, ObjectName, UserOption, UserOptions};

use crate::binder::Binder;
use crate::catalog::CatalogError;
use crate::session::OptimizerContext;
use crate::user::user_authentication::{encrypt_default, encrypted_password};

pub(crate) fn make_prost_user_info(name: ObjectName, options: &UserOptions) -> Result<UserInfo> {
    let mut user_info = UserInfo {
        name: Binder::resolve_user_name(name)?,
        // the LOGIN option is implied if it is not explicitly specified.
        can_login: true,
        can_create_user: false,
        ..Default::default()
    };
    for option in &options.0 {
        match option {
            UserOption::SuperUser => user_info.is_super = true,
            UserOption::NoSuperUser => user_info.is_super = false,
            UserOption::CreateDB => user_info.can_create_db = true,
            UserOption::NoCreateDB => user_info.can_create_db = false,
            UserOption::CreateUser => user_info.can_create_user = true,
            UserOption::NoCreateUser => user_info.can_create_user = false,
            UserOption::Login => user_info.can_login = true,
            UserOption::NoLogin => user_info.can_login = false,
            UserOption::EncryptedPassword(p) => {
                if !p.0.is_empty() {
                    user_info.auth_info = Some(encrypt_default(&user_info.name, &p.0));
                }
            }
            UserOption::Password(opt) => {
                if let Some(password) = opt {
                    user_info.auth_info = encrypted_password(&user_info.name, &password.0);
                }
            }
        }
    }

    Ok(user_info)
}

pub async fn handle_create_user(
    context: OptimizerContext,
    stmt: CreateUserStatement,
) -> Result<PgResponse> {
    let session = context.session_ctx;
    let user_info = make_prost_user_info(stmt.user_name, &stmt.with_options)?;

    {
        let user_reader = session.env().user_info_reader();
        let reader = user_reader.read_guard();
        if reader.get_user_by_name(&user_info.name).is_some() {
            return Err(CatalogError::Duplicated("user", user_info.name).into());
        }

        let session_user = reader.get_user_by_name(session.user_name()).unwrap();
        if !session_user.is_super
            && (!session_user.can_create_user
                || user_info.is_super
                || (!session_user.can_create_db && user_info.can_create_db))
        {
            return Err(PermissionDenied("Do not have the privilege".to_string()).into());
        }
    }

    let user_info_writer = session.env().user_info_writer();
    user_info_writer.create_user(user_info).await?;
    Ok(PgResponse::empty_result(StatementType::CREATE_USER))
}

#[cfg(test)]
mod tests {
    use risingwave_common::catalog::DEFAULT_DATABASE_NAME;
    use risingwave_pb::user::auth_info::EncryptionType;
    use risingwave_pb::user::AuthInfo;

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
                encrypted_value: b"827ccb0eea8a706c4c34a16891f84e7b".to_vec()
            })
        );
        frontend
            .run_sql("CREATE USER usercreator WITH NOSUPERUSER CREATEUSER PASSWORD ''")
            .await
            .unwrap();
        assert!(frontend
            .run_user_sql(
                "CREATE USER fail WITH PASSWORD 'md5827ccb0eea8a706c4c34a16891f84e7b'",
                DEFAULT_DATABASE_NAME.to_string(),
                "user".to_string(),
                user_info.id
            )
            .await
            .is_err());

        assert!(frontend
            .run_user_sql(
                "CREATE USER success WITH NOSUPERUSER PASSWORD ''",
                DEFAULT_DATABASE_NAME.to_string(),
                "usercreator".to_string(),
                user_info.id
            )
            .await
            .is_ok());
        assert!(frontend
            .run_user_sql(
                "CREATE USER fail2 WITH SUPERUSER PASSWORD ''",
                DEFAULT_DATABASE_NAME.to_string(),
                "usercreator".to_string(),
                user_info.id
            )
            .await
            .is_err());
    }
}
