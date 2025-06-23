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

use pgwire::pg_response::{PgResponse, StatementType};
use risingwave_common::catalog::DEFAULT_SUPER_USER_FOR_ADMIN;
use risingwave_sqlparser::ast::ObjectName;

use super::RwPgResponse;
use crate::binder::Binder;
use crate::catalog::CatalogError;
use crate::error::{ErrorCode, Result};
use crate::handler::HandlerArgs;

pub async fn handle_drop_user(
    handler_args: HandlerArgs,
    user_name: ObjectName,
    if_exists: bool,
) -> Result<RwPgResponse> {
    let session = handler_args.session;

    let user_name = Binder::resolve_user_name(user_name)?;
    let user_info_reader = session.env().user_info_reader();
    let user_info = user_info_reader
        .read_guard()
        .get_user_by_name(&user_name)
        .map(|u| (u.id, u.is_super));
    match user_info {
        Some((user_id, is_super)) => {
            if session.user_id() == user_id {
                return Err(ErrorCode::PermissionDenied(
                    "current user cannot be dropped".to_owned(),
                )
                .into());
            }
            if user_name == DEFAULT_SUPER_USER_FOR_ADMIN {
                return Err(ErrorCode::PermissionDenied(format!(
                    "cannot drop the admin superuser \"{}\"",
                    user_name
                ))
                .into());
            }
            if let Some(current_user) = user_info_reader
                .read_guard()
                .get_user_by_name(&session.user_name())
            {
                if !current_user.is_super {
                    if is_super {
                        return Err(ErrorCode::PermissionDenied(
                            "must be superuser to drop superusers".to_owned(),
                        )
                        .into());
                    }
                    if !current_user.can_create_user {
                        return Err(ErrorCode::PermissionDenied(
                            "permission denied to drop user".to_owned(),
                        )
                        .into());
                    }
                }
            } else {
                return Err(
                    ErrorCode::PermissionDenied("Session user is invalid".to_owned()).into(),
                );
            }

            let user_info_writer = session.user_info_writer()?;
            user_info_writer.drop_user(user_id).await?;
        }
        None => {
            return if if_exists {
                Ok(PgResponse::builder(StatementType::DROP_USER)
                    .notice(format!("user \"{}\" does not exist, skipping", user_name))
                    .into())
            } else {
                Err(CatalogError::NotFound("user", user_name).into())
            };
        }
    }

    Ok(PgResponse::empty_result(StatementType::DROP_USER))
}

#[cfg(test)]
mod tests {
    use crate::test_utils::LocalFrontend;

    #[tokio::test]
    async fn test_drop_user() {
        let frontend = LocalFrontend::new(Default::default()).await;
        let session = frontend.session_ref();
        let user_info_reader = session.env().user_info_reader();

        frontend.run_sql("CREATE USER user").await.unwrap();
        assert!(
            user_info_reader
                .read_guard()
                .get_user_by_name("user")
                .is_some()
        );

        frontend.run_sql("DROP USER user").await.unwrap();
        assert!(
            user_info_reader
                .read_guard()
                .get_user_by_name("user")
                .is_none()
        );
    }
}
