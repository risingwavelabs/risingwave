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
use risingwave_common::error::Result;
use risingwave_pb::user::UserInfo;
use risingwave_sqlparser::ast::{
    CreateUserOption, CreateUserStatement, CreateUserWithOptions, ObjectName,
};

use crate::binder::Binder;
use crate::session::OptimizerContext;
use crate::user::{encrypt_default, try_extract};

pub(crate) fn make_prost_user_info(
    name: ObjectName,
    options: &CreateUserWithOptions,
) -> Result<UserInfo> {
    let mut user_info = UserInfo {
        name: Binder::resolve_user_name(name)?,
        // the LOGIN option is implied if it is not explicitly specified.
        can_login: true,
        ..Default::default()
    };
    for option in &options.0 {
        match option {
            CreateUserOption::SuperUser => user_info.is_supper = true,
            CreateUserOption::NoSuperUser => user_info.is_supper = false,
            CreateUserOption::CreateDB => user_info.can_create_db = true,
            CreateUserOption::NoCreateDB => user_info.can_create_db = false,
            CreateUserOption::Login => user_info.can_login = true,
            CreateUserOption::NoLogin => user_info.can_login = false,
            CreateUserOption::EncryptedPassword(p) => {
                if !p.0.is_empty() {
                    user_info.auth_info = Some(encrypt_default(&p.0));
                }
            }
            CreateUserOption::Password(opt) => {
                if let Some(password) = opt {
                    user_info.auth_info = try_extract(&password.0);
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
    let user_info_writer = session.env().user_info_writer();
    user_info_writer.create_user(user_info).await?;
    Ok(PgResponse::empty_result(StatementType::CREATE_USER))
}
