// Copyright 2024 RisingWave Labs
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
use risingwave_sqlparser::ast::{CreateSecretStatement, Value};

use crate::error::{ErrorCode, Result};
use crate::handler::{HandlerArgs, RwPgResponse};
use crate::Binder;

pub async fn handle_create_secret(
    handler_args: HandlerArgs,
    stmt: CreateSecretStatement,
) -> Result<RwPgResponse> {
    let session = handler_args.session.clone();
    let db_name = session.database();
    let (schema_name, connection_name) =
        Binder::resolve_schema_qualified_name(db_name, stmt.secret_name.clone())?;

    if let Err(e) = session.check_secret_name_duplicated(stmt.secret_name.clone()) {
        return if stmt.if_not_exists {
            Ok(PgResponse::builder(StatementType::CREATE_SECRET)
                .notice(format!("secret \"{}\" exists, skipping", connection_name))
                .into())
        } else {
            Err(e)
        };
    }
    let (database_id, schema_id) = session.get_database_and_schema_id_for_create(schema_name)?;
    let secret_payload = match &stmt.credential {
        Value::SingleQuotedString(ref s) => s.as_bytes().to_vec(),
        _ => {
            return Err(ErrorCode::InvalidParameterValue(
                "secret payload must be a string".to_string(),
            )
            .into());
        }
    };

    let catalog_writer = session.catalog_writer()?;
    catalog_writer
        .create_secret(
            stmt.secret_name.real_value(),
            database_id,
            schema_id,
            session.user_id(),
            secret_payload,
        )
        .await?;

    Ok(PgResponse::empty_result(StatementType::CREATE_SECRET))
}
