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
use prost::Message;
use risingwave_common::bail_not_implemented;
use risingwave_common::license::Feature;
use risingwave_sqlparser::ast::{CreateSecretStatement, SqlOption, Value};

use crate::error::{ErrorCode, Result};
use crate::handler::{HandlerArgs, RwPgResponse};
use crate::{Binder, WithOptions};

const SECRET_BACKEND_KEY: &str = "backend";

const SECRET_BACKEND_META: &str = "meta";
const SECRET_BACKEND_HASHICORP_VAULT: &str = "hashicorp_vault";

pub async fn handle_create_secret(
    handler_args: HandlerArgs,
    stmt: CreateSecretStatement,
) -> Result<RwPgResponse> {
    Feature::SecretManagement
        .check_available()
        .map_err(|e| anyhow::anyhow!(e))?;

    let session = handler_args.session.clone();
    let db_name = &session.database();
    let (schema_name, secret_name) =
        Binder::resolve_schema_qualified_name(db_name, stmt.secret_name.clone())?;

    if let Err(e) = session.check_secret_name_duplicated(stmt.secret_name.clone()) {
        return if stmt.if_not_exists {
            Ok(PgResponse::builder(StatementType::CREATE_SECRET)
                .notice(format!("secret \"{}\" exists, skipping", secret_name))
                .into())
        } else {
            Err(e)
        };
    }
    let with_options = WithOptions::try_from(stmt.with_properties.0.as_ref() as &[SqlOption])?;

    let secret_payload = get_secret_payload(stmt.credential, with_options)?;

    let (database_id, schema_id) = session.get_database_and_schema_id_for_create(schema_name)?;

    let catalog_writer = session.catalog_writer()?;
    catalog_writer
        .create_secret(
            secret_name,
            database_id,
            schema_id,
            session.user_id(),
            secret_payload,
        )
        .await?;

    Ok(PgResponse::empty_result(StatementType::CREATE_SECRET))
}

pub fn secret_to_str(value: &Value) -> Result<String> {
    match value {
        Value::DoubleQuotedString(s) | Value::SingleQuotedString(s) => Ok(s.clone()),
        _ => Err(ErrorCode::InvalidInputSyntax(
            "secret value should be quoted by ' or \" ".to_owned(),
        )
        .into()),
    }
}

pub(crate) fn get_secret_payload(credential: Value, with_options: WithOptions) -> Result<Vec<u8>> {
    let secret = secret_to_str(&credential)?.as_bytes().to_vec();

    if let Some(backend) = with_options.get(SECRET_BACKEND_KEY) {
        match backend.to_lowercase().as_ref() {
            SECRET_BACKEND_META => {
                let backend = risingwave_pb::secret::Secret {
                    secret_backend: Some(risingwave_pb::secret::secret::SecretBackend::Meta(
                        risingwave_pb::secret::SecretMetaBackend { value: secret },
                    )),
                };
                Ok(backend.encode_to_vec())
            }
            SECRET_BACKEND_HASHICORP_VAULT => {
                if credential != Value::Null {
                    return Err(ErrorCode::InvalidParameterValue(
                        "credential must be null for hashicorp_vault backend".to_owned(),
                    )
                    .into());
                }
                bail_not_implemented!("hashicorp_vault backend is not implemented yet")
            }
            _ => Err(ErrorCode::InvalidParameterValue(format!(
                "secret backend \"{}\" is not supported. Supported backends are: {}",
                backend,
                [SECRET_BACKEND_META, SECRET_BACKEND_HASHICORP_VAULT].join(",")
            ))
            .into()),
        }
    } else {
        Err(ErrorCode::InvalidParameterValue(format!(
            "secret backend is not specified in with clause. Supported backends are: {}",
            [SECRET_BACKEND_META, SECRET_BACKEND_HASHICORP_VAULT].join(",")
        ))
        .into())
    }
}
