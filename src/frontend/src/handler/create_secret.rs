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

                // Parse required parameters
                let addr = with_options
                    .get("addr")
                    .ok_or_else(|| {
                        ErrorCode::InvalidParameterValue(
                            "'addr' is required for hashicorp_vault backend".to_owned(),
                        )
                    })?
                    .clone();

                let path = with_options
                    .get("path")
                    .ok_or_else(|| {
                        ErrorCode::InvalidParameterValue(
                            "'path' is required for hashicorp_vault backend".to_owned(),
                        )
                    })?
                    .clone();

                let field = with_options
                    .get("field")
                    .cloned()
                    .unwrap_or_else(|| "value".to_owned());

                // Parse auth method
                let auth_method = with_options
                    .get("auth_method")
                    .ok_or_else(|| {
                        ErrorCode::InvalidParameterValue(
                            "'auth_method' is required for hashicorp_vault backend".to_owned(),
                        )
                    })?
                    .to_lowercase();

                // Parse optional parameters
                let tls_skip_verify = with_options
                    .get("tls_skip_verify")
                    .map(|v| v.to_lowercase() == "true")
                    .unwrap_or(false);

                let cache_ttl_secs = with_options
                    .get("cache_ttl_secs")
                    .map(|v| v.parse::<u32>())
                    .transpose()
                    .map_err(|_| {
                        ErrorCode::InvalidParameterValue(
                            "'cache_ttl_secs' must be a valid integer".to_owned(),
                        )
                    })?
                    .unwrap_or(0);

                // Create the auth oneof
                let auth_oneof = match auth_method.as_str() {
                    "token" => {
                        let token = with_options
                            .get("auth_token")
                            .ok_or_else(|| {
                                ErrorCode::InvalidParameterValue(
                                    "'auth_token' is required for token auth method".to_owned(),
                                )
                            })?
                            .clone();
                        Some(
                            risingwave_pb::secret::secret_hashicorp_vault_backend::Auth::TokenAuth(
                                risingwave_pb::secret::VaultTokenAuth { token },
                            ),
                        )
                    }
                    "approle" => {
                        let role_id = with_options
                            .get("auth_role_id")
                            .ok_or_else(|| {
                                ErrorCode::InvalidParameterValue(
                                    "'auth_role_id' is required for approle auth method".to_owned(),
                                )
                            })?
                            .clone();
                        let secret_id = with_options
                            .get("auth_secret_id")
                            .ok_or_else(|| {
                                ErrorCode::InvalidParameterValue(
                                    "'auth_secret_id' is required for approle auth method"
                                        .to_owned(),
                                )
                            })?
                            .clone();
                        Some(
                            risingwave_pb::secret::secret_hashicorp_vault_backend::Auth::ApproleAuth(
                                risingwave_pb::secret::VaultAppRoleAuth { role_id, secret_id },
                            ),
                        )
                    }
                    _ => {
                        return Err(ErrorCode::InvalidParameterValue(format!(
                            "Unsupported auth method: {}. Supported methods are: token, approle",
                            auth_method
                        ))
                        .into());
                    }
                };

                let backend = risingwave_pb::secret::Secret {
                    secret_backend: Some(
                        risingwave_pb::secret::secret::SecretBackend::HashicorpVault(
                            risingwave_pb::secret::SecretHashicorpVaultBackend {
                                addr,
                                path,
                                field,
                                auth: auth_oneof,
                                tls_skip_verify,
                                cache_ttl_secs,
                            },
                        ),
                    ),
                };
                Ok(backend.encode_to_vec())
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
