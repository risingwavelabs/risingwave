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
use prost::Message;
use risingwave_common::license::Feature;
use risingwave_common::secret::aws_secrets_manager_client::{
    AwsSecretsManagerClient, AwsSecretsManagerConfig,
};
use risingwave_common::secret::vault_client::{HashiCorpVaultClient, HashiCorpVaultConfig};
use risingwave_pb::secret::PbSecretRef;
use risingwave_sqlparser::ast::{CreateSecretStatement, SqlOption, Value};
use thiserror_ext::AsReport;

use crate::error::{ErrorCode, Result};
use crate::handler::{HandlerArgs, RwPgResponse};
use crate::session::SessionImpl;
use crate::utils::resolve_secret_ref_in_with_options;
use crate::{Binder, WithOptions};

const SECRET_BACKEND_KEY: &str = "backend";

const SECRET_BACKEND_META: &str = "meta";
const SECRET_BACKEND_HASHICORP_VAULT: &str = "hashicorp_vault";
const SECRET_BACKEND_AWS_SECRETS_MANAGER: &str = "aws_secrets_manager";

const SUPPORTED_SECRET_BACKENDS: [&str; 3] = [
    SECRET_BACKEND_META,
    SECRET_BACKEND_HASHICORP_VAULT,
    SECRET_BACKEND_AWS_SECRETS_MANAGER,
];

const AWS_ACCESS_KEY_ID_KEYS: [&str; 3] = [
    "s3.credentials.access",
    "access_key_id",
    "aws.credentials.access_key_id",
];
const AWS_SECRET_ACCESS_KEY_KEYS: [&str; 3] = [
    "s3.credentials.secret",
    "secret_access_key",
    "aws.credentials.secret_access_key",
];
const AWS_SESSION_TOKEN_KEYS: [&str; 2] = ["session_token", "aws.credentials.session_token"];

pub async fn handle_create_secret(
    handler_args: HandlerArgs,
    stmt: CreateSecretStatement,
) -> Result<RwPgResponse> {
    Feature::SecretManagement.check_available()?;

    let session = handler_args.session.clone();
    let db_name = &session.database();
    let (schema_name, secret_name) =
        Binder::resolve_schema_qualified_name(db_name, &stmt.secret_name)?;

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

    let (database_id, schema_id) = session.get_database_and_schema_id_for_create(schema_name)?;

    let secret_payload = get_secret_payload(stmt.credential, with_options, &session).await?;

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

pub(crate) async fn get_secret_payload(
    credential: Value,
    with_options: WithOptions,
    session: &SessionImpl,
) -> Result<Vec<u8>> {
    if let Some(backend) = with_options.get(SECRET_BACKEND_KEY) {
        match backend.to_lowercase().as_ref() {
            SECRET_BACKEND_META => {
                reject_secret_refs(&with_options)?;
                let secret = secret_to_str(&credential)?.as_bytes().to_vec();
                let backend = risingwave_pb::secret::Secret {
                    secret_backend: Some(risingwave_pb::secret::secret::SecretBackend::Meta(
                        risingwave_pb::secret::SecretMetaBackend { value: secret },
                    )),
                };
                Ok(backend.encode_to_vec())
            }
            SECRET_BACKEND_HASHICORP_VAULT => {
                reject_secret_refs(&with_options)?;
                if credential != Value::Null {
                    return Err(ErrorCode::InvalidParameterValue(
                        "credential must be null for hashicorp_vault backend".to_owned(),
                    )
                    .into());
                }

                // Convert WithOptions to a map for serde deserialization
                let mut config_map = std::collections::HashMap::new();
                for (key, value) in with_options.iter() {
                    config_map.insert(key.clone(), value.clone());
                }

                // Deserialize using serde with validation
                let config: HashiCorpVaultConfig =
                    serde_json::from_value(serde_json::Value::Object(
                        config_map
                            .into_iter()
                            .map(|(k, v)| (k, serde_json::Value::String(v)))
                            .collect(),
                    ))
                    .map_err(|e| {
                        ErrorCode::InvalidParameterValue(format!(
                            "Invalid HashiCorp Vault configuration: {}",
                            e.as_report()
                        ))
                    })?;

                {
                    // validate
                    let client = HashiCorpVaultClient::new(config.clone())?;
                    client.get_secret().await?;
                }

                let backend = risingwave_pb::secret::Secret {
                    secret_backend: Some(
                        risingwave_pb::secret::secret::SecretBackend::HashicorpVault(
                            config.to_protobuf(),
                        ),
                    ),
                };
                Ok(backend.encode_to_vec())
            }
            SECRET_BACKEND_AWS_SECRETS_MANAGER => {
                if credential != Value::Null {
                    return Err(ErrorCode::InvalidParameterValue(
                        "credential must be null for aws_secrets_manager backend".to_owned(),
                    )
                    .into());
                }

                let config = build_aws_secrets_manager_config(with_options, session)?;

                {
                    // Validate that the AWS secret is readable before storing the backend config.
                    let client = AwsSecretsManagerClient::new(config.clone()).await?;
                    client.get_secret().await?;
                }

                let backend = risingwave_pb::secret::Secret {
                    secret_backend: Some(
                        risingwave_pb::secret::secret::SecretBackend::AwsSecretsManager(
                            config.to_protobuf(),
                        ),
                    ),
                };
                Ok(backend.encode_to_vec())
            }
            _ => Err(ErrorCode::InvalidParameterValue(format!(
                "secret backend \"{}\" is not supported. Supported backends are: {}",
                backend,
                SUPPORTED_SECRET_BACKENDS.join(",")
            ))
            .into()),
        }
    } else {
        Err(ErrorCode::InvalidParameterValue(format!(
            "secret backend is not specified in with clause. Supported backends are: {}",
            SUPPORTED_SECRET_BACKENDS.join(",")
        ))
        .into())
    }
}

fn reject_secret_refs(with_options: &WithOptions) -> Result<()> {
    if !with_options.secret_ref().is_empty() {
        return Err(ErrorCode::InvalidParameterValue(
            "Secret references are not allowed when creating this secret backend".to_owned(),
        )
        .into());
    }
    Ok(())
}

fn build_aws_secrets_manager_config(
    with_options: WithOptions,
    session: &SessionImpl,
) -> Result<AwsSecretsManagerConfig> {
    validate_no_duplicate_aws_options(&with_options)?;
    validate_aws_credential_secret_refs(&with_options)?;
    let (options, mut secret_refs) =
        resolve_secret_ref_in_with_options(with_options, session)?.into_parts();
    let mut options = options;
    options.remove(SECRET_BACKEND_KEY);

    let mut config: AwsSecretsManagerConfig = serde_json::from_value(serde_json::Value::Object(
        options
            .into_iter()
            .map(|(k, v)| (k, serde_json::Value::String(v)))
            .collect(),
    ))
    .map_err(|e| {
        ErrorCode::InvalidParameterValue(format!(
            "Invalid AWS Secrets Manager configuration: {}",
            e.as_report()
        ))
    })?;

    let access_key_id_ref = take_first_secret_ref(&mut secret_refs, &AWS_ACCESS_KEY_ID_KEYS);
    let secret_access_key_ref =
        take_first_secret_ref(&mut secret_refs, &AWS_SECRET_ACCESS_KEY_KEYS);
    let session_token_ref = take_first_secret_ref(&mut secret_refs, &AWS_SESSION_TOKEN_KEYS);
    if !secret_refs.is_empty() {
        return Err(ErrorCode::InvalidParameterValue(format!(
            "Unsupported SECRET references in aws_secrets_manager backend: {}",
            secret_refs.keys().cloned().collect::<Vec<_>>().join(",")
        ))
        .into());
    }
    config.set_credential_refs(access_key_id_ref, secret_access_key_ref, session_token_ref);
    if config.aws_config.endpoint_url.is_some() && config.aws_config.session_token_ref.is_some() {
        return Err(ErrorCode::InvalidParameterValue(
            "session_token SECRET reference cannot be used with endpoint_url".to_owned(),
        )
        .into());
    }
    Ok(config)
}

fn validate_aws_credential_secret_refs(with_options: &WithOptions) -> Result<()> {
    for (key, secret_ref) in with_options.secret_ref() {
        if !is_aws_credential_secret_key(key) {
            return Err(ErrorCode::InvalidParameterValue(format!(
                "SECRET reference is not allowed for aws_secrets_manager option `{}`",
                key
            ))
            .into());
        }
        if secret_ref.ref_as != risingwave_sqlparser::ast::SecretRefAsType::Text {
            return Err(ErrorCode::InvalidParameterValue(format!(
                "SECRET reference for aws_secrets_manager option `{}` must be text, not AS FILE",
                key
            ))
            .into());
        }
    }
    Ok(())
}

fn validate_no_duplicate_aws_options(with_options: &WithOptions) -> Result<()> {
    let mut seen = std::collections::BTreeMap::new();
    for key in with_options
        .keys()
        .map(String::as_str)
        .chain(with_options.secret_ref().keys().map(String::as_str))
    {
        let Some(canonical_key) = canonical_aws_option_key(key) else {
            continue;
        };
        if let Some(previous_key) = seen.insert(canonical_key, key) {
            return Err(ErrorCode::InvalidParameterValue(format!(
                "Duplicate AWS Secrets Manager option `{}` and `{}` both map to `{}`",
                previous_key, key, canonical_key
            ))
            .into());
        }
    }
    Ok(())
}

fn canonical_aws_option_key(key: &str) -> Option<&'static str> {
    match key {
        SECRET_BACKEND_KEY => None,
        "secret_id" => Some("secret_id"),
        "s3.region_name" | "region" | "aws.region" => Some("region"),
        "field" => Some("field"),
        "version_id" => Some("version_id"),
        "version_stage" => Some("version_stage"),
        "s3.endpoint_url" | "s3.endpoint" | "endpoint_url" | "aws.endpoint_url" => {
            Some("endpoint_url")
        }
        "s3.credentials.access" | "access_key_id" | "aws.credentials.access_key_id" => {
            Some("access")
        }
        "s3.credentials.secret" | "secret_access_key" | "aws.credentials.secret_access_key" => {
            Some("secret")
        }
        "session_token" | "aws.credentials.session_token" => Some("session_token"),
        "s3.assume_role" | "role_arn" | "aws.credentials.role.arn" => Some("assume_role"),
        "external_id" | "aws.credentials.role.external_id" => Some("external_id"),
        "profile" | "aws.profile" => Some("profile"),
        "enable_config_load" => Some("enable_config_load"),
        _ => None,
    }
}

fn is_aws_credential_secret_key(key: &str) -> bool {
    AWS_ACCESS_KEY_ID_KEYS.contains(&key)
        || AWS_SECRET_ACCESS_KEY_KEYS.contains(&key)
        || AWS_SESSION_TOKEN_KEYS.contains(&key)
}

fn take_first_secret_ref(
    secret_refs: &mut std::collections::BTreeMap<String, PbSecretRef>,
    keys: &[&str],
) -> Option<PbSecretRef> {
    keys.iter().find_map(|key| secret_refs.remove(*key))
}
