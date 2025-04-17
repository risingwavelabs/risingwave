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

use anyhow::anyhow;
use pgwire::pg_response::StatementType;
use prost::Message;
use risingwave_common::bail_not_implemented;
use risingwave_common::license::Feature;
use risingwave_common::secret::LocalSecretManager;
use risingwave_pb::secret::secret;
use risingwave_sqlparser::ast::{AlterSecretOperation, ObjectName, SqlOption};

use super::create_secret::{get_secret_payload, secret_to_str};
use super::drop_secret::fetch_secret_catalog_with_db_schema_id;
use crate::WithOptions;
use crate::error::Result;
use crate::handler::{HandlerArgs, RwPgResponse};

pub async fn handle_alter_secret(
    handler_args: HandlerArgs,
    secret_name: ObjectName,
    sql_options: Vec<SqlOption>,
    operation: AlterSecretOperation,
) -> Result<RwPgResponse> {
    Feature::SecretManagement
        .check_available()
        .map_err(|e| anyhow::anyhow!(e))?;

    let session = handler_args.session;

    if let Some((secret_catalog, _, _)) =
        fetch_secret_catalog_with_db_schema_id(&session, &secret_name, false)?
    {
        let AlterSecretOperation::ChangeCredential { new_credential } = operation;

        let secret_id = secret_catalog.id.secret_id();
        let secret_payload = if sql_options.is_empty() {
            let original_pb_secret_bytes = LocalSecretManager::global()
                .get_secret(secret_id)
                .ok_or(anyhow!(
                    "Failed to get secret in secret manager, secret_id: {}",
                    secret_id
                ))?;
            let original_secret_backend =
                LocalSecretManager::get_pb_secret_backend(&original_pb_secret_bytes)?;
            match original_secret_backend {
                secret::SecretBackend::Meta(_) => {
                    let new_secret_value_bytes =
                        secret_to_str(&new_credential)?.as_bytes().to_vec();
                    let secret_payload = risingwave_pb::secret::Secret {
                        secret_backend: Some(risingwave_pb::secret::secret::SecretBackend::Meta(
                            risingwave_pb::secret::SecretMetaBackend {
                                value: new_secret_value_bytes,
                            },
                        )),
                    };
                    secret_payload.encode_to_vec()
                }
                secret::SecretBackend::HashicorpVault(_) => {
                    bail_not_implemented!("hashicorp_vault backend is not implemented yet")
                }
            }
        } else {
            let with_options = WithOptions::try_from(sql_options.as_ref() as &[SqlOption])?;
            get_secret_payload(new_credential, with_options)?
        };

        let catalog_writer = session.catalog_writer()?;

        catalog_writer
            .alter_secret(
                secret_id,
                secret_catalog.name.clone(),
                secret_catalog.database_id,
                secret_catalog.schema_id,
                secret_catalog.owner,
                secret_payload,
            )
            .await?;

        Ok(RwPgResponse::empty_result(StatementType::ALTER_SECRET))
    } else {
        Ok(RwPgResponse::builder(StatementType::ALTER_SECRET)
            .notice(format!(
                "secret \"{}\" does not exist, skipping",
                secret_name
            ))
            .into())
    }
}
