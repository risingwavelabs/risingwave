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

use pgwire::pg_response::StatementType;
use risingwave_common::license::Feature;
use risingwave_sqlparser::ast::{AlterSecretOperation, ObjectName, SqlOption};

use super::create_secret::get_secret_payload;
use super::drop_secret::fetch_secret_catalog_with_db_schema_id;
use crate::error::Result;
use crate::handler::{HandlerArgs, RwPgResponse};
use crate::WithOptions;

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

        let with_options = WithOptions::try_from(sql_options.as_ref() as &[SqlOption])?;

        let secret_payload = get_secret_payload(new_credential, with_options)?;

        let catalog_writer = session.catalog_writer()?;

        catalog_writer
            .alter_secret(
                secret_catalog.id.secret_id(),
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
