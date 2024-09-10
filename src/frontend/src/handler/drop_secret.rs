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
use risingwave_sqlparser::ast::ObjectName;

use crate::catalog::root_catalog::SchemaPath;
use crate::error::Result;
use crate::handler::{HandlerArgs, RwPgResponse};
use crate::Binder;

pub async fn handle_drop_secret(
    handler_args: HandlerArgs,
    secret_name: ObjectName,
    if_exists: bool,
) -> Result<RwPgResponse> {
    Feature::SecretManagement
        .check_available()
        .map_err(|e| anyhow::anyhow!(e))?;

    let session = handler_args.session;
    let db_name = session.database();
    let (schema_name, secret_name) = Binder::resolve_schema_qualified_name(db_name, secret_name)?;
    let search_path = session.config().search_path();
    let user_name = &session.auth_context().user_name;
    let schema_path = SchemaPath::new(schema_name.as_deref(), &search_path, user_name);

    let secret_id = {
        let reader = session.env().catalog_reader().read_guard();
        let (secret, schema_name) =
            match reader.get_secret_by_name(db_name, schema_path, secret_name.as_str()) {
                Ok((c, s)) => (c, s),
                Err(e) => {
                    return if if_exists {
                        Ok(RwPgResponse::builder(StatementType::DROP_SECRET)
                            .notice(format!(
                                "secret \"{}\" does not exist, skipping",
                                secret_name
                            ))
                            .into())
                    } else {
                        Err(e.into())
                    };
                }
            };
        session.check_privilege_for_drop_alter(schema_name, &**secret)?;

        secret.id
    };

    let catalog_writer = session.catalog_writer()?;
    catalog_writer.drop_secret(secret_id).await?;

    Ok(RwPgResponse::empty_result(StatementType::DROP_SECRET))
}
