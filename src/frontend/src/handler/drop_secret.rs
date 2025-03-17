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

use std::sync::Arc;

use pgwire::pg_response::StatementType;
use risingwave_common::license::Feature;
use risingwave_sqlparser::ast::ObjectName;

use crate::Binder;
use crate::catalog::root_catalog::SchemaPath;
use crate::catalog::secret_catalog::SecretCatalog;
use crate::catalog::{DatabaseId, SchemaId};
use crate::error::Result;
use crate::handler::{HandlerArgs, RwPgResponse};
use crate::session::SessionImpl;

pub async fn handle_drop_secret(
    handler_args: HandlerArgs,
    secret_name: ObjectName,
    if_exists: bool,
) -> Result<RwPgResponse> {
    Feature::SecretManagement
        .check_available()
        .map_err(|e| anyhow::anyhow!(e))?;

    let session = handler_args.session;

    if let Some((secret_catalog, _, _)) =
        fetch_secret_catalog_with_db_schema_id(&session, &secret_name, if_exists)?
    {
        let catalog_writer = session.catalog_writer()?;
        catalog_writer.drop_secret(secret_catalog.id).await?;

        Ok(RwPgResponse::empty_result(StatementType::DROP_SECRET))
    } else {
        Ok(RwPgResponse::builder(StatementType::DROP_SECRET)
            .notice(format!(
                "secret \"{}\" does not exist, skipping",
                secret_name
            ))
            .into())
    }
}

/// Fetch the secret catalog and the `database/schema_id` of the source.
pub fn fetch_secret_catalog_with_db_schema_id(
    session: &SessionImpl,
    secret_name: &ObjectName,
    if_exists: bool,
) -> Result<Option<(Arc<SecretCatalog>, DatabaseId, SchemaId)>> {
    let db_name = &session.database();
    let (schema_name, secret_name) =
        Binder::resolve_schema_qualified_name(db_name, secret_name.clone())?;
    let search_path = session.config().search_path();
    let user_name = &session.user_name();

    let schema_path = SchemaPath::new(schema_name.as_deref(), &search_path, user_name);

    let reader = session.env().catalog_reader().read_guard();
    match reader.get_secret_by_name(db_name, schema_path, &secret_name) {
        Ok((catalog, schema_name)) => {
            session.check_privilege_for_drop_alter(schema_name, &**catalog)?;

            let db = reader.get_database_by_name(db_name)?;
            let schema = db.get_schema_by_name(schema_name).unwrap();

            Ok(Some((Arc::clone(catalog), db.id(), schema.id())))
        }
        Err(e) => {
            if if_exists {
                Ok(None)
            } else {
                Err(e.into())
            }
        }
    }
}
