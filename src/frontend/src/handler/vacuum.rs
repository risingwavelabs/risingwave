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
use risingwave_common::bail;
use risingwave_common::catalog::Engine;
use risingwave_connector::sink::CONNECTOR_TYPE_KEY;
use risingwave_sqlparser::ast::ObjectName;

use crate::binder::Binder;
use crate::error::{ErrorCode, Result, RwError};
use crate::handler::{HandlerArgs, RwPgResponse};

pub async fn handle_vacuum(
    handler_args: HandlerArgs,
    object_name: ObjectName,
) -> Result<RwPgResponse> {
    let session = &handler_args.session;
    let db_name = &session.database();

    let sink_id = {
        let (schema_name, real_object_name) =
            Binder::resolve_schema_qualified_name(db_name, &object_name)?;
        let catalog_reader = session.env().catalog_reader().read_guard();
        let search_path = session.config().search_path();
        let user_name = session.user_name();
        let schema_path = crate::catalog::root_catalog::SchemaPath::new(
            schema_name.as_deref(),
            &search_path,
            &user_name,
        );

        if let Ok((table, _)) =
            catalog_reader.get_created_table_by_name(db_name, schema_path, &real_object_name)
        {
            if table.engine() == Engine::Iceberg {
                // For iceberg engine table, get the associated iceberg sink name
                let sink_name = table.iceberg_sink_name().ok_or_else(|| {
                    RwError::from(ErrorCode::CatalogError(
                        format!("No iceberg sink name found for table {}", real_object_name).into(),
                    ))
                })?;

                // Find the iceberg sink
                let (sink, _) = catalog_reader
                    .get_created_sink_by_name(db_name, schema_path, &sink_name)
                    .map_err(|_| {
                        RwError::from(ErrorCode::CatalogError(
                            format!(
                                "Iceberg sink {} not found for table {}",
                                sink_name, real_object_name
                            )
                            .into(),
                        ))
                    })?;

                sink.id.sink_id()
            } else {
                return Err(ErrorCode::InvalidInputSyntax(format!(
                    "VACUUM can only be used on Iceberg engine tables or Iceberg sinks, but table '{}' uses {:?} engine",
                    real_object_name,
                    table.engine()
                ))
                    .into());
            }
        } else if let Ok((sink, _)) =
            catalog_reader.get_created_sink_by_name(db_name, schema_path, &real_object_name)
        {
            if let Some(connector_type) = sink.properties.get(CONNECTOR_TYPE_KEY) {
                if connector_type == "iceberg" {
                    sink.id.sink_id()
                } else {
                    return Err(ErrorCode::InvalidInputSyntax(format!(
                        "VACUUM can only be used on Iceberg sinks, but sink '{}' is of type '{}'",
                        real_object_name, connector_type
                    ))
                    .into());
                }
            } else {
                return Err(ErrorCode::InvalidInputSyntax(format!(
                    "VACUUM can only be used on Iceberg sinks, but sink '{}' has no connector type specified",
                    real_object_name
                ))
                    .into());
            }
        } else {
            bail!("object {} not found", real_object_name);
        }
    };

    session
        .env()
        .meta_client()
        .compact_iceberg_table(sink_id)
        .await?;
    Ok(PgResponse::builder(StatementType::VACUUM).into())
}
