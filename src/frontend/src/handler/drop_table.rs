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

use anyhow::Context;
use pgwire::pg_response::{PgResponse, StatementType};
use risingwave_common::catalog::Engine;
use risingwave_common::util::tokio_util::either::Either;
use risingwave_connector::sink::iceberg::IcebergConfig;
use risingwave_connector::source::ConnectorProperties;
use risingwave_sqlparser::ast::{Ident, ObjectName};
use tracing::warn;

use super::RwPgResponse;
use crate::binder::Binder;
use crate::catalog::root_catalog::SchemaPath;
use crate::catalog::table_catalog::{ICEBERG_SINK_PREFIX, ICEBERG_SOURCE_PREFIX, TableType};
use crate::error::Result;
use crate::handler::HandlerArgs;

pub async fn handle_drop_table(
    handler_args: HandlerArgs,
    table_name: ObjectName,
    if_exists: bool,
    cascade: bool,
) -> Result<RwPgResponse> {
    let session = handler_args.session.clone();
    let db_name = &session.database();
    let (schema_name, table_name) = Binder::resolve_schema_qualified_name(db_name, table_name)?;
    let search_path = session.config().search_path();
    let user_name = &session.user_name();

    let schema_path = SchemaPath::new(schema_name.as_deref(), &search_path, user_name);

    let (source_id, table_id, engine) = {
        let reader = session.env().catalog_reader().read_guard();
        let (table, schema_name) =
            match reader.get_created_table_by_name(db_name, schema_path, &table_name) {
                Ok((t, s)) => (t, s),
                Err(e) => {
                    return if if_exists {
                        Ok(RwPgResponse::builder(StatementType::DROP_TABLE)
                            .notice(format!("table \"{}\" does not exist, skipping", table_name))
                            .into())
                    } else {
                        Err(e.into())
                    };
                }
            };

        session.check_privilege_for_drop_alter(schema_name, &**table)?;

        if table.table_type() != TableType::Table {
            return Err(table.bad_drop_error());
        }
        (table.associated_source_id(), table.id(), table.engine)
    };

    match engine {
        Engine::Iceberg => {
            let either = if let Ok(source) = session
                .env()
                .catalog_reader()
                .read_guard()
                .get_source_by_name(
                    db_name,
                    schema_path,
                    &(ICEBERG_SOURCE_PREFIX.to_owned() + &table_name),
                )
                .map(|(source, _)| source.clone())
            {
                let config = ConnectorProperties::extract(source.with_properties.clone(), false)?;
                if let ConnectorProperties::Iceberg(iceberg_properties) = config {
                    Some(Either::Left(iceberg_properties))
                } else {
                    unreachable!("must be iceberg source");
                }
            } else if let Ok(sink) = session
                .env()
                .catalog_reader()
                .read_guard()
                .get_created_sink_by_name(
                    db_name,
                    schema_path,
                    &(ICEBERG_SINK_PREFIX.to_owned() + &table_name),
                )
                .map(|(sink, _)| sink.clone())
            {
                // If iceberg source does not exist, use iceberg sink to load iceberg table
                let iceberg_config = IcebergConfig::from_btreemap(sink.properties.clone())?;
                Some(Either::Right(iceberg_config))
            } else {
                None
            };

            // TODO(iceberg): make iceberg engine table drop ddl atomic
            // Drop sink
            // Drop iceberg table
            //   - Purge table from warehouse
            //   - Drop table from catalog
            // Drop source
            crate::handler::drop_sink::handle_drop_sink(
                handler_args.clone(),
                ObjectName::from(match schema_name {
                    Some(ref schema) => vec![
                        Ident::from(schema.as_str()),
                        Ident::from((ICEBERG_SINK_PREFIX.to_owned() + &table_name).as_str()),
                    ],
                    None => vec![Ident::from(
                        (ICEBERG_SINK_PREFIX.to_owned() + &table_name).as_str(),
                    )],
                }),
                true,
                false,
            )
            .await?;

            if let Some(either) = either {
                let (iceberg_catalog, table_id) = match either {
                    Either::Left(iceberg_properties) => {
                        let catalog = iceberg_properties.create_catalog().await?;
                        let table_id = iceberg_properties
                            .common
                            .full_table_name()
                            .context("Unable to parse table name")?;
                        (catalog, table_id)
                    }
                    Either::Right(iceberg_config) => {
                        let catalog = iceberg_config.create_catalog().await?;
                        let table_id = iceberg_config
                            .full_table_name()
                            .context("Unable to parse table name")?;
                        (catalog, table_id)
                    }
                };

                // For JNI catalog and storage catalog, drop table will purge the table as well.
                iceberg_catalog
                    .drop_table(&table_id)
                    .await
                    .context("failed to drop iceberg table")?;

                crate::handler::drop_source::handle_drop_source(
                    handler_args.clone(),
                    ObjectName::from(match schema_name {
                        Some(ref schema) => vec![
                            Ident::from(schema.as_str()),
                            Ident::from((ICEBERG_SOURCE_PREFIX.to_owned() + &table_name).as_str()),
                        ],
                        None => vec![Ident::from(
                            (ICEBERG_SOURCE_PREFIX.to_owned() + &table_name).as_str(),
                        )],
                    }),
                    true,
                    false,
                )
                .await?;
            } else {
                warn!(
                    "Table {} with iceberg engine but with no source and sink. It might be created partially. Please check it with iceberg catalog",
                    table_name
                );
            }
        }
        Engine::Hummock => {}
    }

    let catalog_writer = session.catalog_writer()?;
    catalog_writer
        .drop_table(source_id.map(|id| id.table_id), table_id, cascade)
        .await?;

    Ok(PgResponse::empty_result(StatementType::DROP_TABLE))
}

#[cfg(test)]
mod tests {
    use risingwave_common::catalog::{DEFAULT_DATABASE_NAME, DEFAULT_SCHEMA_NAME};

    use crate::catalog::root_catalog::SchemaPath;
    use crate::test_utils::LocalFrontend;

    #[tokio::test]
    async fn test_drop_table_handler() {
        let sql_create_table = "create table t (v1 smallint);";
        let sql_drop_table = "drop table t;";
        let frontend = LocalFrontend::new(Default::default()).await;
        frontend.run_sql(sql_create_table).await.unwrap();
        frontend.run_sql(sql_drop_table).await.unwrap();

        let session = frontend.session_ref();
        let catalog_reader = session.env().catalog_reader().read_guard();
        let schema_path = SchemaPath::Name(DEFAULT_SCHEMA_NAME);

        let source = catalog_reader.get_source_by_name(DEFAULT_DATABASE_NAME, schema_path, "t");
        assert!(source.is_err());

        let table =
            catalog_reader.get_created_table_by_name(DEFAULT_DATABASE_NAME, schema_path, "t");
        assert!(table.is_err());
    }
}
