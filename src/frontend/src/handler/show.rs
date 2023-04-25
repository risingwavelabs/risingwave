// Copyright 2023 RisingWave Labs
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

use itertools::Itertools;
use pgwire::pg_field_descriptor::PgFieldDescriptor;
use pgwire::pg_response::{PgResponse, StatementType};
use pgwire::types::Row;
use risingwave_common::catalog::{ColumnDesc, DEFAULT_SCHEMA_NAME};
use risingwave_common::error::{ErrorCode, Result};
use risingwave_common::types::DataType;
use risingwave_connector::source::kafka::PRIVATELINK_CONNECTION;
use risingwave_pb::catalog::connection;
use risingwave_sqlparser::ast::{Ident, ObjectName, ShowCreateType, ShowObject};
use serde_json;

use super::RwPgResponse;
use crate::binder::{Binder, Relation};
use crate::catalog::CatalogError;
use crate::handler::util::col_descs_to_rows;
use crate::handler::HandlerArgs;
use crate::session::SessionImpl;

pub fn get_columns_from_table(
    session: &SessionImpl,
    table_name: ObjectName,
) -> Result<Vec<ColumnDesc>> {
    let mut binder = Binder::new_for_system(session);
    let relation = binder.bind_relation_by_name(table_name.clone(), None, false)?;
    let catalogs = match relation {
        Relation::Source(s) => s.catalog.columns,
        Relation::BaseTable(t) => t.table_catalog.columns,
        Relation::SystemTable(t) => t.sys_table_catalog.columns,
        _ => {
            return Err(CatalogError::NotFound("table or source", table_name.to_string()).into());
        }
    };

    Ok(catalogs
        .iter()
        .filter(|c| !c.is_hidden)
        .map(|c| c.column_desc.clone())
        .collect())
}

fn schema_or_default(schema: &Option<Ident>) -> String {
    schema
        .as_ref()
        .map_or_else(|| DEFAULT_SCHEMA_NAME.to_string(), |s| s.real_value())
}

pub fn handle_show_object(handler_args: HandlerArgs, command: ShowObject) -> Result<RwPgResponse> {
    let session = handler_args.session;
    let catalog_reader = session.env().catalog_reader().read_guard();

    let names = match command {
        // If not include schema name, use default schema name
        ShowObject::Table { schema } => catalog_reader
            .get_schema_by_name(session.database(), &schema_or_default(&schema))?
            .iter_table()
            .map(|t| t.name.clone())
            .collect(),
        ShowObject::InternalTable { schema } => catalog_reader
            .get_schema_by_name(session.database(), &schema_or_default(&schema))?
            .iter_internal_table()
            .map(|t| t.name.clone())
            .collect(),
        ShowObject::Database => catalog_reader.get_all_database_names(),
        ShowObject::Schema => catalog_reader.get_all_schema_names(session.database())?,
        ShowObject::View { schema } => catalog_reader
            .get_schema_by_name(session.database(), &schema_or_default(&schema))?
            .iter_view()
            .map(|t| t.name.clone())
            .collect(),
        ShowObject::MaterializedView { schema } => catalog_reader
            .get_schema_by_name(session.database(), &schema_or_default(&schema))?
            .iter_mv()
            .map(|t| t.name.clone())
            .collect(),
        ShowObject::Source { schema } => catalog_reader
            .get_schema_by_name(session.database(), &schema_or_default(&schema))?
            .iter_source()
            .filter(|t| t.associated_table_id.is_none())
            .map(|t| t.name.clone())
            .collect(),
        ShowObject::Sink { schema } => catalog_reader
            .get_schema_by_name(session.database(), &schema_or_default(&schema))?
            .iter_sink()
            .map(|t| t.name.clone())
            .collect(),
        ShowObject::Columns { table } => {
            let columns = get_columns_from_table(&session, table)?;
            let rows = col_descs_to_rows(columns);

            return Ok(PgResponse::new_for_stream(
                StatementType::SHOW_COMMAND,
                None,
                rows.into(),
                vec![
                    PgFieldDescriptor::new(
                        "Name".to_owned(),
                        DataType::Varchar.to_oid(),
                        DataType::Varchar.type_len(),
                    ),
                    PgFieldDescriptor::new(
                        "Type".to_owned(),
                        DataType::Varchar.to_oid(),
                        DataType::Varchar.type_len(),
                    ),
                ],
            ));
        }
        ShowObject::Connection { schema } => {
            let schema = catalog_reader
                .get_schema_by_name(session.database(), &schema_or_default(&schema))?;
            let rows = schema
                .iter_connections()
                .map(|c| {
                    let name = c.name.clone();
                    let conn_type = match &c.info {
                        connection::Info::PrivateLinkService(_) => {
                            PRIVATELINK_CONNECTION.to_string()
                        },
                    };
                    let source_names = schema
                        .get_source_ids_by_connection(c.id)
                        .unwrap_or(Vec::new())
                        .into_iter()
                        .filter_map(|sid| schema.get_source_by_id(&sid).map(|catalog| catalog.name.as_str()))
                        .collect_vec();
                    let sink_names = schema
                        .get_sink_ids_by_connection(c.id)
                        .unwrap_or(Vec::new())
                        .into_iter()
                        .filter_map(|sid| schema.get_sink_by_id(&sid).map(|catalog| catalog.name.as_str()))
                        .collect_vec();
                    let properties = match &c.info {
                        connection::Info::PrivateLinkService(i) => {
                            format!(
                                "provider: {}\nservice_name: {}\nendpoint_id: {}\navailability_zones: {}\nsources: {}\nsinks: {}",
                                i.get_provider().unwrap().as_str_name(),
                                i.service_name,
                                i.endpoint_id,
                                serde_json::to_string(&i.dns_entries.keys().collect_vec()).unwrap(),
                                serde_json::to_string(&source_names).unwrap(),
                                serde_json::to_string(&sink_names).unwrap(),
                            )
                        }
                    };
                    Row::new(vec![
                        Some(name.into()),
                        Some(conn_type.into()),
                        Some(properties.into()),
                    ])
                })
                .collect_vec();
            return Ok(PgResponse::new_for_stream(
                StatementType::SHOW_COMMAND,
                None,
                rows.into(),
                vec![
                    PgFieldDescriptor::new(
                        "Name".to_owned(),
                        DataType::Varchar.to_oid(),
                        DataType::Varchar.type_len(),
                    ),
                    PgFieldDescriptor::new(
                        "Type".to_owned(),
                        DataType::Varchar.to_oid(),
                        DataType::Varchar.type_len(),
                    ),
                    PgFieldDescriptor::new(
                        "Properties".to_owned(),
                        DataType::Varchar.to_oid(),
                        DataType::Varchar.type_len(),
                    ),
                ],
            ));
        }
        ShowObject::Function { schema } => {
            let rows = catalog_reader
                .get_schema_by_name(session.database(), &schema_or_default(&schema))?
                .iter_function()
                .map(|t| {
                    Row::new(vec![
                        Some(t.name.clone().into()),
                        Some(t.arg_types.iter().map(|t| t.to_string()).join(", ").into()),
                        Some(t.return_type.to_string().into()),
                        Some(t.language.clone().into()),
                        Some(t.link.clone().into()),
                    ])
                })
                .collect_vec();
            return Ok(PgResponse::new_for_stream(
                StatementType::SHOW_COMMAND,
                None,
                rows.into(),
                vec![
                    PgFieldDescriptor::new(
                        "Name".to_owned(),
                        DataType::Varchar.to_oid(),
                        DataType::Varchar.type_len(),
                    ),
                    PgFieldDescriptor::new(
                        "Arguments".to_owned(),
                        DataType::Varchar.to_oid(),
                        DataType::Varchar.type_len(),
                    ),
                    PgFieldDescriptor::new(
                        "Return Type".to_owned(),
                        DataType::Varchar.to_oid(),
                        DataType::Varchar.type_len(),
                    ),
                    PgFieldDescriptor::new(
                        "Language".to_owned(),
                        DataType::Varchar.to_oid(),
                        DataType::Varchar.type_len(),
                    ),
                    PgFieldDescriptor::new(
                        "Link".to_owned(),
                        DataType::Varchar.to_oid(),
                        DataType::Varchar.type_len(),
                    ),
                ],
            ));
        }
    };

    let rows = names
        .into_iter()
        .map(|n| Row::new(vec![Some(n.into())]))
        .collect_vec();

    Ok(PgResponse::new_for_stream(
        StatementType::SHOW_COMMAND,
        None,
        rows.into(),
        vec![PgFieldDescriptor::new(
            "Name".to_owned(),
            DataType::Varchar.to_oid(),
            DataType::Varchar.type_len(),
        )],
    ))
}

pub fn handle_show_create_object(
    handle_args: HandlerArgs,
    show_create_type: ShowCreateType,
    name: ObjectName,
) -> Result<RwPgResponse> {
    let session = handle_args.session;
    let catalog_reader = session.env().catalog_reader().read_guard();
    let (schema_name, object_name) =
        Binder::resolve_schema_qualified_name(session.database(), name.clone())?;
    let schema_name = schema_name.unwrap_or(DEFAULT_SCHEMA_NAME.to_string());
    let schema = catalog_reader.get_schema_by_name(session.database(), &schema_name)?;
    let sql = match show_create_type {
        ShowCreateType::MaterializedView => {
            let mv = schema
                .get_table_by_name(&object_name)
                .filter(|t| t.is_mview())
                .ok_or_else(|| CatalogError::NotFound("materialized view", name.to_string()))?;
            mv.create_sql()
        }
        ShowCreateType::View => {
            let view = schema
                .get_view_by_name(&object_name)
                .ok_or_else(|| CatalogError::NotFound("view", name.to_string()))?;
            view.create_sql()
        }
        ShowCreateType::Table => {
            let table = schema
                .get_table_by_name(&object_name)
                .filter(|t| t.is_table())
                .ok_or_else(|| CatalogError::NotFound("table", name.to_string()))?;
            table.create_sql()
        }
        ShowCreateType::Sink => {
            let sink = schema
                .get_sink_by_name(&object_name)
                .ok_or_else(|| CatalogError::NotFound("sink", name.to_string()))?;
            sink.create_sql()
        }
        ShowCreateType::Source => {
            let source = schema
                .get_source_by_name(&object_name)
                .filter(|s| s.associated_table_id.is_none())
                .ok_or_else(|| CatalogError::NotFound("source", name.to_string()))?;
            source.create_sql()
        }
        _ => {
            return Err(ErrorCode::NotImplemented(
                format!("show create on: {}", show_create_type),
                None.into(),
            )
            .into());
        }
    };
    let name = format!("{}.{}", schema_name, object_name);

    Ok(PgResponse::new_for_stream(
        StatementType::SHOW_COMMAND,
        None,
        vec![Row::new(vec![Some(name.into()), Some(sql.into())])].into(),
        vec![
            PgFieldDescriptor::new(
                "Name".to_owned(),
                DataType::Varchar.to_oid(),
                DataType::Varchar.type_len(),
            ),
            PgFieldDescriptor::new(
                "Create Sql".to_owned(),
                DataType::Varchar.to_oid(),
                DataType::Varchar.type_len(),
            ),
        ],
    ))
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::ops::Index;

    use futures_async_stream::for_await;

    use crate::test_utils::{create_proto_file, LocalFrontend, PROTO_FILE_DATA};

    #[tokio::test]
    async fn test_show_source() {
        let frontend = LocalFrontend::new(Default::default()).await;

        let sql = r#"CREATE SOURCE t1
        WITH (connector = 'kafka', kafka.topic = 'abc', kafka.servers = 'localhost:1001')
        ROW FORMAT JSON"#;
        frontend.run_sql(sql).await.unwrap();

        let mut rows = frontend.query_formatted_result("SHOW SOURCES").await;
        rows.sort();
        assert_eq!(rows, vec!["Row([Some(b\"t1\")])".to_string(),]);
    }

    #[tokio::test]
    async fn test_show_column() {
        let proto_file = create_proto_file(PROTO_FILE_DATA);
        let sql = format!(
            r#"CREATE SOURCE t
    WITH (connector = 'kafka', kafka.topic = 'abc', kafka.servers = 'localhost:1001')
    ROW FORMAT PROTOBUF MESSAGE '.test.TestRecord' ROW SCHEMA LOCATION 'file://{}'"#,
            proto_file.path().to_str().unwrap()
        );
        let frontend = LocalFrontend::new(Default::default()).await;
        frontend.run_sql(sql).await.unwrap();

        let sql = "show columns from t";
        let mut pg_response = frontend.run_sql(sql).await.unwrap();

        let mut columns = HashMap::new();
        #[for_await]
        for row_set in pg_response.values_stream() {
            let row_set = row_set.unwrap();
            for row in row_set {
                columns.insert(
                    std::str::from_utf8(row.index(0).as_ref().unwrap())
                        .unwrap()
                        .to_string(),
                    std::str::from_utf8(row.index(1).as_ref().unwrap())
                        .unwrap()
                        .to_string(),
                );
            }
        }

        let expected_columns: HashMap<String, String> = maplit::hashmap! {
            "id".into() => "integer".into(),
            "country.zipcode".into() => "varchar".into(),
            "zipcode".into() => "bigint".into(),
            "country.city.address".into() => "varchar".into(),
            "country.address".into() => "varchar".into(),
            "country.city".into() => "test.City".into(),
            "country.city.zipcode".into() => "varchar".into(),
            "rate".into() => "real".into(),
            "country".into() => "test.Country".into(),
        };

        assert_eq!(columns, expected_columns);
    }
}
