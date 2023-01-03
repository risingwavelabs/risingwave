// Copyright 2023 Singularity Data
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

use std::collections::HashMap;

use itertools::Itertools;
use pgwire::pg_response::{PgResponse, StatementType};
use risingwave_common::error::ErrorCode::{self, ProtocolError};
use risingwave_common::error::{Result, RwError};
use risingwave_pb::catalog::{
    ColumnIndex as ProstColumnIndex, Source as ProstSource, StreamSourceInfo,
};
use risingwave_pb::plan_common::RowFormatType;
use risingwave_source::{AvroParser, ProtobufParser};
use risingwave_sqlparser::ast::{AvroSchema, CreateSourceStatement, ProtobufSchema, SourceSchema};

use super::create_table::{bind_sql_columns, bind_sql_table_constraints, gen_materialize_plan};
use super::RwPgResponse;
use crate::binder::Binder;
use crate::catalog::column_catalog::ColumnCatalog;
use crate::handler::HandlerArgs;
use crate::optimizer::OptimizerContext;
use crate::stream_fragmenter::build_graph;

/// Map an Avro schema to a relational schema.
async fn extract_avro_table_schema(
    schema: &AvroSchema,
    with_properties: HashMap<String, String>,
) -> Result<Vec<ColumnCatalog>> {
    let parser = AvroParser::new(
        schema.row_schema_location.0.as_str(),
        schema.use_schema_registry,
        with_properties,
    )
    .await?;
    let vec_column_desc = parser.map_to_columns()?;
    Ok(vec_column_desc
        .into_iter()
        .map(|col| ColumnCatalog {
            column_desc: col.into(),
            is_hidden: false,
        })
        .collect_vec())
}

/// Map a protobuf schema to a relational schema.
async fn extract_protobuf_table_schema(
    schema: &ProtobufSchema,
    with_properties: HashMap<String, String>,
) -> Result<Vec<ColumnCatalog>> {
    let parser = ProtobufParser::new(
        &schema.row_schema_location.0,
        &schema.message_name.0,
        schema.use_schema_registry,
        with_properties,
    )
    .await?;
    let column_descs = parser.map_to_columns()?;

    Ok(column_descs
        .into_iter()
        .map(|col| ColumnCatalog {
            column_desc: col.into(),
            is_hidden: false,
        })
        .collect_vec())
}

// TODO(Yuanxin): Only create a source w/o materializing.
pub async fn handle_create_source(
    handler_args: HandlerArgs,
    is_materialized: bool,
    stmt: CreateSourceStatement,
) -> Result<RwPgResponse> {
    let (column_descs, pk_column_id_from_columns) = bind_sql_columns(stmt.columns)?;
    let (mut columns, pk_column_ids, row_id_index) =
        bind_sql_table_constraints(column_descs, pk_column_id_from_columns, stmt.constraints)?;
    if row_id_index.is_none() && !is_materialized {
        return Err(ErrorCode::InvalidInputSyntax(
            "The non-materialized source does not support PRIMARY KEY constraint, please use \"CREATE MATERIALIZED SOURCE\" instead".to_owned(),
        )
        .into());
    }
    let with_properties = handler_args.with_options.inner().clone();
    const UPSTREAM_SOURCE_KEY: &str = "connector";
    // confluent schema registry must be used with kafka
    let is_kafka = with_properties
        .get("connector")
        .unwrap_or(&"".to_string())
        .to_lowercase()
        .eq("kafka");
    if !is_kafka
        && matches!(
            &stmt.source_schema,
            SourceSchema::Protobuf(ProtobufSchema {
                use_schema_registry: true,
                ..
            }) | SourceSchema::Avro(AvroSchema {
                use_schema_registry: true,
                ..
            })
        )
    {
        return Err(RwError::from(ProtocolError(format!(
            "The {} must be kafka when schema registry is used",
            UPSTREAM_SOURCE_KEY
        ))));
    }
    let (columns, source_info) = match &stmt.source_schema {
        SourceSchema::Protobuf(protobuf_schema) => {
            if columns.len() != 1 || pk_column_ids != vec![0.into()] || row_id_index != Some(0) {
                return Err(RwError::from(ProtocolError(
                    "User-defined schema is not allowed with row format protobuf. Please refer to https://www.risingwave.dev/docs/current/sql-create-source/#protobuf for more information.".to_string(),
                )));
            }

            columns.extend(
                extract_protobuf_table_schema(protobuf_schema, with_properties.clone()).await?,
            );

            (
                columns,
                StreamSourceInfo {
                    row_format: RowFormatType::Protobuf as i32,
                    row_schema_location: protobuf_schema.row_schema_location.0.clone(),
                    use_schema_registry: protobuf_schema.use_schema_registry,
                    proto_message_name: protobuf_schema.message_name.0.clone(),
                    ..Default::default()
                },
            )
        }
        SourceSchema::Avro(avro_schema) => {
            if columns.len() != 1 || pk_column_ids != vec![0.into()] || row_id_index != Some(0) {
                return Err(RwError::from(ProtocolError(
                    "User-defined schema is not allowed with row format avro. Please refer to https://www.risingwave.dev/docs/current/sql-create-source/#avro for more information.".to_string(),
                )));
            }
            columns.extend(extract_avro_table_schema(avro_schema, with_properties.clone()).await?);
            (
                columns,
                StreamSourceInfo {
                    row_format: RowFormatType::Avro as i32,
                    row_schema_location: avro_schema.row_schema_location.0.clone(),
                    use_schema_registry: avro_schema.use_schema_registry,
                    proto_message_name: "".to_owned(),
                    ..Default::default()
                },
            )
        }
        SourceSchema::Json => (
            columns,
            StreamSourceInfo {
                row_format: RowFormatType::Json as i32,
                ..Default::default()
            },
        ),
        SourceSchema::Maxwell => {
            // return err if user has not specified a pk
            if row_id_index.is_some() {
                return Err(RwError::from(ProtocolError(
                    "Primary key must be specified when creating source with row format debezium."
                        .to_string(),
                )));
            }
            (
                columns,
                StreamSourceInfo {
                    row_format: RowFormatType::Maxwell as i32,
                    ..Default::default()
                },
            )
        }
        SourceSchema::DebeziumJson => {
            // return err if user has not specified a pk
            if row_id_index.is_some() {
                return Err(RwError::from(ProtocolError(
                    "Primary key must be specified when creating source with row format debezium."
                        .to_string(),
                )));
            }
            (
                columns,
                StreamSourceInfo {
                    row_format: RowFormatType::DebeziumJson as i32,
                    ..Default::default()
                },
            )
        }
        SourceSchema::CanalJson => {
            // return err if user has not specified a pk
            if row_id_index.is_some() {
                return Err(RwError::from(ProtocolError(
                    "Primary key must be specified when creating source with row format cannal_json."
                        .to_string(),
                )));
            }
            (
                columns,
                StreamSourceInfo {
                    row_format: RowFormatType::CanalJson as i32,
                    ..Default::default()
                },
            )
        }
        SourceSchema::CSV(csv_info) => (
            columns,
            StreamSourceInfo {
                row_format: RowFormatType::Csv as i32,
                csv_delimiter: csv_info.delimiter as i32,
                csv_has_header: csv_info.has_header,
                ..Default::default()
            },
        ),
    };

    let row_id_index = row_id_index.map(|index| ProstColumnIndex { index: index as _ });
    let pk_column_ids = pk_column_ids.into_iter().map(Into::into).collect();

    let session = handler_args.session.clone();

    session.check_relation_name_duplicated(stmt.source_name.clone())?;

    let db_name = session.database();
    let (schema_name, name) = Binder::resolve_schema_qualified_name(db_name, stmt.source_name)?;
    let (database_id, schema_id) = session.get_database_and_schema_id_for_create(schema_name)?;

    let columns = columns.into_iter().map(|c| c.to_protobuf()).collect_vec();

    let source = ProstSource {
        id: 0,
        schema_id,
        database_id,
        name,
        row_id_index,
        columns,
        pk_column_ids,
        properties: with_properties,
        info: Some(source_info),
        owner: session.user_id(),
    };
    let catalog_writer = session.env().catalog_writer();

    // TODO(Yuanxin): This should be removed after unsupporting `CREATE MATERIALIZED SOURCE`.
    if is_materialized {
        let (graph, table) = {
            let context = OptimizerContext::new_with_handler_args(handler_args);
            let (plan, table) =
                gen_materialize_plan(context.into(), source.clone(), session.user_id())?;
            let graph = build_graph(plan);

            (graph, table)
        };

        catalog_writer
            .create_table(Some(source), table, graph)
            .await?;
    } else {
        catalog_writer.create_source(source).await?;
    }
    Ok(PgResponse::empty_result(StatementType::CREATE_SOURCE))
}

#[cfg(test)]
pub mod tests {
    use std::collections::HashMap;

    use risingwave_common::catalog::{DEFAULT_DATABASE_NAME, DEFAULT_SCHEMA_NAME};
    use risingwave_common::types::DataType;

    use crate::catalog::root_catalog::SchemaPath;
    use crate::catalog::row_id_column_name;
    use crate::test_utils::{create_proto_file, LocalFrontend, PROTO_FILE_DATA};

    #[tokio::test]
    async fn test_create_source_handler() {
        let proto_file = create_proto_file(PROTO_FILE_DATA);
        let sql = format!(
            r#"CREATE SOURCE t
    WITH (kafka.topic = 'abc', kafka.servers = 'localhost:1001')
    ROW FORMAT PROTOBUF MESSAGE '.test.TestRecord' ROW SCHEMA LOCATION 'file://{}'"#,
            proto_file.path().to_str().unwrap()
        );
        let frontend = LocalFrontend::new(Default::default()).await;
        frontend.run_sql(sql).await.unwrap();

        let session = frontend.session_ref();
        let catalog_reader = session.env().catalog_reader().read_guard();
        let schema_path = SchemaPath::Name(DEFAULT_SCHEMA_NAME);

        // Check source exists.
        let (source, _) = catalog_reader
            .get_source_by_name(DEFAULT_DATABASE_NAME, schema_path, "t")
            .unwrap();
        assert_eq!(source.name, "t");

        let columns = source
            .columns
            .iter()
            .map(|col| (col.name(), col.data_type().clone()))
            .collect::<HashMap<&str, DataType>>();

        let city_type = DataType::new_struct(
            vec![DataType::Varchar, DataType::Varchar],
            vec!["address".to_string(), "zipcode".to_string()],
        );
        let row_id_col_name = row_id_column_name();
        let expected_columns = maplit::hashmap! {
            row_id_col_name.as_str() => DataType::Int64,
            "id" => DataType::Int32,
            "zipcode" => DataType::Int64,
            "rate" => DataType::Float32,
            "country" => DataType::new_struct(
                vec![DataType::Varchar,city_type,DataType::Varchar],
                vec!["address".to_string(), "city".to_string(), "zipcode".to_string()],
            ),
        };
        assert_eq!(columns, expected_columns);
    }
}
