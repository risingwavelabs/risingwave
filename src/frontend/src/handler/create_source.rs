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
use risingwave_common::catalog::ColumnDesc;
use risingwave_common::error::ErrorCode::{self, ProtocolError};
use risingwave_common::error::{Result, RwError};
use risingwave_common::types::DataType;
use risingwave_connector::source::KAFKA_CONNECTOR;
use risingwave_pb::catalog::{
    ColumnIndex as ProstColumnIndex, Source as ProstSource, StreamSourceInfo,
};
use risingwave_pb::plan_common::RowFormatType;
use risingwave_source::{AvroParser, ProtobufParser};
use risingwave_sqlparser::ast::{AvroSchema, CreateSourceStatement, ProtobufSchema, SourceSchema};

use super::create_table::bind_sql_table_constraints;
use super::RwPgResponse;
use crate::binder::Binder;
use crate::catalog::column_catalog::ColumnCatalog;
use crate::catalog::ColumnId;
use crate::handler::create_table::{bind_sql_columns, ColumnIdGenerator};
use crate::handler::HandlerArgs;
use crate::optimizer::plan_node::KAFKA_TIMESTAMP_COLUMN_NAME;

pub(crate) const UPSTREAM_SOURCE_KEY: &str = "connector";

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

pub(crate) fn is_kafka_source(with_properties: &HashMap<String, String>) -> bool {
    with_properties
        .get(UPSTREAM_SOURCE_KEY)
        .unwrap_or(&"".to_string())
        .to_lowercase()
        .eq(KAFKA_CONNECTOR)
}

pub(crate) async fn resolve_source_schema(
    source_schema: SourceSchema,
    columns: &mut Vec<ColumnCatalog>,
    with_properties: &HashMap<String, String>,
    row_id_index: Option<usize>,
    pk_column_ids: &[ColumnId],
) -> Result<StreamSourceInfo> {
    if !is_kafka_source(with_properties)
        && matches!(
            &source_schema,
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

    let source_info = match &source_schema {
        SourceSchema::Protobuf(protobuf_schema) => {
            if columns.len() != 1 || pk_column_ids != vec![0.into()] || row_id_index != Some(0) {
                return Err(RwError::from(ProtocolError(
                    "User-defined schema is not allowed with row format protobuf. Please refer to https://www.risingwave.dev/docs/current/sql-create-source/#protobuf for more information.".to_string(),
                )));
            }

            columns.extend(
                extract_protobuf_table_schema(protobuf_schema, with_properties.clone()).await?,
            );

            StreamSourceInfo {
                row_format: RowFormatType::Protobuf as i32,
                row_schema_location: protobuf_schema.row_schema_location.0.clone(),
                use_schema_registry: protobuf_schema.use_schema_registry,
                proto_message_name: protobuf_schema.message_name.0.clone(),
                ..Default::default()
            }
        }

        SourceSchema::Avro(avro_schema) => {
            if columns.len() != 1 || pk_column_ids != vec![0.into()] || row_id_index != Some(0) {
                return Err(RwError::from(ProtocolError(
                    "User-defined schema is not allowed with row format avro. Please refer to https://www.risingwave.dev/docs/current/sql-create-source/#avro for more information.".to_string(),
                )));
            }

            columns.extend(extract_avro_table_schema(avro_schema, with_properties.clone()).await?);

            StreamSourceInfo {
                row_format: RowFormatType::Avro as i32,
                row_schema_location: avro_schema.row_schema_location.0.clone(),
                use_schema_registry: avro_schema.use_schema_registry,
                proto_message_name: "".to_owned(),
                ..Default::default()
            }
        }

        SourceSchema::Json => StreamSourceInfo {
            row_format: RowFormatType::Json as i32,
            ..Default::default()
        },

        SourceSchema::Maxwell => {
            // return err if user has not specified a pk
            if row_id_index.is_some() {
                return Err(RwError::from(ProtocolError(
                    "Primary key must be specified when creating source with row format debezium."
                        .to_string(),
                )));
            }

            StreamSourceInfo {
                row_format: RowFormatType::Maxwell as i32,
                ..Default::default()
            }
        }

        SourceSchema::DebeziumJson => {
            // return err if user has not specified a pk
            if row_id_index.is_some() {
                return Err(RwError::from(ProtocolError(
                    "Primary key must be specified when creating source with row format debezium."
                        .to_string(),
                )));
            }

            StreamSourceInfo {
                row_format: RowFormatType::DebeziumJson as i32,
                ..Default::default()
            }
        }

        SourceSchema::CanalJson => {
            // return err if user has not specified a pk
            if row_id_index.is_some() {
                return Err(RwError::from(ProtocolError(
                    "Primary key must be specified when creating source with row format cannal_json."
                        .to_string(),
                )));
            }

            StreamSourceInfo {
                row_format: RowFormatType::CanalJson as i32,
                ..Default::default()
            }
        }

        SourceSchema::Csv(csv_info) => StreamSourceInfo {
            row_format: RowFormatType::Csv as i32,
            csv_delimiter: csv_info.delimiter as i32,
            csv_has_header: csv_info.has_header,
            ..Default::default()
        },
    };

    Ok(source_info)
}

// Add a hidden column `_rw_kafka_timestamp` to each message from Kafka source.
pub(crate) fn check_and_add_timestamp_column(
    with_properties: &HashMap<String, String>,
    column_descs: &mut Vec<ColumnDesc>,
    col_id_gen: &mut ColumnIdGenerator,
) {
    if is_kafka_source(with_properties) {
        let kafka_timestamp_column = ColumnDesc {
            data_type: DataType::Timestamptz,
            column_id: col_id_gen.generate(KAFKA_TIMESTAMP_COLUMN_NAME),
            name: KAFKA_TIMESTAMP_COLUMN_NAME.to_string(),
            field_descs: vec![],
            type_name: "".to_string(),
        };
        column_descs.push(kafka_timestamp_column);
    }
}

pub async fn handle_create_source(
    handler_args: HandlerArgs,
    stmt: CreateSourceStatement,
) -> Result<RwPgResponse> {
    let with_properties = handler_args.with_options.inner().clone();

    let mut col_id_gen = ColumnIdGenerator::new_initial();

    let (mut column_descs, pk_column_id_from_columns) =
        bind_sql_columns(stmt.columns, &mut col_id_gen)?;

    check_and_add_timestamp_column(&with_properties, &mut column_descs, &mut col_id_gen);

    let (mut columns, pk_column_ids, row_id_index) =
        bind_sql_table_constraints(column_descs, pk_column_id_from_columns, stmt.constraints)?;
    if row_id_index.is_none() {
        return Err(ErrorCode::InvalidInputSyntax(
            "Source does not support PRIMARY KEY constraint, please use \"CREATE TABLE\" instead"
                .to_owned(),
        )
        .into());
    }

    let source_info = resolve_source_schema(
        stmt.source_schema,
        &mut columns,
        &with_properties,
        row_id_index,
        &pk_column_ids,
    )
    .await?;

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
    catalog_writer.create_source(source).await?;

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
