// Copyright 2022 Singularity Data
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::collections::HashMap;

use itertools::Itertools;
use pgwire::pg_response::{PgResponse, StatementType};
use risingwave_common::catalog::DEFAULT_SCHEMA_NAME;
use risingwave_common::error::ErrorCode::{self, ProtocolError};
use risingwave_common::error::{Result, RwError};
use risingwave_pb::catalog::source::Info;
use risingwave_pb::catalog::{
    ColumnIndex as ProstColumnIndex, Source as ProstSource, StreamSourceInfo,
};
use risingwave_pb::plan_common::{ColumnCatalog as ProstColumnCatalog, RowFormatType};
use risingwave_pb::user::grant_privilege::{Action, Object};
use risingwave_source::{AvroParser, ProtobufParser, SourceFormat};
use risingwave_sqlparser::ast::{
    AvroSchema, CreateSourceStatement, ObjectName, ProtobufSchema, SourceSchema,
};

use super::create_table::{
    bind_sql_columns, bind_sql_table_constraints, gen_materialized_source_plan,
};
use super::privilege::check_privileges;
use crate::binder::Binder;
use crate::catalog::check_schema_writable;
use crate::handler::privilege::ObjectCheckItem;
use crate::session::{OptimizerContext, SessionImpl};
use crate::stream_fragmenter::build_graph;

pub(crate) fn make_prost_source(
    session: &SessionImpl,
    name: ObjectName,
    source_info: Info,
) -> Result<ProstSource> {
    let (schema_name, name) = Binder::resolve_table_name(name)?;
    check_schema_writable(&schema_name)?;

    let (database_id, schema_id) = {
        let catalog_reader = session.env().catalog_reader().read_guard();

        if schema_name != DEFAULT_SCHEMA_NAME {
            let schema = catalog_reader.get_schema_by_name(session.database(), &schema_name)?;
            check_privileges(
                session,
                &vec![ObjectCheckItem::new(
                    schema.owner(),
                    Action::Create,
                    Object::SchemaId(schema.id()),
                )],
            )?;
        }

        let db_id = catalog_reader
            .get_database_by_name(session.database())?
            .id();
        let schema_id = catalog_reader
            .get_schema_by_name(session.database(), &schema_name)?
            .id();
        (db_id, schema_id)
    };

    Ok(ProstSource {
        id: 0,
        schema_id,
        database_id,
        name,
        info: Some(source_info),
        owner: session.user_id(),
    })
}

/// Map an Avro schema to a relational schema.
async fn extract_avro_table_schema(
    schema: &AvroSchema,
    with_properties: HashMap<String, String>,
) -> Result<Vec<ProstColumnCatalog>> {
    let parser = AvroParser::new(schema.row_schema_location.0.as_str(), with_properties).await?;
    let vec_column_desc = parser.map_to_columns()?;
    Ok(vec_column_desc
        .into_iter()
        .map(|c| ProstColumnCatalog {
            column_desc: Some(c),
            is_hidden: false,
        })
        .collect_vec())
}

/// Map a protobuf schema to a relational schema.
fn extract_protobuf_table_schema(schema: &ProtobufSchema) -> Result<Vec<ProstColumnCatalog>> {
    let parser = ProtobufParser::new(&schema.row_schema_location.0, &schema.message_name.0)?;
    let column_descs = parser.map_to_columns()?;

    Ok(column_descs
        .into_iter()
        .map(|col| ProstColumnCatalog {
            column_desc: Some(col),
            is_hidden: false,
        })
        .collect_vec())
}

fn source_schema_to_source_format(schema: &SourceSchema) -> SourceFormat {
    match schema {
        SourceSchema::Protobuf(_) => SourceFormat::Protobuf,
        SourceSchema::Json => SourceFormat::Json,
        SourceSchema::DebeziumJson => SourceFormat::DebeziumJson,
        SourceSchema::Avro(_) => SourceFormat::Avro,
    }
}

pub async fn handle_create_source(
    context: OptimizerContext,
    is_materialized: bool,
    stmt: CreateSourceStatement,
) -> Result<PgResponse> {
    let (column_descs, pk_column_id_from_columns) = bind_sql_columns(stmt.columns)?;
    let format = source_schema_to_source_format(&stmt.source_schema);
    let col = column_descs
        .iter()
        .find(|c| !format.supported_type(&c.data_type));
    if let Some(c) = col {
        return Err(ErrorCode::InvalidParameterValue(format!(
            "type \"{}\" is not supported by row format \"{:?}\"",
            c.data_type, format
        ))
        .into());
    }
    let (mut columns, pk_column_ids, row_id_index) =
        bind_sql_table_constraints(column_descs, pk_column_id_from_columns, stmt.constraints)?;

    let with_properties = context.with_options.inner().clone();

    let source = match &stmt.source_schema {
        SourceSchema::Protobuf(protobuf_schema) => {
            assert_eq!(columns.len(), 1);
            assert_eq!(pk_column_ids, vec![0.into()]);
            assert_eq!(row_id_index, Some(0));
            columns.extend(extract_protobuf_table_schema(protobuf_schema)?);
            StreamSourceInfo {
                properties: with_properties.clone(),
                row_format: RowFormatType::Protobuf as i32,
                row_schema_location: protobuf_schema.row_schema_location.0.clone(),
                row_id_index: row_id_index.map(|index| ProstColumnIndex { index: index as _ }),
                columns,
                pk_column_ids: pk_column_ids.into_iter().map(Into::into).collect(),
            }
        }
        SourceSchema::Avro(avro_schema) => {
            assert_eq!(columns.len(), 1);
            assert_eq!(pk_column_ids, vec![0.into()]);
            assert_eq!(row_id_index, Some(0));
            columns.extend(extract_avro_table_schema(avro_schema, with_properties.clone()).await?);
            StreamSourceInfo {
                properties: with_properties.clone(),
                row_format: RowFormatType::Avro as i32,
                row_schema_location: avro_schema.row_schema_location.0.clone(),
                row_id_index: row_id_index.map(|index| ProstColumnIndex { index: index as _ }),
                columns,
                pk_column_ids: pk_column_ids.into_iter().map(Into::into).collect(),
            }
        }
        SourceSchema::Json => StreamSourceInfo {
            properties: with_properties.clone(),
            row_format: RowFormatType::Json as i32,
            row_schema_location: "".to_string(),
            row_id_index: row_id_index.map(|index| ProstColumnIndex { index: index as _ }),
            columns,
            pk_column_ids: pk_column_ids.into_iter().map(Into::into).collect(),
        },
        SourceSchema::DebeziumJson => {
            // return err if user has not specified a pk
            if row_id_index.is_some() {
                return Err(RwError::from(ProtocolError(
                    "Primary key must be specified when creating source with row format debezium."
                        .to_string(),
                )));
            }
            StreamSourceInfo {
                properties: with_properties.clone(),
                row_format: RowFormatType::DebeziumJson as i32,
                row_schema_location: "".to_string(),
                row_id_index: row_id_index.map(|index| ProstColumnIndex { index: index as _ }),
                columns,
                pk_column_ids: pk_column_ids.into_iter().map(Into::into).collect(),
            }
        }
    };

    let session = context.session_ctx.clone();
    {
        let catalog_reader = session.env().catalog_reader().read_guard();
        let (schema_name, name) = Binder::resolve_table_name(stmt.source_name.clone())?;
        catalog_reader.check_relation_name_duplicated(session.database(), &schema_name, &name)?;
    }
    let source = make_prost_source(&session, stmt.source_name, Info::StreamSource(source))?;
    let catalog_writer = session.env().catalog_writer();
    if is_materialized {
        let (graph, table) = {
            let (plan, table) =
                gen_materialized_source_plan(context.into(), source.clone(), session.user_id())?;
            let graph = build_graph(plan);

            (graph, table)
        };

        catalog_writer
            .create_materialized_source(source, table, graph)
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
        let catalog_reader = session.env().catalog_reader();

        // Check source exists.
        let source = catalog_reader
            .read_guard()
            .get_source_by_name(DEFAULT_DATABASE_NAME, DEFAULT_SCHEMA_NAME, "t")
            .unwrap()
            .clone();
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
