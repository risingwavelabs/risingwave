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
use risingwave_common::error::ErrorCode::ProtocolError;
use risingwave_common::error::{Result, RwError};
use risingwave_pb::catalog::source::Info;
use risingwave_pb::catalog::{Source as ProstSource, StreamSourceInfo};
use risingwave_pb::plan::{ColumnCatalog as ProstColumnCatalog, RowFormatType};
use risingwave_source::ProtobufParser;
use risingwave_sqlparser::ast::{
    CreateSourceStatement, ObjectName, ProtobufSchema, SourceSchema, SqlOption, Value,
};

use super::create_table::{bind_sql_columns, gen_materialized_source_plan};
use crate::binder::Binder;
use crate::catalog::column_catalog::ColumnCatalog;
use crate::session::{OptimizerContext, SessionImpl};

pub(crate) fn make_prost_source(
    session: &SessionImpl,
    name: ObjectName,
    source_info: Info,
) -> Result<ProstSource> {
    let (schema_name, name) = Binder::resolve_table_name(name)?;

    let (database_id, schema_id) = session
        .env()
        .catalog_reader()
        .read_guard()
        .check_relation_name_duplicated(session.database(), &schema_name, &name)?;

    Ok(ProstSource {
        id: 0,
        schema_id,
        database_id,
        name,
        info: Some(source_info),
    })
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

fn handle_source_with_properties(options: Vec<SqlOption>) -> Result<HashMap<String, String>> {
    options
        .into_iter()
        .map(|x| match x.value {
            Value::SingleQuotedString(s) => Ok((x.name.value, s)),
            _ => Err(RwError::from(ProtocolError(
                "with properties only support single quoted string value".to_string(),
            ))),
        })
        .collect()
}

pub(super) async fn handle_create_source(
    context: OptimizerContext,
    is_materialized: bool,
    stmt: CreateSourceStatement,
) -> Result<PgResponse> {
    let source = match &stmt.source_schema {
        SourceSchema::Protobuf(protobuf_schema) => {
            let mut columns = vec![ColumnCatalog::row_id_column().to_protobuf()];
            columns.extend(extract_protobuf_table_schema(protobuf_schema)?.into_iter());
            StreamSourceInfo {
                properties: handle_source_with_properties(stmt.with_properties.0)?,
                row_format: RowFormatType::Protobuf as i32,
                row_schema_location: protobuf_schema.row_schema_location.0.clone(),
                row_id_index: 0,
                columns,
                pk_column_ids: vec![0],
            }
        }
        SourceSchema::Json => StreamSourceInfo {
            properties: handle_source_with_properties(stmt.with_properties.0)?,
            row_format: RowFormatType::Json as i32,
            row_schema_location: "".to_string(),
            row_id_index: 0,
            columns: bind_sql_columns(stmt.columns)?,
            pk_column_ids: vec![0],
        },
    };

    let session = context.session_ctx.clone();
    let source = make_prost_source(&session, stmt.source_name, Info::StreamSource(source))?;
    let catalog_writer = session.env().catalog_writer();
    if is_materialized {
        let (plan, table) = {
            let (plan, table) = gen_materialized_source_plan(context.into(), source.clone())?;
            let plan = plan.to_stream_prost();
            (plan, table)
        };
        catalog_writer
            .create_materialized_source(source, table, plan)
            .await?;
    } else {
        catalog_writer.create_source(source).await?;
    }
    Ok(PgResponse::empty_result(StatementType::CREATE_SOURCE))
}

#[cfg(test)]
pub mod tests {
    use std::collections::HashMap;
    use std::io::Write;

    use risingwave_common::catalog::{DEFAULT_DATABASE_NAME, DEFAULT_SCHEMA_NAME};
    use risingwave_common::types::DataType;
    use tempfile::NamedTempFile;

    use crate::catalog::gen_row_id_column_name;
    use crate::test_utils::LocalFrontend;

    /// Returns the file.
    /// (`NamedTempFile` will automatically delete the file when it goes out of scope.)
    pub fn create_proto_file() -> NamedTempFile {
        static PROTO_FILE_DATA: &str = r#"
    syntax = "proto3";
    package test;
    message TestRecord {
      int32 id = 1;
      Country country = 3;
      int64 zipcode = 4;
      float rate = 5;
    }
    message Country {
      string address = 1;
      City city = 2;
      string zipcode = 3;
    }
    message City {
      string address = 1;
      string zipcode = 2;
    }"#;
        let temp_file = tempfile::Builder::new()
            .prefix("temp")
            .suffix(".proto")
            .rand_bytes(5)
            .tempfile()
            .unwrap();
        let mut file = temp_file.as_file();
        file.write_all(PROTO_FILE_DATA.as_ref()).unwrap();
        temp_file
    }

    #[tokio::test]
    async fn test_create_source_handler() {
        let proto_file = create_proto_file();
        let sql = format!(
            r#"CREATE SOURCE t
    WITH ('kafka.topic' = 'abc', 'kafka.servers' = 'localhost:1001')
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

        // Only check stream source
        let catalogs = source.columns;
        let mut columns = vec![];

        // Get all column descs
        for catalog in catalogs {
            columns.append(&mut catalog.column_desc.get_column_descs());
        }
        let columns = columns
            .iter()
            .map(|col| (col.name.as_str(), col.data_type.clone()))
            .collect::<HashMap<&str, DataType>>();

        let city_type = DataType::Struct {
            fields: vec![DataType::Varchar, DataType::Varchar].into(),
        };
        let row_id_col_name = gen_row_id_column_name(0);
        let expected_columns = maplit::hashmap! {
            row_id_col_name.as_str() => DataType::Int64,
            "id" => DataType::Int32,
            "country.zipcode" => DataType::Varchar,
            "zipcode" => DataType::Int64,
            "country.city.address" => DataType::Varchar,
            "country.address" => DataType::Varchar,
            "country.city" => city_type.clone(),
            "country.city.zipcode" => DataType::Varchar,
            "rate" => DataType::Float32,
            "country" => DataType::Struct {fields:vec![DataType::Varchar,city_type,DataType::Varchar].into()},
        };
        assert_eq!(columns, expected_columns);
    }
}
