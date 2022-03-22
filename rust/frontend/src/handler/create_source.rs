use std::collections::HashMap;

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
use pgwire::pg_response::{PgResponse, StatementType};
use risingwave_common::catalog::DEFAULT_SCHEMA_NAME;
use risingwave_common::error::Result;
use risingwave_pb::catalog::source::Info;
use risingwave_pb::catalog::{Source as ProstSource, StreamSourceInfo};
use risingwave_pb::plan::{ColumnDesc as ProstColumnDesc, RowFormatType};
use risingwave_source::ProtobufParser;
use risingwave_sqlparser::ast::{CreateSourceStatement, ProtobufSchema, SourceSchema};

use crate::session::QueryContext;

fn extract_protobuf_table_schema(
    schema: &ProtobufSchema,
) -> Result<(StreamSourceInfo, Vec<ProstColumnDesc>)> {
    let parser = ProtobufParser::new(&schema.row_schema_location.0, &schema.message_name.0)?;
    let column_descs = parser.map_to_columns()?;
    let info = StreamSourceInfo {
        // append_only: true,
        row_format: RowFormatType::Protobuf as i32,
        row_schema_location: schema.row_schema_location.0.clone(),
        ..Default::default()
    };

    Ok((info, column_descs))
}

pub(super) async fn handle_create_source(
    context: QueryContext,
    stmt: CreateSourceStatement,
) -> Result<PgResponse> {
    let session = context.session_ctx;
    // fix: there is no schema name?
    let schema_name = DEFAULT_SCHEMA_NAME;
    let source_name = stmt.source_name.value.clone();
    let (database_id, schema_id) = session
        .env()
        .catalog_reader()
        .read_guard()
        .check_relation_name(session.database(), schema_name, &source_name)?;
    let (source, _columns) = match &stmt.source_schema {
        SourceSchema::Protobuf(protobuf_schema) => extract_protobuf_table_schema(protobuf_schema)?,
        SourceSchema::Json => (
            StreamSourceInfo {
                properties: HashMap::from(stmt.with_properties),
                row_format: RowFormatType::Json as i32,
                ..Default::default()
            },
            vec![],
        ),
    };
    let catalog_writer = session.env().catalog_writer();

    catalog_writer
        .create_source(ProstSource {
            id: 0,
            schema_id,
            database_id,
            name: source_name.clone(),
            info: Some(Info::StreamSource(source)),
        })
        .await?;

    Ok(PgResponse::new(
        StatementType::CREATE_SOURCE,
        0,
        vec![],
        vec![],
    ))
}

// TODO: with a good MockMeta and then we can open the tests.
// #[cfg(test)]
// mod tests {
//     use std::collections::HashMap;
//     use std::io::Write;

//     use risingwave_common::types::DataType;
//     use tempfile::NamedTempFile;

//     use crate::catalog::table_catalog::ROWID_NAME;
//     use crate::test_utils::LocalFrontend;

//     /// Returns the file.
//     /// (`NamedTempFile` will automatically delete the file when it goes out of scope.)
//     fn create_proto_file() -> NamedTempFile {
//         static PROTO_FILE_DATA: &str = r#"
//     syntax = "proto3";
//     package test;
//     message TestRecord {
//         int32 id = 1;
//         string city = 3;
//         int64 zipcode = 4;
//         float rate = 5;
//     }"#;
//         let temp_file = tempfile::Builder::new()
//             .prefix("temp")
//             .suffix(".proto")
//             .rand_bytes(5)
//             .tempfile()
//             .unwrap();
//         let mut file = temp_file.as_file();
//         file.write_all(PROTO_FILE_DATA.as_ref()).unwrap();
//         temp_file
//     }

//     #[tokio::test]
//     async fn handle_create_source() {
//         let proto_file = create_proto_file();
//         let sql = format!(
//             r#"CREATE SOURCE t
//     WITH ('kafka.topic' = 'abc', 'kafka.servers' = 'localhost:1001')
//     ROW FORMAT PROTOBUF MESSAGE '.test.TestRecord' ROW SCHEMA LOCATION 'file://{}'"#,
//             proto_file.path().to_str().unwrap()
//         );
//         let frontend = LocalFrontend::new().await;
//         frontend.run_sql(sql).await.unwrap();

//         let catalog_manager = frontend.session().env().catalog_mgr();
//         let table = catalog_manager.get_table("dev", "dev", "t").unwrap();
//         let columns = table
//             .columns()
//             .iter()
//             .map(|col| (col.name().into(), col.data_type()))
//             .collect::<HashMap<String, DataType>>();
//         let mut expected_map = HashMap::new();
//         expected_map.insert(ROWID_NAME.to_string(), DataType::Int64);
//         expected_map.insert("id".to_string(), DataType::Int32);
//         expected_map.insert("city".to_string(), DataType::Varchar);
//         expected_map.insert("zipcode".to_string(), DataType::Int64);
//         expected_map.insert("rate".to_string(), DataType::Float32);
//         assert_eq!(columns, expected_map);
//     }
// }
