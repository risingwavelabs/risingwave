use std::iter;

use pgwire::pg_response::{PgResponse, StatementType};
use risingwave_common::catalog::DEFAULT_SCHEMA_NAME;
use risingwave_common::error::Result;
use risingwave_common::types::DataType;
use risingwave_common::util::sort_util::OrderType;
use risingwave_pb::catalog::Table as ProstTable;
use risingwave_pb::plan::{ColumnCatalog, ColumnDesc, RowFormatType, StreamSourceInfo};
use risingwave_source::ProtobufParser;
use risingwave_sqlparser::ast::{CreateSourceStatement, ProtobufSchema, SourceSchema};

use crate::handler::create_table::ROWID_NAME;
use crate::session::QueryContext;

fn create_protobuf_table_schema(
    schema: &ProtobufSchema,
) -> Result<(StreamSourceInfo, Vec<ColumnCatalog>)> {
    let parser = ProtobufParser::new(&schema.row_schema_location.0, &schema.message_name.0)?;
    let column_catalogs = parser.map_to_columns()?;
    let info = StreamSourceInfo {
        append_only: true,
        row_format: RowFormatType::Protobuf as i32,
        row_schema_location: schema.row_schema_location.0.clone(),
        ..Default::default()
    };

    Ok((info, column_catalogs))
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
    let (_source, columns) = match &stmt.source_schema {
        SourceSchema::Protobuf(protobuf_schema) => create_protobuf_table_schema(protobuf_schema)?,
        SourceSchema::Json => todo!(),
    };
    let column_catalogs = iter::once(Ok(ColumnCatalog {
        column_desc: Some(ColumnDesc {
            column_id: 0,
            name: ROWID_NAME.to_string(),
            column_type: Some(DataType::Int32.to_protobuf()),
        }),
        is_hidden: true,
        ..Default::default()
    }))
    .chain(columns.into_iter().map(Ok))
    .collect::<Result<_>>()?;

    let table = ProstTable {
        id: 0,
        schema_id,
        database_id,
        name: source_name,
        columns: column_catalogs,
        pk_column_ids: vec![0],
        pk_orders: vec![OrderType::Ascending.to_prost() as i32],
        dependent_relations: vec![],
        optional_associated_source_id: None,
    };

    let catalog_writer = session.env().catalog_writer();
    // FIX ME
    catalog_writer
        .create_materialized_table_source_workaround(table)
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
