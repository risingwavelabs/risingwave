use pgwire::pg_response::{PgResponse, StatementType};
use risingwave_common::error::Result;
use risingwave_pb::meta::Table;
use risingwave_source::ProtobufParser;
use risingwave_sqlparser::ast::{CreateSourceStatement, ProtobufSchema, SourceSchema};

use crate::catalog::catalog_service::DEFAULT_SCHEMA_NAME;
use crate::session::RwSession;

fn create_protobuf_table_schema(schema: &ProtobufSchema) -> Result<Table> {
    let parser = ProtobufParser::new(&schema.row_schema_location.0, &schema.message_name.0)?;
    let column_descs = parser.map_to_columns()?;
    Ok(Table {
        column_descs,
        row_format: schema.row_schema_location.0.clone(),
        row_schema_location: schema.row_schema_location.0.clone(),
        ..Default::default()
    })
}

pub(super) async fn handle_create_source(
    session: &RwSession,
    stmt: CreateSourceStatement,
) -> Result<PgResponse> {
    let mut table = match &stmt.source_schema {
        SourceSchema::Protobuf(protobuf_schema) => create_protobuf_table_schema(protobuf_schema)?,
        SourceSchema::Json => todo!(),
    };
    table.table_name = stmt.source_name.value.clone();
    table.is_source = true;
    table.properties = stmt.with_properties.into();
    table.append_only = true;

    let catalog_mgr = session.env().catalog_mgr();
    catalog_mgr
        .create_table(session.database(), DEFAULT_SCHEMA_NAME, table)
        .await?;

    Ok(PgResponse::new(
        StatementType::CREATE_SOURCE,
        0,
        vec![],
        vec![],
    ))
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::io::Write;

    use risingwave_common::types::DataType;
    use risingwave_meta::test_utils::LocalMeta;
    use tempfile::NamedTempFile;

    use crate::catalog::table_catalog::ROWID_NAME;
    use crate::test_utils::LocalFrontend;

    /// Returns the file.
    /// (`NamedTempFile` will automatically delete the file when it goes out of scope.)
    fn create_proto_file() -> NamedTempFile {
        static PROTO_FILE_DATA: &str = r#"
    syntax = "proto3";
    package test;
    message TestRecord {
      int32 id = 1;
      string city = 3;
      int64 zipcode = 4;
      float rate = 5;
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
    #[serial_test::serial]
    async fn handle_create_source() {
        let meta = LocalMeta::start(12001).await;

        let proto_file = create_proto_file();
        let sql = format!(
            r#"CREATE SOURCE t
    WITH ('kafka.topic' = 'abc', 'kafka.servers' = 'localhost:1001')
    ROW FORMAT PROTOBUF MESSAGE '.test.TestRecord' ROW SCHEMA LOCATION 'file://{}'"#,
            proto_file.path().to_str().unwrap()
        );
        let frontend = LocalFrontend::new(&meta).await;
        frontend.run_sql(sql).await.unwrap();

        let catalog_manager = frontend.session().env().catalog_mgr();
        let table = catalog_manager.get_table("dev", "dev", "t").unwrap();
        let columns = table
            .columns()
            .iter()
            .map(|(col_name, col)| (col_name.clone(), col.data_type()))
            .collect::<HashMap<String, DataType>>();
        let mut expected_map = HashMap::new();
        expected_map.insert(ROWID_NAME.to_string(), DataType::Int64);
        expected_map.insert("id".to_string(), DataType::Int32);
        expected_map.insert("city".to_string(), DataType::Varchar);
        expected_map.insert("zipcode".to_string(), DataType::Int64);
        expected_map.insert("rate".to_string(), DataType::Float32);
        assert_eq!(columns, expected_map);

        meta.stop().await;
    }
}
