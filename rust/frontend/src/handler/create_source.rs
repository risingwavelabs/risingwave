use pgwire::pg_response::{PgResponse, StatementType};
use risingwave_common::error::Result;
use risingwave_pb::meta::table::Info;
use risingwave_pb::meta::Table;
use risingwave_pb::plan::{RowFormatType, StreamSourceInfo};
use risingwave_source::ProtobufParser;
use risingwave_sqlparser::ast::{CreateSourceStatement, ProtobufSchema, SourceSchema};

use crate::catalog::catalog_service::DEFAULT_SCHEMA_NAME;
use crate::session::RwSession;

fn create_protobuf_table_schema(schema: &ProtobufSchema) -> Result<Table> {
    let parser = ProtobufParser::new(&schema.row_schema_location.0, &schema.message_name.0)?;
    let column_descs = parser.map_to_columns()?;
    let info = StreamSourceInfo {
        append_only: true,
        row_format: RowFormatType::Protobuf as i32,
        row_schema_location: schema.row_schema_location.0.clone(),
        ..Default::default()
    };

    Ok(Table {
        column_descs,
        info: Some(Info::StreamSource(info)),
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
    match table.info.as_mut().unwrap() {
        Info::StreamSource(info) => {
            info.properties = stmt.with_properties.into();
        }
        _ => unreachable!(),
    }
    table.table_name = stmt.source_name.value.clone();

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
    use tempfile::NamedTempFile;

    use crate::catalog::table_catalog::ROWID_NAME;
    use crate::handler::show_table::column_to_rows;
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
    async fn handle_create_source() {
        let proto_file = create_proto_file();
        let sql = format!(
            r#"CREATE SOURCE t
    WITH ('kafka.topic' = 'abc', 'kafka.servers' = 'localhost:1001')
    ROW FORMAT PROTOBUF MESSAGE '.test.TestRecord' ROW SCHEMA LOCATION 'file://{}'"#,
            proto_file.path().to_str().unwrap()
        );
        let frontend = LocalFrontend::new().await;
        frontend.run_sql(sql).await.unwrap();

        let catalog_manager = frontend.session().env().catalog_mgr();
        let table = catalog_manager.get_table("dev", "dev", "t").unwrap();
        let columns = table
            .columns()
            .iter()
            .map(|col| (col.name().into(), col.data_type()))
            .collect::<HashMap<String, DataType>>();
        let mut expected_map = HashMap::new();
        expected_map.insert(ROWID_NAME.to_string(), DataType::Int64);
        expected_map.insert("id".to_string(), DataType::Int32);
        expected_map.insert("city".to_string(), DataType::Varchar);
        expected_map.insert("zipcode".to_string(), DataType::Int64);
        expected_map.insert("rate".to_string(), DataType::Float32);
        assert_eq!(columns, expected_map);
    }

    /// Returns the file.
    /// (`NamedTempFile` will automatically delete the file when it goes out of scope.)
    fn create_complicate_proto_file() -> NamedTempFile {
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
    async fn handle_create_complicate_source() {
        let proto_file = create_complicate_proto_file();
        let sql = format!(
            r#"CREATE SOURCE t
    WITH ('kafka.topic' = 'abc', 'kafka.servers' = 'localhost:1001')
    ROW FORMAT PROTOBUF MESSAGE '.test.TestRecord' ROW SCHEMA LOCATION 'file://{}'"#,
            proto_file.path().to_str().unwrap()
        );
        let frontend = LocalFrontend::new().await;
        frontend.run_sql(sql).await.unwrap();

        let catalog_manager = frontend.session().env().catalog_mgr();
        let table = catalog_manager.get_table("dev", "dev", "t").unwrap();

        let mut rows = vec![];
        for col in table.columns() {
            rows.append(&mut column_to_rows(col.col_desc_ref()));
        }
        let columns = rows
            .iter()
            .map(|row| {
                (
                    row.values().to_vec()[0].as_ref().unwrap().to_string(),
                    row.values().to_vec()[1].as_ref().unwrap().to_string(),
                )
            })
            .collect::<HashMap<String, String>>();
        let mut expected_map = HashMap::new();
        expected_map.insert("country.address".to_string(), "Varchar".to_string());
        expected_map.insert("country".to_string(), ".test.Country".to_string());
        expected_map.insert("zipcode".to_string(), "Int64".to_string());
        expected_map.insert("country.zipcode".to_string(), "Varchar".to_string());
        expected_map.insert("id".to_string(), "Int32".to_string());
        expected_map.insert("_row_id".to_string(), "Int64".to_string());
        expected_map.insert("rate".to_string(), "Float32".to_string());
        expected_map.insert("country.city".to_string(), ".test.City".to_string());
        expected_map.insert("country.city.address".to_string(), "Varchar".to_string());
        expected_map.insert("country.city.zipcode".to_string(), "Varchar".to_string());

        assert_eq!(columns, expected_map);
    }
}
