use pgwire::pg_field_descriptor::{PgFieldDescriptor, TypeOid};
use pgwire::pg_response::{PgResponse, StatementType};
use pgwire::types::Row;
use risingwave_common::error::Result;
use risingwave_common::types::DataType;
use risingwave_sqlparser::ast::ObjectName;

use crate::catalog::catalog_service::DEFAULT_SCHEMA_NAME;
use crate::catalog::column_catalog::ColumnCatalog;
use crate::session::SessionImpl;

pub fn column_to_rows(column_catalog: &ColumnCatalog) -> Vec<Row> {
    if let DataType::Struct { fields: _f } = column_catalog.data_type() {
        let mut v = vec![];
        let option = vec![
            Some(column_catalog.name().to_string()),
            Some(get_type_name(column_catalog)),
        ];
        v.push(Row::new(option));
        for col in &column_catalog.sub_catalogs {
            v.append(&mut column_to_rows(col));
        }
        v
    } else {
        let option = vec![
            Some(column_catalog.name().to_string()),
            Some(get_type_name(column_catalog)),
        ];
        vec![Row::new(option)]
    }
}

fn get_type_name(column_catalog: &ColumnCatalog) -> String {
    if let DataType::Struct { fields: _f } = column_catalog.data_type() {
        column_catalog
            .col_desc_ref()
            .type_name
            .as_ref()
            .unwrap()
            .to_string()
    } else {
        format!("{:?}", &column_catalog.data_type())
    }
}

pub async fn handle_show_table(
    session: &SessionImpl,
    table_name: ObjectName,
) -> Result<PgResponse> {
    let str_table_name = table_name.to_string();

    let catalog_mgr = session.env().catalog_mgr();
    let catalog = catalog_mgr
        .get_database_snapshot(session.database())
        .unwrap();
    let table = catalog
        .as_ref()
        .get_schema(DEFAULT_SCHEMA_NAME)
        .unwrap()
        .get_table(str_table_name.as_str())
        .unwrap();

    let mut rows = vec![];
    for col in table.columns() {
        rows.append(&mut column_to_rows(col));
    }

    Ok(PgResponse::new(
        StatementType::SHOW_TABLE,
        0,
        rows,
        vec![
            PgFieldDescriptor::new("column_name".to_owned(), TypeOid::Varchar),
            PgFieldDescriptor::new("data_type".to_owned(), TypeOid::Varchar),
        ],
    ))
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::io::Write;

    use tempfile::NamedTempFile;

    use crate::test_utils::LocalFrontend;

    fn create_proto_file() -> NamedTempFile {
        static PROTO_FILE_DATA: &str = r#"
    syntax = "proto3";
    package test;
    message TestRecord {
      int32 id = 1;
      Result country = 3;
      int64 zipcode = 4;
      float rate = 5;
    }
    message Result {
      string address = 1;
      string city = 2;
      string date = 3;
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
    async fn test_show_table_handler() {
        let proto_file = create_proto_file();
        let sql = format!(
            r#"CREATE SOURCE t
        WITH ('kafka.topic' = 'abc', 'kafka.servers' = 'localhost:1001')
        ROW FORMAT PROTOBUF MESSAGE '.test.TestRecord' ROW SCHEMA LOCATION 'file://{}'"#,
            proto_file.path().to_str().unwrap()
        );
        let frontend = LocalFrontend::new().await;
        frontend.run_sql(sql).await.unwrap();

        let sql_show_table = "show table t;";
        let pg_response = frontend.run_sql(sql_show_table).await.unwrap();

        let columns = pg_response
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
        expected_map.insert("country".to_string(), ".test.Result".to_string());
        expected_map.insert("zipcode".to_string(), "Int64".to_string());
        expected_map.insert("country.date".to_string(), "Varchar".to_string());
        expected_map.insert("id".to_string(), "Int32".to_string());
        expected_map.insert("_row_id".to_string(), "Int64".to_string());
        expected_map.insert("rate".to_string(), "Float32".to_string());
        expected_map.insert("country.city".to_string(), "Varchar".to_string());
        assert_eq!(columns, expected_map)
    }
}
