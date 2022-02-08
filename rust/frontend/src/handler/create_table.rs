use pgwire::pg_response::{PgResponse, StatementType};
use risingwave_common::error::Result;
use risingwave_common::types::DataType;
use risingwave_pb::meta::Table;
use risingwave_pb::plan::ColumnDesc;
use risingwave_sqlparser::ast::{ColumnDef, DataType as AstDataType, ObjectName};

use crate::catalog::catalog_service::DEFAULT_SCHEMA_NAME;
use crate::session::RwSession;

fn columns_to_prost(columns: &[ColumnDef]) -> Result<Vec<ColumnDesc>> {
    columns
        .iter()
        .enumerate()
        .map(|(idx, col)| {
            Ok(ColumnDesc {
                column_id: idx as i32,
                name: col.name.to_string(),
                column_type: Some(convert_data_type(&col.data_type).to_protobuf()?),
                ..Default::default()
            })
        })
        .collect::<Result<_>>()
}

fn convert_data_type(data_type: &AstDataType) -> DataType {
    match data_type {
        AstDataType::SmallInt(_) => DataType::Int16,
        AstDataType::Int(_) => DataType::Int32,
        AstDataType::BigInt(_) => DataType::Int64,
        AstDataType::Float(_) => DataType::Float32,
        AstDataType::Double => DataType::Float64,
        _ => unimplemented!("Unsupported data type {:?} in create table", data_type),
    }
}

pub(super) async fn handle_create_table(
    session: &RwSession,
    table_name: ObjectName,
    columns: Vec<ColumnDef>,
) -> Result<PgResponse> {
    let mut table = Table {
        ..Default::default()
    };

    // Only support simple create table.
    table.table_name = table_name.to_string();
    table.column_descs = columns_to_prost(&columns)?;

    let catalog_mgr = session.env().catalog_mgr();
    catalog_mgr
        .lock()
        .await
        .create_table(session.database(), DEFAULT_SCHEMA_NAME, table)
        .await?;

    Ok(PgResponse::new(
        StatementType::CREATE_TABLE,
        0,
        vec![],
        vec![],
    ))
}

#[cfg(test)]
mod tests {
    use risingwave_common::catalog::{Field, Schema};
    use risingwave_common::types::DataType;
    use risingwave_meta::test_utils::LocalMeta;

    use crate::catalog::catalog_service::{DEFAULT_DATABASE_NAME, DEFAULT_SCHEMA_NAME};
    use crate::test_utils::LocalFrontend;

    #[tokio::test]
    #[serial_test::serial]
    async fn test_create_table_handler() {
        let meta = LocalMeta::start_in_tempdir().await;
        let sql = "create table t (v1 smallint, v2 int, v3 bigint, v4 float, v5 double);";
        let frontend = LocalFrontend::new().await;
        frontend.run_sql(sql).await.unwrap();

        let catalog_manager = frontend.session().env().catalog_mgr();
        let catalog_manager_guard = catalog_manager.lock().await;
        let table = catalog_manager_guard
            .get_table(DEFAULT_DATABASE_NAME, DEFAULT_SCHEMA_NAME, "t")
            .unwrap();
        let columns_schema = table.columns_schema();
        let expected_schema = Schema::new(vec![
            Field::with_name(DataType::Int16, "v1".to_string()),
            Field::with_name(DataType::Int32, "v2".to_string()),
            Field::with_name(DataType::Int64, "v3".to_string()),
            Field::with_name(DataType::Float32, "v4".to_string()),
            Field::with_name(DataType::Float64, "v5".to_string()),
        ]);
        assert_eq!(columns_schema, &expected_schema);

        meta.stop().await;
    }
}
