use pgwire::pg_response::{PgResponse, StatementType};
use risingwave_common::error::Result;
use risingwave_pb::meta::table::Info;
use risingwave_pb::meta::Table;
use risingwave_pb::plan::{ColumnDesc, TableSourceInfo};
use risingwave_sqlparser::ast::{ColumnDef, ObjectName};

use crate::binder::expr::bind_data_type;
use crate::catalog::catalog_service::DEFAULT_SCHEMA_NAME;
use crate::session::QueryContext;

fn columns_to_prost(columns: &[ColumnDef]) -> Result<Vec<ColumnDesc>> {
    columns
        .iter()
        .enumerate()
        .map(|(idx, col)| {
            Ok(ColumnDesc {
                column_id: idx as i32,
                name: col.name.to_string(),
                column_type: Some(bind_data_type(&col.data_type)?.to_protobuf()?),
                ..Default::default()
            })
        })
        .collect::<Result<_>>()
}

pub async fn handle_create_table(
    context: QueryContext,
    table_name: ObjectName,
    columns: Vec<ColumnDef>,
) -> Result<PgResponse> {
    let session = context.session_ctx;
    let mut table = Table {
        info: Info::TableSource(TableSourceInfo::default()).into(),
        ..Default::default()
    };

    // Only support simple create table.
    table.table_name = table_name.to_string();
    table.column_descs = columns_to_prost(&columns)?;

    let catalog_mgr = session.env().catalog_mgr();
    catalog_mgr
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
    use std::collections::HashMap;

    use risingwave_common::types::DataType;

    use crate::catalog::catalog_service::{DEFAULT_DATABASE_NAME, DEFAULT_SCHEMA_NAME};
    use crate::catalog::table_catalog::ROWID_NAME;
    use crate::test_utils::LocalFrontend;

    #[tokio::test]
    async fn test_create_table_handler() {
        let sql = "create table t (v1 smallint, v2 int, v3 bigint, v4 float, v5 double);";
        let frontend = LocalFrontend::new().await;
        frontend.run_sql(sql).await.unwrap();

        let catalog_manager = frontend.session().ctx.env().catalog_mgr();
        let table = catalog_manager
            .get_table(DEFAULT_DATABASE_NAME, DEFAULT_SCHEMA_NAME, "t")
            .unwrap();
        let columns = table
            .columns()
            .iter()
            .map(|col| (col.name().into(), col.data_type()))
            .collect::<HashMap<String, DataType>>();
        let mut expected_map = HashMap::new();
        expected_map.insert(ROWID_NAME.to_string(), DataType::Int64);
        expected_map.insert("v1".to_string(), DataType::Int16);
        expected_map.insert("v2".to_string(), DataType::Int32);
        expected_map.insert("v3".to_string(), DataType::Int64);
        expected_map.insert("v4".to_string(), DataType::Float64);
        expected_map.insert("v5".to_string(), DataType::Float64);
        assert_eq!(columns, expected_map);
    }
}
