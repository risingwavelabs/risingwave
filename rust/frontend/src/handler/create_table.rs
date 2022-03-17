use pgwire::pg_response::{PgResponse, StatementType};
use risingwave_common::error::Result;
use risingwave_pb::meta::table::Info;
use risingwave_pb::meta::Table;
use risingwave_pb::plan::{ColumnDesc, TableSourceInfo};
use risingwave_sqlparser::ast::{ColumnDef, ObjectName};

use crate::binder::expr::bind_data_type;
use crate::binder::Binder;
use crate::session::QueryContext;

fn columns_to_prost(columns: &[ColumnDef]) -> Result<Vec<ColumnDesc>> {
    columns
        .iter()
        .enumerate()
        .map(|(idx, col)| {
            Ok(ColumnDesc {
                column_id: idx as i32,
                name: col.name.to_string(),
                column_type: Some(bind_data_type(&col.data_type)?.to_protobuf()),
            })
        })
        .collect::<Result<_>>()
}

pub async fn handle_create_table(
    context: QueryContext,
    table_name: ObjectName,
    _columns: Vec<ColumnDef>,
) -> Result<PgResponse> {
    let session = context.session_ctx;
    let (schema_name, table_name) = Binder::resolve_table_name(table_name)?;
    let (_db_id, _schema_id) = session
        .env()
        .catalog_reader()
        .read_guard()
        .check_relation_name(session.database(), &schema_name, &table_name)?;

    let _table_source = Table {
        info: Info::TableSource(TableSourceInfo::default()).into(),
        ..Default::default()
    };
    // TODO: construct a stream plan to materialize table source here

    Ok(PgResponse::new(
        StatementType::CREATE_TABLE,
        0,
        vec![],
        vec![],
    ))
}

// #[cfg(test)]
// mod tests {
//     use std::collections::HashMap;

//     use risingwave_common::types::DataType;

//     use crate::catalog::catalog::{DEFAULT_DATABASE_NAME, DEFAULT_SCHEMA_NAME};
//     use crate::catalog::table_catalog::ROWID_NAME;
//     use crate::test_utils::LocalFrontend;

//     #[tokio::test]
//     async fn test_create_table_handler() {
//         let sql = "create table t (v1 smallint, v2 int, v3 bigint, v4 float, v5 double);";
//         let frontend = LocalFrontend::new().await;
//         frontend.run_sql(sql).await.unwrap();

//         let catalog_manager = frontend.session().env().catalog_mgr();
//         let table = catalog_manager
//             .get_table(DEFAULT_DATABASE_NAME, DEFAULT_SCHEMA_NAME, "t")
//             .unwrap();
//         let columns = table
//             .columns()
//             .iter()
//             .map(|col| (col.name().into(), col.data_type()))
//             .collect::<HashMap<String, DataType>>();
//         let mut expected_map = HashMap::new();
//         expected_map.insert(ROWID_NAME.to_string(), DataType::Int64);
//         expected_map.insert("v1".to_string(), DataType::Int16);
//         expected_map.insert("v2".to_string(), DataType::Int32);
//         expected_map.insert("v3".to_string(), DataType::Int64);
//         expected_map.insert("v4".to_string(), DataType::Float64);
//         expected_map.insert("v5".to_string(), DataType::Float64);
//         assert_eq!(columns, expected_map);
//     }
// }
