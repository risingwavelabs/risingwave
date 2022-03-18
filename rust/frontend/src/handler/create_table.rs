use std::iter;

use pgwire::pg_response::{PgResponse, StatementType};
use risingwave_common::error::Result;
use risingwave_common::types::DataType;
use risingwave_common::util::sort_util::OrderType;
use risingwave_pb::catalog::{Source as ProstSource, Table as ProstTable};
use risingwave_pb::plan::{ColumnCatalog, ColumnDesc, TableSourceInfo};
use risingwave_sqlparser::ast::{ColumnDef, ObjectName};

use crate::binder::expr::bind_data_type;
use crate::binder::Binder;
use crate::session::QueryContext;

fn construct_table_prost(columns: &[ColumnDef]) -> Result<Vec<ColumnDesc>> {
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
    columns: Vec<ColumnDef>,
) -> Result<PgResponse> {
    let session = context.session_ctx;
    let (schema_name, table_name) = Binder::resolve_table_name(table_name)?;
    let (database_id, schema_id) = session
        .env()
        .catalog_reader()
        .read_guard()
        .check_relation_name(session.database(), &schema_name, &table_name)?;
    let column_catalogs = iter::once(Ok(ColumnCatalog {
        column_desc: Some(ColumnDesc {
            column_id: 0,
            name: "_row_id".to_string(),
            column_type: Some(DataType::Int32.to_protobuf()),
        }),
        is_hidden: true,
    }))
    .chain(columns.into_iter().enumerate().map(|(idx, col)| {
        Ok(ColumnCatalog {
            column_desc: Some(ColumnDesc {
                column_id: (idx + 1) as i32,
                name: col.name.to_string(),
                column_type: Some(bind_data_type(&col.data_type)?.to_protobuf()),
            }),
            is_hidden: false,
        })
    }))
    .collect::<Result<_>>()?;

    let table = ProstTable {
        id: 0,
        schema_id,
        database_id,
        name: table_name,
        columns: column_catalogs,
        pk_column_ids: vec![0],
        pk_orders: vec![OrderType::Ascending.to_prost() as i32],
        dependent_relations: vec![],
    };

    let catalog_writer = session.env().catalog_writer();
    // FIX ME
    catalog_writer
        .create_materialized_table_source_workaround(table)
        .await?;

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
