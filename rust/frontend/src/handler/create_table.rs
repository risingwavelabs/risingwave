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

use std::cell::RefCell;
use std::rc::Rc;

use itertools::Itertools;
use pgwire::pg_response::{PgResponse, StatementType};
use risingwave_common::catalog::{ColumnDesc, ColumnId, Field, Schema, TableId};
use risingwave_common::error::Result;
use risingwave_common::types::DataType;
use risingwave_pb::catalog::source::Info;
use risingwave_pb::catalog::{Source as ProstSource, Table as ProstTable, TableSourceInfo};
use risingwave_pb::plan::{ColumnCatalog, ColumnDesc as ProstColumnDesc, OrderType};
use risingwave_pb::stream_plan::source_node::SourceType;
use risingwave_sqlparser::ast::{ColumnDef, ObjectName};

use crate::binder::expr::bind_data_type;
use crate::binder::Binder;
use crate::optimizer::plan_node::{LogicalScan, StreamExchange, StreamMaterialize, StreamSource};
use crate::optimizer::property::{Direction, Distribution, FieldOrder};
use crate::optimizer::PlanRef;
use crate::session::QueryContext;

pub const ROWID_NAME: &str = "_row_id";

pub async fn handle_create_table(
    context: QueryContext,
    table_name: ObjectName,
    columns: Vec<ColumnDef>,
) -> Result<PgResponse> {
    let session = context.session_ctx.clone();

    let (schema_name, table_name) = Binder::resolve_table_name(table_name)?;
    let (database_id, schema_id) = session
        .env()
        .catalog_reader()
        .read_guard()
        .check_relation_name(session.database(), &schema_name, &table_name)?;

    let column_descs = {
        let mut column_descs = Vec::with_capacity(columns.len() + 1);
        column_descs.push(ColumnDesc {
            data_type: DataType::Int64,
            column_id: ColumnId::new(0),
            name: ROWID_NAME.to_string(),
        });
        for (i, column) in columns.into_iter().enumerate() {
            column_descs.push(ColumnDesc {
                data_type: bind_data_type(&column.data_type)?,
                column_id: ColumnId::new((i + 1) as i32),
                name: column.name.value,
            });
        }
        column_descs
    };

    let plan = {
        let context = Rc::new(RefCell::new(context));

        let source_node = {
            let (columns, fields) = column_descs
                .iter()
                .map(|c| {
                    (
                        c.column_id,
                        Field::with_name(c.data_type.clone(), c.name.clone()),
                    )
                })
                .unzip();
            let schema = Schema::new(fields);
            let logical_scan = LogicalScan::new(
                table_name.clone(),
                TableId::placeholder(),
                columns,
                schema,
                context.clone(),
            );
            StreamSource::new(logical_scan, SourceType::Table)
        };

        let exchange_node =
            { StreamExchange::new(source_node.into(), Distribution::HashShard(vec![0])) };

        let materialize_node = {
            StreamMaterialize::new(
                context,
                exchange_node.into(),
                TableId::placeholder(),
                vec![
                    // RowId column as key
                    FieldOrder {
                        index: 0,
                        direct: Direction::Asc,
                    },
                ],
                column_descs.iter().map(|x| x.column_id).collect(),
            )
        };

        (Rc::new(materialize_node) as PlanRef).to_stream_prost()
    };

    let json_plan = serde_json::to_string_pretty(&plan).unwrap();
    log::debug!("name={}, plan=\n{}", table_name, json_plan);

    let columns = column_descs
        .into_iter()
        .enumerate()
        .map(|(i, c)| ColumnCatalog {
            column_desc: ProstColumnDesc {
                column_type: c.data_type.to_protobuf().into(),
                column_id: c.column_id.get_id(),
                name: c.name,
            }
            .into(),
            is_hidden: i == 0,
        })
        .collect_vec();

    let source = ProstSource {
        id: TableId::placeholder().table_id(),
        schema_id,
        database_id,
        name: table_name.clone(),
        info: Info::TableSource(TableSourceInfo {
            columns: columns.clone(),
        })
        .into(),
    };

    let table = ProstTable {
        id: TableId::placeholder().table_id(),
        schema_id,
        database_id,
        name: table_name,
        columns,
        pk_column_ids: vec![0],
        pk_orders: vec![OrderType::Ascending as i32],
        dependent_relations: vec![],
        optional_associated_source_id: None,
    };

    let catalog_writer = session.env().catalog_writer();
    catalog_writer
        .create_materialized_source(source, table, plan)
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
