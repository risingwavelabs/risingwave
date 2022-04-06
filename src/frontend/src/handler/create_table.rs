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

use std::rc::Rc;

use fixedbitset::FixedBitSet;
use itertools::Itertools;
use pgwire::pg_response::{PgResponse, StatementType};
use risingwave_common::catalog::{ColumnDesc, ColumnId, TableId};
use risingwave_common::error::{ErrorCode, Result};
use risingwave_common::types::DataType;
use risingwave_pb::catalog::source::Info;
use risingwave_pb::catalog::{Source as ProstSource, Table as ProstTable, TableSourceInfo};
use risingwave_pb::plan::ColumnCatalog;
use risingwave_sqlparser::ast::{ColumnDef, ObjectName};

use crate::binder::expr::bind_data_type;
use crate::binder::Binder;
use crate::catalog::{gen_row_id_column_name, is_row_id_column_name, ROWID_PREFIX};
use crate::optimizer::plan_node::{LogicalSource, StreamSource};
use crate::optimizer::property::{Distribution, Order};
use crate::optimizer::{PlanRef, PlanRoot};
use crate::session::{OptimizerContext, OptimizerContextRef, SessionImpl};
// FIXME: store PK columns in ProstTableSourceInfo as Catalog information, and then remove this
pub const TABLE_SOURCE_PK_COLID: ColumnId = ColumnId::new(0);

pub fn gen_create_table_plan(
    session: &SessionImpl,
    context: OptimizerContextRef,
    table_name: ObjectName,
    columns: Vec<ColumnDef>,
) -> Result<(PlanRef, ProstSource, ProstTable)> {
    let (schema_name, table_name) = Binder::resolve_table_name(table_name)?;
    let (database_id, schema_id) = session
        .env()
        .catalog_reader()
        .read_guard()
        .check_relation_name_duplicated(session.database(), &schema_name, &table_name)?;

    let column_descs = {
        let mut column_descs = Vec::with_capacity(columns.len() + 1);
        // Put the hidden row id column in the first column. This is used for PK.
        column_descs.push(ColumnDesc {
            data_type: DataType::Int64,
            column_id: TABLE_SOURCE_PK_COLID,
            name: gen_row_id_column_name(0),
            field_descs: vec![],
            type_name: "".to_string(),
        });
        // Then user columns.
        for (i, column) in columns.into_iter().enumerate() {
            if is_row_id_column_name(&column.name.value) {
                return Err(ErrorCode::InternalError(format!(
                    "column name prefixed with {:?} are reserved word.",
                    ROWID_PREFIX
                ))
                .into());
            }

            column_descs.push(ColumnDesc {
                data_type: bind_data_type(&column.data_type)?,
                column_id: ColumnId::new((i + 1) as i32),
                name: column.name.value,
                field_descs: vec![],
                type_name: "".to_string(),
            });
        }
        column_descs
    };

    let columns_catalog = column_descs
        .into_iter()
        .enumerate()
        .map(|(i, c)| ColumnCatalog {
            column_desc: c.to_protobuf().into(),
            is_hidden: i == 0, // the row id column is hidden
        })
        .collect_vec();

    let source = ProstSource {
        id: TableId::placeholder().table_id(),
        schema_id,
        database_id,
        name: table_name.clone(),
        info: Info::TableSource(TableSourceInfo {
            columns: columns_catalog,
        })
        .into(),
    };

    let materialize = {
        // Manually assemble the materialization plan for the table.
        let source_node: PlanRef =
            StreamSource::new(LogicalSource::new(Rc::new((&source).into()), context)).into();
        let mut required_cols = FixedBitSet::with_capacity(source_node.schema().len());
        required_cols.toggle_range(..);
        required_cols.toggle(0);

        PlanRoot::new(
            source_node,
            Distribution::HashShard(vec![0]),
            Order::any().clone(),
            required_cols,
        )
        .gen_create_mv_plan(table_name)?
    };
    let table = materialize.table().to_prost(schema_id, database_id);

    Ok((materialize.into(), source, table))
}

pub async fn handle_create_table(
    context: OptimizerContext,
    table_name: ObjectName,
    columns: Vec<ColumnDef>,
) -> Result<PgResponse> {
    let session = context.session_ctx.clone();

    let (plan, source, table) = {
        let (plan, source, table) =
            gen_create_table_plan(&session, context.into(), table_name.clone(), columns)?;
        let plan = plan.to_stream_prost();

        (plan, source, table)
    };

    log::trace!(
        "name={}, plan=\n{}",
        table_name,
        serde_json::to_string_pretty(&plan).unwrap()
    );

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

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use risingwave_common::catalog::{DEFAULT_DATABASE_NAME, DEFAULT_SCHEMA_NAME};
    use risingwave_common::types::DataType;

    use crate::catalog::gen_row_id_column_name;
    use crate::test_utils::LocalFrontend;

    #[tokio::test]
    async fn test_create_table_handler() {
        let sql = "create table t (v1 smallint, v2 int, v3 bigint, v4 float, v5 double);";
        let frontend = LocalFrontend::new(Default::default()).await;
        frontend.run_sql(sql).await.unwrap();

        let session = frontend.session_ref();
        let catalog_reader = session.env().catalog_reader();

        // Check source exists.
        let source = catalog_reader
            .read_guard()
            .get_source_by_name(DEFAULT_DATABASE_NAME, DEFAULT_SCHEMA_NAME, "t")
            .unwrap()
            .clone();
        assert_eq!(source.name, "t");

        // Check table exists.
        let table = catalog_reader
            .read_guard()
            .get_table_by_name(DEFAULT_DATABASE_NAME, DEFAULT_SCHEMA_NAME, "t")
            .unwrap()
            .clone();
        assert_eq!(table.name(), "t");

        let columns = table
            .columns()
            .iter()
            .map(|col| (col.name(), col.data_type().clone()))
            .collect::<HashMap<&str, DataType>>();

        let row_id_col_name = gen_row_id_column_name(0);
        let expected_columns = maplit::hashmap! {
            row_id_col_name.as_str() => DataType::Int64,
            "v1" => DataType::Int16,
            "v2" => DataType::Int32,
            "v3" => DataType::Int64,
            "v4" => DataType::Float64,
            "v5" => DataType::Float64,
        };

        assert_eq!(columns, expected_columns);
    }
}
