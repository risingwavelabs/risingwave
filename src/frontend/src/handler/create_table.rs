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
use risingwave_common::catalog::{ColumnDesc, ColumnId};
use risingwave_common::error::Result;
use risingwave_pb::catalog::source::Info;
use risingwave_pb::catalog::{Source as ProstSource, Table as ProstTable, TableSourceInfo};
use risingwave_pb::plan_common::ColumnCatalog;
use risingwave_sqlparser::ast::{
    ColumnDef, DataType as AstDataType, ObjectName, SqlOption, WithProperties,
};

use super::create_source::make_prost_source;
use crate::binder::expr::{bind_data_type, bind_struct_field};
use crate::catalog::{check_valid_column_name, row_id_column_desc};
use crate::optimizer::plan_node::{LogicalSource, StreamSource};
use crate::optimizer::property::{Order, RequiredDist};
use crate::optimizer::{PlanRef, PlanRoot};
use crate::session::{OptimizerContext, OptimizerContextRef, SessionImpl};
use crate::stream_fragmenter::StreamFragmenter;

// FIXME: store PK columns in ProstTableSourceInfo as Catalog information, and then remove this

/// Binds the column schemas declared in CREATE statement into `ColumnCatalog`.
pub fn bind_sql_columns(columns: Vec<ColumnDef>) -> Result<Vec<ColumnCatalog>> {
    let column_descs = {
        let mut column_descs = Vec::with_capacity(columns.len() + 1);
        // Put the hidden row id column in the first column. This is used for PK.
        column_descs.push(row_id_column_desc());
        // Then user columns.
        for (i, column) in columns.into_iter().enumerate() {
            check_valid_column_name(&column.name.value)?;
            let field_descs = if let AstDataType::Struct(fields) = &column.data_type {
                fields
                    .iter()
                    .map(bind_struct_field)
                    .collect::<Result<Vec<_>>>()?
            } else {
                vec![]
            };
            column_descs.push(ColumnDesc {
                data_type: bind_data_type(&column.data_type)?,
                column_id: ColumnId::new((i + 1) as i32),
                name: column.name.value,
                field_descs,
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
    Ok(columns_catalog)
}

pub(crate) fn gen_create_table_plan(
    session: &SessionImpl,
    context: OptimizerContextRef,
    table_name: ObjectName,
    columns: Vec<ColumnDef>,
    with_options: WithProperties,
) -> Result<(PlanRef, ProstSource, ProstTable)> {
    let source = make_prost_source(
        session,
        table_name,
        Info::TableSource(TableSourceInfo {
            columns: bind_sql_columns(columns)?,
            properties: with_options.into(),
        }),
    )?;
    let (plan, table) =
        gen_materialized_source_plan(context, source.clone(), session.user_name().to_string())?;
    Ok((plan, source, table))
}

/// Generate a stream plan with `StreamSource` + `StreamMaterialize`, it resembles a
/// `CREATE MATERIALIZED VIEW AS SELECT * FROM <source>`.
pub(crate) fn gen_materialized_source_plan(
    context: OptimizerContextRef,
    source: ProstSource,
    owner: String,
) -> Result<(PlanRef, ProstTable)> {
    let materialize = {
        // Manually assemble the materialization plan for the table.
        let source_node: PlanRef =
            StreamSource::new(LogicalSource::new(Rc::new((&source).into()), context)).into();
        let mut required_cols = FixedBitSet::with_capacity(source_node.schema().len());
        required_cols.toggle_range(..);
        required_cols.toggle(0);
        let mut out_names = source_node.schema().names();
        out_names.remove(0);

        PlanRoot::new(
            source_node,
            RequiredDist::Any,
            Order::any().clone(),
            required_cols,
            out_names,
        )
        .gen_create_mv_plan(source.name.clone())?
    };
    let mut table = materialize
        .table()
        .to_prost(source.schema_id, source.database_id);
    table.owner = owner;
    Ok((materialize.into(), table))
}

pub async fn handle_create_table(
    context: OptimizerContext,
    table_name: ObjectName,
    columns: Vec<ColumnDef>,
    with_options: Vec<SqlOption>,
) -> Result<PgResponse> {
    let session = context.session_ctx.clone();

    let (graph, source, table) = {
        let (plan, source, table) = gen_create_table_plan(
            &session,
            context.into(),
            table_name.clone(),
            columns,
            WithProperties(with_options),
        )?;
        let plan = plan.to_stream_prost();
        let graph = StreamFragmenter::build_graph(plan);

        (graph, source, table)
    };

    log::trace!(
        "name={}, graph=\n{}",
        table_name,
        serde_json::to_string_pretty(&graph).unwrap()
    );

    let catalog_writer = session.env().catalog_writer();
    catalog_writer
        .create_materialized_source(source, table, graph)
        .await?;

    Ok(PgResponse::empty_result(StatementType::CREATE_TABLE))
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use itertools::Itertools;
    use risingwave_common::catalog::{DEFAULT_DATABASE_NAME, DEFAULT_SCHEMA_NAME};
    use risingwave_common::types::DataType;

    use crate::catalog::row_id_column_name;
    use crate::test_utils::LocalFrontend;

    #[tokio::test]
    async fn test_create_table_handler() {
        let sql = "create table t (v1 smallint, v2 struct<v3 bigint, v4 float, v5 double>) with ('appendonly' = true);";
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
        assert!(source.append_only);

        // Check table exists.
        let table = catalog_reader
            .read_guard()
            .get_table_by_name(DEFAULT_DATABASE_NAME, DEFAULT_SCHEMA_NAME, "t")
            .unwrap()
            .clone();
        assert_eq!(table.name(), "t");

        // Get all column descs.
        let columns = table
            .columns
            .iter()
            .flat_map(|c| c.column_desc.flatten())
            .collect_vec();
        let columns = columns
            .iter()
            .map(|col| (col.name.as_str(), col.data_type.clone()))
            .collect::<HashMap<&str, DataType>>();

        let row_id_col_name = row_id_column_name();
        let expected_columns = maplit::hashmap! {
            row_id_col_name.as_str() => DataType::Int64,
            "v1" => DataType::Int16,
            "v2" => DataType::Struct {fields:vec![DataType::Int64,DataType::Float64,DataType::Float64].into()},
            "v2.v3" => DataType::Int64,
            "v2.v4" => DataType::Float64,
            "v2.v5" => DataType::Float64,
        };

        assert_eq!(columns, expected_columns);
    }
}
