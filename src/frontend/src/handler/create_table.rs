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

use std::collections::HashMap;
use std::rc::Rc;

use fixedbitset::FixedBitSet;
use itertools::Itertools;
use pgwire::pg_response::{PgResponse, StatementType};
use risingwave_common::catalog::ColumnDesc;
use risingwave_common::error::{ErrorCode, Result};
use risingwave_pb::catalog::source::Info;
use risingwave_pb::catalog::{
    ColumnIndex as ProstColumnIndex, Source as ProstSource, StreamSourceInfo, Table as ProstTable,
    TableSourceInfo,
};
use risingwave_pb::plan_common::ColumnCatalog as ProstColumnCatalog;
use risingwave_sqlparser::ast::{
    ColumnDef, ColumnOption, DataType as AstDataType, ObjectName, TableConstraint,
};

use super::create_source::make_prost_source;
use super::RwPgResponse;
use crate::binder::{bind_data_type, bind_struct_field};
use crate::catalog::column_catalog::ColumnCatalog;
use crate::catalog::{check_valid_column_name, ColumnId};
use crate::optimizer::plan_node::{LogicalSource, StreamSource};
use crate::optimizer::property::{Order, RequiredDist};
use crate::optimizer::{PlanRef, PlanRoot};
use crate::session::{OptimizerContext, OptimizerContextRef, SessionImpl};
use crate::stream_fragmenter::build_graph;

/// Binds the column schemas declared in CREATE statement into `ColumnDesc`.
/// If a column is marked as `primary key`, its `ColumnId` is also returned.
/// This primary key is not combined with table constraints yet.
pub fn bind_sql_columns(columns: Vec<ColumnDef>) -> Result<(Vec<ColumnDesc>, Option<ColumnId>)> {
    // In `ColumnDef`, pk can contain only one column. So we use `Option` rather than `Vec`.
    let mut pk_column_id = None;

    let column_descs = {
        let mut column_descs = Vec::with_capacity(columns.len());
        for (i, column) in columns.into_iter().enumerate() {
            let column_id = ColumnId::new(i as i32);
            // Destruct to make sure all fields are properly handled rather than ignored.
            // Do NOT use `..` to ignore fields you do not want to deal with.
            // Reject them with a clear NotImplemented error.
            let ColumnDef {
                name,
                data_type,
                collation,
                options,
            } = column;
            if let Some(collation) = collation {
                return Err(ErrorCode::NotImplemented(
                    format!("collation \"{}\"", collation),
                    None.into(),
                )
                .into());
            }
            for option_def in options {
                match option_def.option {
                    ColumnOption::Unique { is_primary: true } => {
                        if pk_column_id.is_some() {
                            return Err(ErrorCode::BindError(
                                "multiple primary keys are not allowed".into(),
                            )
                            .into());
                        }
                        pk_column_id = Some(column_id);
                    }
                    _ => {
                        return Err(ErrorCode::NotImplemented(
                            format!("column constraints \"{}\"", option_def),
                            None.into(),
                        )
                        .into())
                    }
                }
            }
            check_valid_column_name(&name.real_value())?;
            let field_descs = if let AstDataType::Struct(fields) = &data_type {
                fields
                    .iter()
                    .map(bind_struct_field)
                    .collect::<Result<Vec<_>>>()?
            } else {
                vec![]
            };
            column_descs.push(ColumnDesc {
                data_type: bind_data_type(&data_type)?,
                column_id,
                name: name.real_value(),
                field_descs,
                type_name: "".to_string(),
            });
        }
        column_descs
    };

    Ok((column_descs, pk_column_id))
}

/// Binds table constraints given the binding results from column definitions.
/// It returns the columns together with `pk_column_ids`, and an optional row id column index if
/// added.
pub fn bind_sql_table_constraints(
    column_descs: Vec<ColumnDesc>,
    pk_column_id_from_columns: Option<ColumnId>,
    constraints: Vec<TableConstraint>,
) -> Result<(Vec<ProstColumnCatalog>, Vec<ColumnId>, Option<usize>)> {
    let mut pk_column_names = vec![];
    for constraint in constraints {
        match constraint {
            TableConstraint::Unique {
                name: _,
                columns,
                is_primary: true,
            } => {
                if !pk_column_names.is_empty() {
                    return Err(ErrorCode::BindError(
                        "multiple primary keys are not allowed".into(),
                    )
                    .into());
                }
                pk_column_names = columns;
            }
            _ => {
                return Err(ErrorCode::NotImplemented(
                    format!("table constraint \"{}\"", constraint),
                    None.into(),
                )
                .into())
            }
        }
    }
    let mut pk_column_ids = match (pk_column_id_from_columns, pk_column_names.is_empty()) {
        (Some(_), false) => {
            return Err(ErrorCode::BindError("multiple primary keys are not allowed".into()).into())
        }
        (None, true) => {
            // We don't have a pk column now, so we need to add row_id column as the pk column
            // later.
            vec![]
        }
        (Some(cid), true) => vec![cid],
        (None, false) => {
            let name_to_id = column_descs
                .iter()
                .map(|c| (c.name.as_str(), c.column_id))
                .collect::<HashMap<_, _>>();
            pk_column_names
                .iter()
                .map(|ident| {
                    let name = ident.real_value();
                    name_to_id.get(name.as_str()).copied().ok_or_else(|| {
                        ErrorCode::BindError(format!(
                            "column \"{name}\" named in key does not exist"
                        ))
                    })
                })
                .try_collect()?
        }
    };

    let mut columns_catalog = column_descs
        .into_iter()
        .map(|c| {
            ColumnCatalog {
                column_desc: c,
                // All columns except `_row_id` should be visible.
                is_hidden: false,
            }
            .to_protobuf()
        })
        .collect_vec();

    // Add `_row_id` column if `pk_column_ids` is empty.
    let row_id_index = pk_column_ids.is_empty().then(|| {
        let row_id_index = columns_catalog.len();
        let row_id_column_id = ColumnId::new(row_id_index as i32);
        columns_catalog.push(ColumnCatalog::row_id_column(row_id_column_id).to_protobuf());
        pk_column_ids.push(row_id_column_id);
        row_id_index
    });
    Ok((columns_catalog, pk_column_ids, row_id_index))
}

pub(crate) fn gen_create_table_plan(
    session: &SessionImpl,
    context: OptimizerContextRef,
    table_name: ObjectName,
    columns: Vec<ColumnDef>,
    constraints: Vec<TableConstraint>,
) -> Result<(PlanRef, ProstSource, ProstTable)> {
    let (column_descs, pk_column_id_from_columns) = bind_sql_columns(columns)?;
    let (columns, pk_column_ids, row_id_index) =
        bind_sql_table_constraints(column_descs, pk_column_id_from_columns, constraints)?;
    let source = make_prost_source(
        session,
        table_name,
        Info::TableSource(TableSourceInfo {
            row_id_index: row_id_index.map(|index| ProstColumnIndex { index: index as _ }),
            columns,
            pk_column_ids: pk_column_ids.into_iter().map(Into::into).collect(),
            properties: context.inner().with_options.inner().clone(),
        }),
    )?;
    let (plan, table) = gen_materialized_source_plan(context, source.clone(), session.user_id())?;
    Ok((plan, source, table))
}

/// Generate a stream plan with `StreamSource` + `StreamMaterialize`, it resembles a
/// `CREATE MATERIALIZED VIEW AS SELECT * FROM <source>`.
pub(crate) fn gen_materialized_source_plan(
    context: OptimizerContextRef,
    source: ProstSource,
    owner: u32,
) -> Result<(PlanRef, ProstTable)> {
    let materialize = {
        // Manually assemble the materialization plan for the table.
        let source_node: PlanRef =
            StreamSource::new(LogicalSource::new(Rc::new((&source).into()), context)).into();
        let row_id_index = {
            let (Info::StreamSource(StreamSourceInfo { row_id_index, .. })
            | Info::TableSource(TableSourceInfo { row_id_index, .. })) = source.info.unwrap();
            row_id_index.as_ref().map(|index| index.index as _)
        };
        let mut required_cols = FixedBitSet::with_capacity(source_node.schema().len());
        required_cols.toggle_range(..);
        let mut out_names = source_node.schema().names();
        if let Some(row_id_index) = row_id_index {
            required_cols.toggle(row_id_index);
            out_names.remove(row_id_index);
        }

        PlanRoot::new(
            source_node,
            RequiredDist::Any,
            Order::any(),
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
    constraints: Vec<TableConstraint>,
) -> Result<RwPgResponse> {
    let session = context.session_ctx.clone();

    let (graph, source, table) = {
        let (plan, source, table) = gen_create_table_plan(
            &session,
            context.into(),
            table_name.clone(),
            columns,
            constraints,
        )?;
        let graph = build_graph(plan);

        (graph, source, table)
    };

    tracing::trace!(
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

    use risingwave_common::catalog::{DEFAULT_DATABASE_NAME, DEFAULT_SCHEMA_NAME};
    use risingwave_common::types::DataType;

    use super::*;
    use crate::catalog::row_id_column_name;
    use crate::test_utils::LocalFrontend;

    #[tokio::test]
    async fn test_create_table_handler() {
        let sql = "create table t (v1 smallint, v2 struct<v3 bigint, v4 float, v5 double>) with (appendonly = true);";
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

        let columns = table
            .columns
            .iter()
            .map(|col| (col.name(), col.data_type().clone()))
            .collect::<HashMap<&str, DataType>>();

        let row_id_col_name = row_id_column_name();
        let expected_columns = maplit::hashmap! {
            row_id_col_name.as_str() => DataType::Int64,
            "v1" => DataType::Int16,
            "v2" => DataType::new_struct(
                vec![DataType::Int64,DataType::Float64,DataType::Float64],
                vec!["v3".to_string(), "v4".to_string(), "v5".to_string()],
            ),
        };

        assert_eq!(columns, expected_columns);
    }

    #[test]
    fn test_bind_primary_key() {
        for (sql, expected) in [
            ("create table t (v1 int, v2 int)", Ok(&[2] as &[_])),
            ("create table t (v1 int primary key, v2 int)", Ok(&[0])),
            ("create table t (v1 int, v2 int primary key)", Ok(&[1])),
            (
                "create table t (v1 int primary key, v2 int primary key)",
                Err("multiple primary keys are not allowed"),
            ),
            (
                "create table t (v1 int primary key primary key, v2 int)",
                Err("multiple primary keys are not allowed"),
            ),
            (
                "create table t (v1 int, v2 int, primary key (v1))",
                Ok(&[0]),
            ),
            (
                "create table t (v1 int, primary key (v2), v2 int)",
                Ok(&[1]),
            ),
            (
                "create table t (primary key (v2, v1), v1 int, v2 int)",
                Ok(&[1, 0]),
            ),
            (
                "create table t (v1 int, primary key (v1), v2 int, primary key (v1))",
                Err("multiple primary keys are not allowed"),
            ),
            (
                "create table t (v1 int primary key, primary key (v1), v2 int)",
                Err("multiple primary keys are not allowed"),
            ),
            (
                "create table t (v1 int, primary key (V3), v2 int)",
                Err("column \"v3\" named in key does not exist"),
            ),
        ] {
            let mut ast = risingwave_sqlparser::parser::Parser::parse_sql(sql).unwrap();
            let risingwave_sqlparser::ast::Statement::CreateTable {
                    columns,
                    constraints,
                    ..
                } = ast.remove(0) else { panic!("test case should be create table") };
            let actual: Result<_> = (|| {
                let (column_descs, pk_column_id_from_columns) = bind_sql_columns(columns)?;
                let (_, pk_column_ids, _) = bind_sql_table_constraints(
                    column_descs,
                    pk_column_id_from_columns,
                    constraints,
                )?;
                Ok(pk_column_ids)
            })();
            match (expected, actual) {
                (Ok(expected), Ok(actual)) => assert_eq!(
                    expected.iter().copied().map(ColumnId::new).collect_vec(),
                    actual,
                    "sql: {sql}"
                ),
                (Ok(_), Err(actual)) => panic!("sql: {sql}\nunexpected error: {actual:?}"),
                (Err(_), Ok(actual)) => panic!("sql: {sql}\nexpects error but got: {actual:?}"),
                (Err(expected), Err(actual)) => assert!(
                    actual.to_string().contains(expected),
                    "sql: {sql}\nexpected: {expected:?}\nactual: {actual:?}"
                ),
            }
        }
    }
}
